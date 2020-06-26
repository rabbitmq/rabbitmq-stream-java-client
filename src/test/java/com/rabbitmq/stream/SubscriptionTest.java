// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

package com.rabbitmq.stream;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.rabbitmq.stream.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SubscriptionTest {

    String stream;
    TestUtils.ClientFactory cf;

    @Test
    void severalSubscriptionsInSameConnection() throws Exception {
        int messageCount = 1000;
        Client publisher = cf.get();

        ConcurrentMap<Integer, CountDownLatch> latches = new ConcurrentHashMap<>(2);
        latches.put(1, new CountDownLatch(messageCount * 2));
        latches.put(2, new CountDownLatch(messageCount));

        ConcurrentMap<Integer, AtomicInteger> messageCounts = new ConcurrentHashMap<>(2);
        Client consumer = cf.get(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        consumer.subscribe(1, stream, OffsetSpecification.first(), messageCount * 2);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("" + i).getBytes()));

        waitAtMost(5, () -> messageCounts.computeIfAbsent(1, k -> new AtomicInteger(0)).get() == messageCount);

        consumer.subscribe(2, stream, OffsetSpecification.first(), messageCount * 2);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, ("" + i).getBytes()));

        assertThat(latches.get(1).await(5, SECONDS)).isTrue();
        assertThat(latches.get(2).await(5, SECONDS)).isTrue();
    }

    @Test
    void subscriptionToNonExistingStreamShouldReturnError() {
        String nonExistingStream = UUID.randomUUID().toString();
        Client.Response response = cf.get().subscribe(1, nonExistingStream, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }

    @Test
    void subscriptionToNonStreamQueueShouldReturnError() throws Exception {
        String nonStreamQueue = UUID.randomUUID().toString();
        ConnectionFactory connectionFactory = new ConnectionFactory();
        try (Connection amqpConnection = connectionFactory.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.queueDeclare(nonStreamQueue, false, true, false, null);

            Client.Response response = cf.get().subscribe(1, nonStreamQueue, OffsetSpecification.first(), 10);
            assertThat(response.isOk()).isFalse();
            assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
        }
    }

    @Test
    void unsubscribeShouldNotReceiveMoreMessageAfterUnsubscribe() throws Exception {
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger receivedMessageCount = new AtomicInteger(0);
        Client client = cf.get(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            receivedMessageCount.incrementAndGet();
            latch.countDown();
        }));
        Client.Response response = client.subscribe(1, stream, OffsetSpecification.first(), messageCount * 100);
        assertThat(response.isOk()).isTrue();
        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latch.await(10, SECONDS)).isTrue();
        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        CountDownLatch latch2 = new CountDownLatch(messageCount);
        Client client2 = cf.get(new Client.ClientParameters().messageListener((correlationId, offset, message) -> latch2.countDown()));
        client2.subscribe(1, stream, OffsetSpecification.first(), messageCount * 100);
        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latch2.await(10, SECONDS)).isTrue();
        Thread.sleep(1000L);
        assertThat(receivedMessageCount).hasValue(messageCount);
    }

    @Test
    void unsubscribeTwoSubscriptionsOneIsCancelled() throws Exception {
        int messageCount = 10;
        ConcurrentMap<Integer, CountDownLatch> latches = new ConcurrentHashMap<>(2);
        latches.put(1, new CountDownLatch(messageCount));
        latches.put(2, new CountDownLatch(messageCount * 2));
        ConcurrentMap<Integer, AtomicInteger> messageCounts = new ConcurrentHashMap<>(2);
        Client client = cf.get(new Client.ClientParameters().messageListener((correlationId, offset, message) -> {
            messageCounts.computeIfAbsent(correlationId, k -> new AtomicInteger(0)).incrementAndGet();
            latches.get(correlationId).countDown();
        }));

        Client.Response response = client.subscribe(1, stream, OffsetSpecification.first(), messageCount * 100);
        assertThat(response.isOk()).isTrue();
        response = client.subscribe(2, stream, OffsetSpecification.first(), messageCount * 100);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latches.get(1).await(10, SECONDS)).isTrue();

        response = client.unsubscribe(1);
        assertThat(response.isOk()).isTrue();

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream, ("" + i).getBytes()));
        assertThat(latches.get(2).await(10, SECONDS)).isTrue();
        assertThat(messageCounts.get(2)).hasValue(messageCount * 2);
        assertThat(messageCounts.get(1)).hasValue(messageCount);

        client.unsubscribe(2);
    }

    @Test
    void unsubscribeNonExistingSubscriptionShouldReturnError() {
        Client client = cf.get();
        Client.Response response = client.subscribe(1, stream, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();

        response = client.unsubscribe(42);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST);
    }

    @Test
    void subscriptionWithAlreadyExistingSubscriptionIdShouldReturnError() {
        Client client = cf.get();
        Client.Response response = client.subscribe(1, stream, OffsetSpecification.first(), 20);
        assertThat(response.isOk()).isTrue();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

        response = client.subscribe(1, stream, OffsetSpecification.first(), 20);
        assertThat(response.isOk()).isFalse();
        assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS);
    }

}
