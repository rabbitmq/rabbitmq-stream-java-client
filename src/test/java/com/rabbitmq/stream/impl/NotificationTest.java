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

package com.rabbitmq.stream.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.OffsetSpecification;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class NotificationTest {

    TestUtils.ClientFactory cf;

    @Test
    void metadataListenerIsCalledWhenStreamIsDeletedWithAmqp() throws Exception {
        byte subscriptionCount = 1;
        String t = UUID.randomUUID().toString();
        CountDownLatch subscriptionListenerLatch = new CountDownLatch(subscriptionCount);
        Client subscriptionClient = cf.get(new Client.ClientParameters()
                .metadataListener((stream, code) -> {
                    if (t.equals(stream) && code == Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE) {
                        subscriptionListenerLatch.countDown();
                    }
                })
        );

        try (Connection c = new ConnectionFactory().newConnection()) {
            Channel ch = c.createChannel();
            ch.queueDeclare(t, true, false, false, Collections.singletonMap("x-queue-type", "stream"));

            IntStream.range(0, subscriptionCount).forEach(i -> subscriptionClient.subscribe((byte) i, t, OffsetSpecification.first(), 10));

            ch.queueDelete(t);

            assertThat(subscriptionListenerLatch.await(5, SECONDS)).isTrue();
        }
    }

    @Test
    void publisherIsNotifiedAndReceivesPublishErrorIfStreamIsDeleted() throws Exception {
        String s = UUID.randomUUID().toString();
        Client client = cf.get();
        Client.Response response = client.create(s);
        assertThat(response.isOk()).isTrue();

        CountDownLatch publishLatch = new CountDownLatch(1);
        CountDownLatch metadataLatch = new CountDownLatch(1);
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicInteger receivedCode = new AtomicInteger(-1);
        AtomicReference<String> receivedStream = new AtomicReference<>();
        Client publisher = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> publishLatch.countDown())
                .metadataListener((stream, code) -> {
                    receivedStream.set(stream);
                    receivedCode.set(code);
                    metadataLatch.countDown();
                })
                .publishErrorListener((publishingId, errorCode) -> errorLatch.countDown()));

        publisher.publish(s, Collections.singletonList(publisher.messageBuilder().addData("".getBytes()).build()));

        assertThat(publishLatch.await(10, SECONDS)).isTrue();

        response = cf.get().delete(s);
        assertThat(response.isOk()).isTrue();

        assertThat(metadataLatch.await(10, SECONDS)).isTrue();
        assertThat(receivedStream.get()).isEqualTo(s);
        assertThat(receivedCode.get()).isEqualTo(Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

        publisher.publish(s, Collections.singletonList(publisher.messageBuilder().addData("".getBytes()).build()));
        assertThat(errorLatch.await(10, SECONDS)).isTrue();
    }

    @Test
    void consumerIsNotifiedIfStreamIsDeleted() throws Exception {
        String s = UUID.randomUUID().toString();
        CountDownLatch publishLatch = new CountDownLatch(1);
        Client publisher = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> publishLatch.countDown()));
        Client.Response response = publisher.create(s);
        assertThat(response.isOk()).isTrue();
        publisher.publish(s, Collections.singletonList(publisher.messageBuilder().addData("".getBytes()).build()));

        assertThat(publishLatch.await(10, SECONDS)).isTrue();

        CountDownLatch consumeLatch = new CountDownLatch(1);
        CountDownLatch metadataLatch = new CountDownLatch(1);
        AtomicInteger receivedCode = new AtomicInteger(-1);
        AtomicReference<String> receivedStream = new AtomicReference<>();
        Client consumer = cf.get(new Client.ClientParameters()
                .messageListener((subscriptionId, offset, message) -> consumeLatch.countDown())
                .metadataListener((stream, code) -> {
                    receivedStream.set(stream);
                    receivedCode.set(code);
                    metadataLatch.countDown();
                }));

        response = consumer.subscribe((byte) 1, s, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();
        assertThat(consumeLatch.await(10, SECONDS)).isTrue();

        response = cf.get().delete(s);
        assertThat(response.isOk()).isTrue();

        assertThat(metadataLatch.await(10, SECONDS)).isTrue();
        assertThat(receivedStream.get()).isEqualTo(s);
        assertThat(receivedCode.get()).isEqualTo(Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);
    }

    @Test
    void consumerIsNotNotifiedWhenStreamIsDeletedIfItHasUnsuscribed() throws Exception {
        String s = UUID.randomUUID().toString();
        Client client = cf.get();
        Client.Response response = client.create(s);
        assertThat(response.isOk()).isTrue();

        CountDownLatch metadataLatch = new CountDownLatch(1);
        Client consumer = cf.get(new Client.ClientParameters()
                .metadataListener((stream, code) -> metadataLatch.countDown()));

        response = consumer.subscribe((byte) 1, s, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();

        response = consumer.unsubscribe((byte) 1);
        assertThat(response.isOk()).isTrue();

        response = cf.get().delete(s);
        assertThat(response.isOk()).isTrue();

        assertThat(metadataLatch.await(2, SECONDS)).isFalse();
    }

    @Test
    void metadataListenerIsCalledWhenStreamIsDeleted() throws Exception {
        class TestParameters {
            final boolean sameClient;
            final int subscriptionCount;

            TestParameters(boolean sameClient, int subscriptionCount) {
                this.sameClient = sameClient;
                this.subscriptionCount = subscriptionCount;
            }
        }

        TestParameters[] testParameters = new TestParameters[]{
                new TestParameters(true, 1),
                new TestParameters(false, 1),
                new TestParameters(true, 5),
                new TestParameters(false, 5),
        };

        for (TestParameters testParameter : testParameters) {
            String t = UUID.randomUUID().toString();
            AtomicInteger metadataListenerCalls = new AtomicInteger(0);
            CountDownLatch subscriptionListenerLatch = new CountDownLatch(1);
            Client subscriptionClient = cf.get(new Client.ClientParameters()
                    .metadataListener((stream, code) -> {
                        if (t.equals(stream) && code == Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE) {
                            metadataListenerCalls.incrementAndGet();
                            subscriptionListenerLatch.countDown();
                        }
                    }));

            Client createDeleteClient;
            if (testParameter.sameClient) {
                createDeleteClient = subscriptionClient;
            } else {
                createDeleteClient = cf.get();
            }

            createDeleteClient.create(t);

            IntStream.range(0, testParameter.subscriptionCount).forEach(i -> subscriptionClient.subscribe((byte) i, t, OffsetSpecification.first(), 10));

            createDeleteClient.delete(t);

            assertThat(subscriptionListenerLatch.await(5, SECONDS)).isTrue();
            assertThat(metadataListenerCalls.get()).isEqualTo(1);
        }

    }

}
