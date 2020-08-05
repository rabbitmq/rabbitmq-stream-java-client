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

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamConsumerTest {

    static final Duration RECOVERY_DELAY = Duration.ofSeconds(1);

    String stream;
    EventLoopGroup eventLoopGroup;
    TestUtils.ClientFactory cf;

    Environment environment;

    @BeforeEach
    void init() {
        environment = Environment.builder()
                .eventLoopGroup(eventLoopGroup)
                .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(RECOVERY_DELAY))
                .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        environment.close();
    }

    @Test
    void consume() throws Exception {
        int messageCount = 100_000;
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> publishLatch.countDown()));

        IntStream.range(0, messageCount).forEach(i -> client.publish(stream,
                Collections.singletonList(client.messageBuilder().addData("".getBytes()).build())));

        assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);

        Consumer consumer = environment.consumerBuilder()
                .stream(stream)
                .messageHandler((offset, message) -> consumeLatch.countDown())
                .build();

        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

        consumer.close();
    }

    @Test
    void creatingConsumerOnNonExistingStreamShouldThrowException() {
        String nonExistingStream = UUID.randomUUID().toString();
        assertThatThrownBy(() -> {
            environment.consumerBuilder()
                    .stream(nonExistingStream)
                    .messageHandler((offset, message) -> {

                    })
                    .build();
        }).isInstanceOf(StreamDoesNotExistException.class)
                .hasMessageContaining(nonExistingStream)
                .extracting("stream").isEqualTo(nonExistingStream);
    }

    @Test
    void consumerShouldBeClosedWhenStreamGetsDeleted() throws Exception {
        String s = UUID.randomUUID().toString();
        environment.streamCreator().stream(s).create();

        int messageCount = 10_000;
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Producer producer = environment.producerBuilder().stream(s).build();
        IntStream.range(0, messageCount).forEach(i -> producer.send(
                producer.messageBuilder().addData("".getBytes()).build(),
                confirmationStatus -> publishLatch.countDown()
        ));

        assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        StreamConsumer consumer = (StreamConsumer) environment.consumerBuilder()
                .stream(s)
                .messageHandler((offset, message) -> consumeLatch.countDown())
                .build();

        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(consumer.isOpen()).isTrue();

        environment.deleteStream(s);

        TestUtils.waitAtMost(10, () -> !consumer.isOpen());

        assertThat(consumer.isOpen()).isFalse();
    }

    @Test
    @TestUtils.DisabledIfRabbitMqCtlNotSet
    void consumerShouldKeepConsumingIfStreamBecomesUnavailable() throws Exception {
        String s = UUID.randomUUID().toString();
        environment.streamCreator().stream(s).create();
        try {
            int messageCount = 10_000;
            CountDownLatch publishLatch = new CountDownLatch(messageCount);
            Producer producer = environment.producerBuilder().stream(s).build();
            IntStream.range(0, messageCount).forEach(i -> producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatch.countDown()
            ));

            assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
            producer.close();

            AtomicInteger receivedMessageCount = new AtomicInteger(0);
            CountDownLatch consumeLatch = new CountDownLatch(messageCount);
            CountDownLatch consumeLatchSecondWave = new CountDownLatch(messageCount * 2);
            StreamConsumer consumer = (StreamConsumer) environment.consumerBuilder()
                    .stream(s)
                    .messageHandler((offset, message) -> {
                        receivedMessageCount.incrementAndGet();
                        consumeLatch.countDown();
                        consumeLatchSecondWave.countDown();
                    })
                    .build();

            assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(consumer.isOpen()).isTrue();

            Host.killStreamLeaderProcess(s);

            // give the system some time to recover
            Thread.sleep(DefaultClientSubscriptions.METADATA_UPDATE_DEFAULT_INITIAL_DELAY.toMillis());

            Client client = cf.get();
            TestUtils.waitAtMost(10, () -> {
                Client.StreamMetadata metadata = client.metadata(s).get(s);
                return metadata.getLeader() != null || !metadata.getReplicas().isEmpty();
            });

            CountDownLatch publishLatchSecondWave = new CountDownLatch(messageCount);
            Producer producerSecondWave = environment.producerBuilder().stream(s).build();
            IntStream.range(0, messageCount).forEach(i -> producerSecondWave.send(
                    producerSecondWave.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatchSecondWave.countDown()
            ));

            assertThat(publishLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
            producerSecondWave.close();

            assertThat(consumeLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(receivedMessageCount.get()).isBetween(messageCount * 2, messageCount * 2 + 1); // there can be a duplicate
            assertThat(consumer.isOpen()).isTrue();

            consumer.close();
        } finally {
            environment.deleteStream(s);
        }
    }

    @Test
    @TestUtils.DisabledIfRabbitMqCtlNotSet
    void consumerShouldKeepConsumingIfConnectionFails() throws Exception {
        String s = UUID.randomUUID().toString();
        environment.streamCreator().stream(s).create();
        try {
            int messageCount = 10_000;
            CountDownLatch publishLatch = new CountDownLatch(messageCount);
            Producer producer = environment.producerBuilder().stream(s).build();
            IntStream.range(0, messageCount).forEach(i -> producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatch.countDown()
            ));

            assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
            producer.close();

            AtomicInteger receivedMessageCount = new AtomicInteger(0);
            CountDownLatch consumeLatch = new CountDownLatch(messageCount);
            CountDownLatch consumeLatchSecondWave = new CountDownLatch(messageCount * 2);
            StreamConsumer consumer = (StreamConsumer) environment.consumerBuilder()
                    .stream(s)
                    .messageHandler((offset, message) -> {
                        receivedMessageCount.incrementAndGet();
                        consumeLatch.countDown();
                        consumeLatchSecondWave.countDown();
                    })
                    .build();

            assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(consumer.isOpen()).isTrue();

            Host.killConnection("rabbitmq-stream-consumer");

            // give the system some time to recover
            Thread.sleep(RECOVERY_DELAY.toMillis() * 2);

            Client client = cf.get();
            TestUtils.waitAtMost(10, () -> {
                Client.StreamMetadata metadata = client.metadata(s).get(s);
                return metadata.getLeader() != null || !metadata.getReplicas().isEmpty();
            });

            CountDownLatch publishLatchSecondWave = new CountDownLatch(messageCount);
            Producer producerSecondWave = environment.producerBuilder().stream(s).build();
            IntStream.range(0, messageCount).forEach(i -> producerSecondWave.send(
                    producerSecondWave.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatchSecondWave.countDown()
            ));

            assertThat(publishLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
            producerSecondWave.close();

            assertThat(consumeLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(receivedMessageCount.get()).isBetween(messageCount * 2, messageCount * 2 + 1); // there can be a duplicate
            assertThat(consumer.isOpen()).isTrue();

            consumer.close();
        } finally {
            environment.deleteStream(s);
        }
    }

}
