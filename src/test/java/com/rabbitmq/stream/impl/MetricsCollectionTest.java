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

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.metrics.MetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class MetricsCollectionTest {

    String stream;
    TestUtils.ClientFactory cf;

    CountMetricsCollector metricsCollector;

    @BeforeEach
    void init() {
        metricsCollector = new CountMetricsCollector();
    }

    @Test
    void publishConfirmChunkConsumeShouldBeCollected() throws Exception {
        int messageCount = 1000;
        CountDownLatch publishLatch = new CountDownLatch(messageCount);
        Client publisher = cf.get(new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream, (byte) 1,
                Collections.singletonList(publisher.messageBuilder().addData("".getBytes()).build())));

        assertThat(publishLatch.await(10, TimeUnit.SECONDS));
        assertThat(metricsCollector.publish.get()).isEqualTo(messageCount);
        assertThat(metricsCollector.confirm.get()).isEqualTo(messageCount);
        assertThat(metricsCollector.error.get()).isZero();

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Client consumer = cf.get(new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> consumeLatch.countDown())
        );

        Client.Response response = consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();

        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(metricsCollector.chunk.get()).isPositive();
        assertThat(metricsCollector.entriesInChunk.get()).isEqualTo(messageCount);
        assertThat(metricsCollector.consume.get()).isEqualTo(messageCount);
    }

    @Test
    void publishErrorShouldBeCollected() throws Exception {
        int messageCount = 1000;
        CountDownLatch publishErrorLatch = new CountDownLatch(messageCount);
        Client publisher = cf.get(new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishErrorListener((publisherId, publishingId, errorCode) -> publishErrorLatch.countDown()));

        String nonExistingStream = UUID.randomUUID().toString();
        IntStream.range(0, messageCount).forEach(i -> publisher.publish(nonExistingStream, (byte) 1,
                Collections.singletonList(publisher.messageBuilder().addData("".getBytes()).build())
        ));

        assertThat(publishErrorLatch.await(10, TimeUnit.SECONDS));
        assertThat(metricsCollector.confirm.get()).isZero();
        assertThat(metricsCollector.error.get()).isEqualTo(messageCount);
    }

    @Test
    void publishConfirmChunkConsumeShouldBeCollectedWithBatchEntryPublishing() throws Exception {
        int batchCount = 100;
        int messagesInBatch = 30;
        int messageCount = batchCount * messagesInBatch;
        CountDownLatch publishLatch = new CountDownLatch(batchCount);
        Client publisher = cf.get(new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

        IntStream.range(0, batchCount).forEach(batchIndex -> {
            MessageBatch messageBatch = new MessageBatch(MessageBatch.Compression.NONE);
            IntStream.range(0, messagesInBatch).forEach(messageIndex -> {
                messageBatch.add(publisher.messageBuilder().addData("".getBytes()).build());
            });
            publisher.publishBatches(stream, (byte) 1, Collections.singletonList(messageBatch));
        });

        assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(metricsCollector.publish.get()).isEqualTo(messageCount);
        assertThat(metricsCollector.confirm.get()).isEqualTo(batchCount); // one confirm per sub batch entry
        assertThat(metricsCollector.error.get()).isZero();

        CountDownLatch consumeLatch = new CountDownLatch(messageCount);
        Client consumer = cf.get(new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    consumeLatch.countDown();
                }));

        Client.Response response = consumer.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
        assertThat(response.isOk()).isTrue();

        assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(metricsCollector.chunk.get()).isPositive();
        assertThat(metricsCollector.entriesInChunk.get()).isEqualTo(batchCount);
        assertThat(metricsCollector.consume.get()).isEqualTo(messageCount);
    }

    private static class CountMetricsCollector implements MetricsCollector {

        private final AtomicLong publish = new AtomicLong(0);
        private final AtomicLong confirm = new AtomicLong(0);
        private final AtomicLong error = new AtomicLong(0);
        private final AtomicLong chunk = new AtomicLong(0);
        private final AtomicLong entriesInChunk = new AtomicLong(0);
        private final AtomicLong consume = new AtomicLong(0);

        @Override
        public void publish(int count) {
            publish.addAndGet(count);
        }

        @Override
        public void publishConfirm(int count) {
            confirm.addAndGet(count);
        }

        @Override
        public void publishError(int count) {
            error.addAndGet(count);
        }

        @Override
        public void chunk(int entriesCount) {
            chunk.incrementAndGet();
            entriesInChunk.addAndGet(entriesCount);
        }

        @Override
        public void consume(long count) {
            consume.addAndGet(count);
        }
    }

}
