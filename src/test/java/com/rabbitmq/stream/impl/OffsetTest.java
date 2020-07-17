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
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static com.rabbitmq.stream.impl.TestUtils.publishAndWaitForConfirms;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class OffsetTest {

    String stream;
    TestUtils.ClientFactory cf;
    EventLoopGroup eventLoopGroup;

    @Test
    void offsetTypeFirstShouldStartConsumingFromBeginning() throws Exception {
        int messageCount = 50000;
        TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicLong first = new AtomicLong(-1);
        AtomicLong last = new AtomicLong();
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset12, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset1, message) -> {
                    first.compareAndSet(-1, offset1);
                    last.set(offset1);
                    latch.countDown();
                }));
        client.subscribe(1, stream, OffsetSpecification.first(), 10);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(first.get()).isEqualTo(0);
        assertThat(last.get()).isEqualTo(messageCount - 1);
    }

    @Test
    void offsetTypeLastShouldReturnLastChunk() throws Exception {
        int messageCount = 50000;
        long lastOffset = messageCount - 1;
        TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong first = new AtomicLong(-1);
        AtomicLong last = new AtomicLong();
        AtomicInteger chunkCount = new AtomicInteger(0);
        AtomicLong chunkOffset = new AtomicLong(-1);
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset12, messageCount1, dataSize) -> {
                    client1.credit(subscriptionId, 1);
                    chunkOffset.compareAndSet(-1, offset12);
                    chunkCount.incrementAndGet();
                })
                .messageListener((subscriptionId, offset1, message) -> {
                    first.compareAndSet(-1, offset1);
                    last.set(offset1);
                    if (offset1 == lastOffset) {
                        latch.countDown();
                    }
                }));
        client.subscribe(1, stream, OffsetSpecification.last(), 10);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(chunkCount.get()).isEqualTo(1);
        assertThat(first.get()).isEqualTo(chunkOffset.get());
        assertThat(last.get()).isEqualTo(lastOffset);
    }

    @Test
    void offsetTypeNextShouldReturnNewPublishedMessages() throws Exception {
        int firstWaveMessageCount = 50000;
        int secondWaveMessageCount = 20000;
        int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
        TestUtils.publishAndWaitForConfirms(cf, firstWaveMessageCount, stream);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong first = new AtomicLong(-1);
        AtomicLong last = new AtomicLong();
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset1, message) -> {
                    first.compareAndSet(-1, offset1);
                    last.set(offset1);
                    if (offset1 == lastOffset) {
                        latch.countDown();
                    }
                }));
        client.subscribe(1, stream, OffsetSpecification.next(), 10);
        assertThat(latch.await(2, SECONDS)).isFalse(); // should not receive anything
        TestUtils.publishAndWaitForConfirms(cf, secondWaveMessageCount, stream);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(first.get()).isEqualTo(firstWaveMessageCount);
        assertThat(last.get()).isEqualTo(lastOffset);
    }

    @Test
    void offsetTypeOffsetShouldStartConsumingFromOffset() throws Exception {
        int messageCount = 50000;
        TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
        int offset = messageCount / 10;
        CountDownLatch latch = new CountDownLatch(messageCount - offset);
        AtomicLong first = new AtomicLong(-1);
        AtomicLong last = new AtomicLong();
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset12, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset1, message) -> {
                    first.compareAndSet(-1, offset1);
                    last.set(offset1);
                    latch.countDown();
                }));
        client.subscribe(1, stream, OffsetSpecification.offset(offset), 10);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(first.get()).isEqualTo(offset);
        assertThat(last.get()).isEqualTo(messageCount - 1);
    }

    @Test
    void offsetTypeTimestampShouldStartConsumingFromTimestamp() throws Exception {
        int firstWaveMessageCount = 50000;
        int secondWaveMessageCount = 20000;
        int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
        TestUtils.publishAndWaitForConfirms(cf, "first wave ", firstWaveMessageCount, stream);
        Thread.sleep(5000);
        long now = System.currentTimeMillis();
        TestUtils.publishAndWaitForConfirms(cf, "second wave ", secondWaveMessageCount, stream);
        long timestampOffset = now - 1000; // one second earlier
        CountDownLatch latch = new CountDownLatch(1);
        AtomicLong first = new AtomicLong(-1);
        AtomicLong last = new AtomicLong();
        Set<String> consumed = ConcurrentHashMap.newKeySet();
        Client client = cf.get(new Client.ClientParameters()
                .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset1, message) -> {
                    first.compareAndSet(-1, offset1);
                    last.set(offset1);
                    consumed.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
                    if (offset1 == lastOffset) {
                        latch.countDown();
                    }
                }));
        client.subscribe(1, stream, OffsetSpecification.timestamp(timestampOffset), 10);
        assertThat(latch.await(10, SECONDS)).isTrue();
        assertThat(first.get()).isEqualTo(firstWaveMessageCount);
        assertThat(last.get()).isEqualTo(lastOffset);
        consumed.stream().forEach(v -> assertThat(v).startsWith("second wave").doesNotStartWith("first wave"));
    }

    @Test
    void filterSmallerOffsets() throws Exception {
        int messageCount = 50000;
        TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
        for (int i = 0; i < 10; i++) {
            Map<Integer, Long> firstOffsets = new ConcurrentHashMap<>();
            Map<Integer, CountDownLatch> latches = new ConcurrentHashMap<>();
            latches.put(1, new CountDownLatch(1));
            latches.put(2, new CountDownLatch(1));
            Client client = new Client(new Client.ClientParameters().messageListener((subscriptionId, offset, message) -> {
                if (firstOffsets.get(subscriptionId) == null) {
                    firstOffsets.put(subscriptionId, offset);
                }
                if (offset == messageCount - 1) {
                    latches.get(subscriptionId).countDown();
                }
            }).chunkListener((client1, subscriptionId, offset, msgCount, dataSize) -> client1.credit(subscriptionId, 1))
                    .eventLoopGroup(eventLoopGroup));
            client.subscribe(1, stream, OffsetSpecification.offset(50), 10);
            client.subscribe(2, stream, OffsetSpecification.offset(100), 10);

            assertThat(latches.get(1).await(10, SECONDS)).isTrue();
            assertThat(latches.get(2).await(10, SECONDS)).isTrue();
            assertThat(firstOffsets.get(1)).isEqualTo(50);
            assertThat(firstOffsets.get(2)).isEqualTo(100);
            client.close();
        }
    }

    @Test
    void consumeFromTail() throws Exception {
        int messageCount = 10000;
        CountDownLatch firstWaveLatch = new CountDownLatch(messageCount);
        CountDownLatch secondWaveLatch = new CountDownLatch(messageCount * 2);
        Client publisher = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> {
                    firstWaveLatch.countDown();
                    secondWaveLatch.countDown();
                }));
        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream,
                Collections.singletonList(publisher.messageBuilder().addData(("first wave " + i).getBytes(StandardCharsets.UTF_8)).build())));
        assertThat(firstWaveLatch.await(10, SECONDS)).isTrue();

        CountDownLatch consumedLatch = new CountDownLatch(messageCount);

        Set<String> consumed = ConcurrentHashMap.newKeySet();
        Client consumer = cf.get(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, messageCount1, dataSize) -> client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    consumed.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
                    consumedLatch.countDown();
                }));

        consumer.subscribe(1, stream, OffsetSpecification.next(), 10);

        IntStream.range(0, messageCount).forEach(i -> publisher.publish(stream,
                Collections.singletonList(publisher.messageBuilder().addData(("second wave " + i).getBytes(StandardCharsets.UTF_8)).build())));

        assertThat(consumedLatch.await(10, SECONDS)).isTrue();
        assertThat(consumed).hasSize(messageCount);
        consumed.stream().forEach(v -> assertThat(v).startsWith("second wave").doesNotStartWith("first wave"));
    }

    @Test
    void shouldReachTailWhenPublisherStopWhileConsumerIsBehind() throws Exception {
        int messageCount = 100000;
        int messageLimit = messageCount * 2;
        AtomicLong lastConfirmed = new AtomicLong();
        CountDownLatch consumerStartLatch = new CountDownLatch(messageCount);
        Client publisher = cf.get(new Client.ClientParameters()
                .publishConfirmListener(publishingId -> {
                    lastConfirmed.set(publishingId);
                    consumerStartLatch.countDown();
                }));

        CountDownLatch consumedMessagesLatch = new CountDownLatch(messageLimit);
        AtomicReference<String> lastConsumedMessage = new AtomicReference<>();
        Client consumer = cf.get(new Client.ClientParameters()
                .chunkListener((client, subscriptionId, offset, msgCount, dataSize) -> client.credit(0, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    lastConsumedMessage.set(new String(message.getBodyAsBinary()));
                    consumedMessagesLatch.countDown();
                }));

        AtomicBoolean publisherHasStopped = new AtomicBoolean(false);
        new Thread(() -> {
            int publishedMessageCount = 0;
            while (true) {
                publisher.publish(stream,
                        Collections.singletonList(publisher.messageBuilder().addData(String.valueOf(publishedMessageCount).getBytes()).build()));
                if (++publishedMessageCount == messageLimit) {
                    break;
                }
            }
            publisherHasStopped.set(true);
        }).start();

        assertThat(consumerStartLatch.await(10, SECONDS)).isTrue();

        assertThat(consumer.subscribe(0, stream, OffsetSpecification.first(), 10).isOk()).isTrue();
        assertThat(consumedMessagesLatch.await(10, SECONDS)).isTrue();
        assertThat(publisherHasStopped).isTrue();
        assertThat(lastConfirmed).hasValue(messageLimit - 1);
        assertThat(lastConsumedMessage).hasValue(String.valueOf(messageLimit - 1));
    }

}
