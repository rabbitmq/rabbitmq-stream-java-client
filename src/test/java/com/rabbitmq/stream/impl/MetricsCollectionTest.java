// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
// Inc. and/or its subsidiaries.
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

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.metrics.MetricsCollector;
import io.netty.channel.EventLoopGroup;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class MetricsCollectionTest {

  String stream;
  TestUtils.ClientFactory cf;
  EventLoopGroup eventLoopGroup;
  CountMetricsCollector metricsCollector;

  @BeforeEach
  void init() {
    metricsCollector = new CountMetricsCollector();
  }

  @Test
  void connectionCountShouldIncreaseAndDecrease() {
    assertThat(metricsCollector.connections.get()).isZero();
    Client c1 = cf.get(new ClientParameters().metricsCollector(metricsCollector));
    assertThat(metricsCollector.connections.get()).isEqualTo(1);
    Client c2 = cf.get(new ClientParameters().metricsCollector(metricsCollector));
    assertThat(metricsCollector.connections.get()).isEqualTo(2);
    c2.close();
    assertThat(metricsCollector.connections.get()).isEqualTo(1);
    c1.close();
    assertThat(metricsCollector.connections.get()).isZero();
    c1.close();
    assertThat(metricsCollector.connections.get()).isZero();
  }

  @Test
  void publishConfirmChunkConsumeShouldBeCollected() throws Exception {
    int messageCount = 1000;
    assertThat(metricsCollector.writtenBytes.get()).isZero();
    assertThat(metricsCollector.readBytes.get()).isZero();
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    publisher.declarePublisher(b(1), null, stream);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publisher.publish(
                    b(1),
                    Collections.singletonList(
                        publisher.messageBuilder().addData("".getBytes()).build())));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS));
    assertThat(metricsCollector.publish.get()).isEqualTo(messageCount);
    assertThat(metricsCollector.confirm.get()).isEqualTo(messageCount);
    assertThat(metricsCollector.error.get()).isZero();

    assertThat(metricsCollector.writtenBytes.get()).isPositive();
    assertThat(metricsCollector.readBytes.get()).isPositive();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> consumeLatch.countDown()));

    Client.Response response = consumer.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(metricsCollector.chunk.get()).isPositive();
    waitAtMost(() -> metricsCollector.entriesInChunk.get() == messageCount);
    waitAtMost(() -> metricsCollector.consume.get() == messageCount);
  }

  @Test
  void publishErrorShouldBeCollected() throws Exception {
    int messageCount = 1000;
    CountDownLatch publishErrorLatch = new CountDownLatch(messageCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishErrorListener(
                    (publisherId, publishingId, errorCode) -> publishErrorLatch.countDown()));

    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publisher.publish(
                    b(1),
                    Collections.singletonList(
                        publisher.messageBuilder().addData("".getBytes()).build())));

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
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    publisher.declarePublisher(b(1), null, stream);
    IntStream.range(0, batchCount)
        .forEach(
            batchIndex -> {
              MessageBatch messageBatch = new MessageBatch(Compression.NONE);
              IntStream.range(0, messagesInBatch)
                  .forEach(
                      messageIndex -> {
                        messageBatch.add(publisher.messageBuilder().addData("".getBytes()).build());
                      });
              publisher.publishBatches(b(1), Collections.singletonList(messageBatch));
            });

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    assertThat(metricsCollector.publish.get()).isEqualTo(messageCount);
    assertThat(metricsCollector.confirm.get())
        .isEqualTo(batchCount); // one confirm per sub batch entry
    assertThat(metricsCollector.error.get()).isZero();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .metricsCollector(metricsCollector)
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {
                      consumeLatch.countDown();
                    }));

    Client.Response response = consumer.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();

    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

    assertThat(metricsCollector.chunk.get()).isPositive();
    waitAtMost(() -> metricsCollector.entriesInChunk.get() == batchCount);
    waitAtMost(() -> metricsCollector.consume.get() == messageCount);
  }

  @Test
  void filteredSmallerOffsetsInChunksShouldNotBeCounted() throws Exception {
    int messageCount = 50000;
    AtomicLong messageIdSequence = new AtomicLong(0);
    TestUtils.publishAndWaitForConfirms(
        cf,
        builder ->
            builder
                .properties()
                .messageId(messageIdSequence.incrementAndGet())
                .messageBuilder()
                .build(),
        messageCount,
        stream);
    for (int i = 0; i < 10; i++) {
      Map<Byte, CountDownLatch> latches = new ConcurrentHashMap<>();
      latches.put(b(1), new CountDownLatch(1));
      latches.put(b(2), new CountDownLatch(1));
      Map<Byte, AtomicLong> counts = new ConcurrentHashMap<>();
      counts.put(b(1), new AtomicLong());
      counts.put(b(2), new AtomicLong());
      Map<Byte, AtomicLong> expectedCounts = new ConcurrentHashMap<>();
      expectedCounts.put(b(1), new AtomicLong(messageCount - 50));
      expectedCounts.put(b(2), new AtomicLong(messageCount - 100));
      CountMetricsCollector metricsCollector = new CountMetricsCollector();
      Client client =
          new Client(
              new ClientParameters()
                  .messageListener(
                      (subscriptionId,
                          offset,
                          chunkTimestamp,
                          committedChunkId,
                          chunkContext,
                          message) -> {
                        counts.get(subscriptionId).incrementAndGet();
                        if (message.getProperties().getMessageIdAsLong() == messageCount) {
                          latches.get(subscriptionId).countDown();
                        }
                      })
                  .chunkListener(TestUtils.credit())
                  .metricsCollector(metricsCollector)
                  .eventLoopGroup(eventLoopGroup));
      client.subscribe(b(1), stream, OffsetSpecification.offset(50), 10);
      client.subscribe(b(2), stream, OffsetSpecification.offset(100), 10);

      assertThat(latches.get(b(1)).await(10, SECONDS)).isTrue();
      assertThat(latches.get(b(2)).await(10, SECONDS)).isTrue();

      waitAtMost(() -> counts.get(b(1)).get() == expectedCounts.get(b(1)).get());
      waitAtMost(() -> counts.get(b(2)).get() == expectedCounts.get(b(2)).get());
      waitAtMost(
          () ->
              metricsCollector.consume.get()
                  == expectedCounts.values().stream()
                      .mapToLong(v -> v.get())
                      .reduce(0, (a, b) -> a + b));

      client.close();
    }
  }

  private static class CountMetricsCollector implements MetricsCollector {

    private final AtomicLong connections = new AtomicLong();
    private final AtomicLong publish = new AtomicLong(0);
    private final AtomicLong confirm = new AtomicLong(0);
    private final AtomicLong error = new AtomicLong(0);
    private final AtomicLong chunk = new AtomicLong(0);
    private final AtomicLong entriesInChunk = new AtomicLong(0);
    private final AtomicLong consume = new AtomicLong(0);
    private final AtomicLong writtenBytes = new AtomicLong(0);
    private final AtomicLong readBytes = new AtomicLong(0);

    @Override
    public void openConnection() {
      connections.incrementAndGet();
    }

    @Override
    public void closeConnection() {
      connections.decrementAndGet();
    }

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

    @Override
    public void writtenBytes(int writtenBytes) {
      this.writtenBytes.addAndGet(writtenBytes);
    }

    @Override
    public void readBytes(int readBytes) {
      this.readBytes.addAndGet(readBytes);
    }
  }
}
