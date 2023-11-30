// Copyright (c) 2021-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
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
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.compression.CommonsCompressCompressionCodecFactory;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.compression.DefaultCompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.metrics.MetricsCollector;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SubEntryBatchingTest {
  static final Charset UTF8 = StandardCharsets.UTF_8;

  TestUtils.ClientFactory cf;

  String stream;

  static Stream<CompressionCodecFactory> compressionCodecFactories() {
    return Stream.of(
        new CommonsCompressCompressionCodecFactory(), new DefaultCompressionCodecFactory());
  }

  @ParameterizedTest
  @MethodSource("compressionCodecFactories")
  void publishConsumeCompressedMessages(
      CompressionCodecFactory compressionCodecFactory, TestInfo info) {
    Map<Compression, Integer> compressionToReadBytes = new HashMap<>();
    for (Compression compression : Compression.values()) {
      int batchCount = 100;
      int messagesInBatch = 30;
      int messageCount = batchCount * messagesInBatch;
      CountDownLatch publishLatch = new CountDownLatch(batchCount);
      Client publisher =
          cf.get(
              new ClientParameters()
                  .compressionCodecFactory(compressionCodecFactory)
                  .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

      String s = TestUtils.streamName(info) + "_" + compression.name();

      try {
        Response response = publisher.create(s);
        assertThat(response.isOk()).isTrue();
        response = publisher.declarePublisher(b(0), null, s);
        assertThat(response.isOk()).isTrue();
        Set<String> publishedBodies = ConcurrentHashMap.newKeySet(messageCount);
        IntStream.range(0, batchCount)
            .forEach(
                batchIndex -> {
                  MessageBatch messageBatch = new MessageBatch(compression);
                  IntStream.range(0, messagesInBatch)
                      .forEach(
                          messageIndex -> {
                            String body = "batch " + batchIndex + " message " + messageIndex;
                            messageBatch.add(
                                publisher.messageBuilder().addData(body.getBytes(UTF8)).build());
                            publishedBodies.add(body);
                          });
                  publisher.publishBatches(b(0), Collections.singletonList(messageBatch));
                });

        assertThat(latchAssert(publishLatch)).completes();

        Set<String> consumedBodies = ConcurrentHashMap.newKeySet(batchCount * messagesInBatch);
        CountDownLatch consumeLatch = new CountDownLatch(batchCount * messagesInBatch);
        CountMetricsCollector metricsCollector = new CountMetricsCollector();
        Client consumer =
            cf.get(
                new ClientParameters()
                    .compressionCodecFactory(compressionCodecFactory)
                    .chunkListener(TestUtils.credit())
                    .messageListener(
                        (subscriptionId,
                            offset,
                            chunkTimestamp,
                            committedChunkId,
                            chunkContext,
                            message) -> {
                          consumedBodies.add(new String(message.getBodyAsBinary(), UTF8));
                          consumeLatch.countDown();
                        })
                    .metricsCollector(metricsCollector));

        response = consumer.subscribe(b(1), s, OffsetSpecification.first(), 2);
        assertThat(response.isOk()).isTrue();

        assertThat(latchAssert(consumeLatch)).completes();
        assertThat(consumedBodies).hasSize(messageCount).hasSameSizeAs(publishedBodies);
        publishedBodies.forEach(
            publishedBody -> assertThat(consumedBodies.contains(publishedBody)).isTrue());
        compressionToReadBytes.put(compression, metricsCollector.readBytes.get());
      } finally {
        Response response = publisher.delete(s);
        assertThat(response.isOk()).isTrue();
      }
    }
    int plainReadBytes = compressionToReadBytes.get(Compression.NONE);
    Arrays.stream(Compression.values())
        .filter(comp -> comp != Compression.NONE)
        .forEach(
            compression -> {
              assertThat(compressionToReadBytes.get(compression)).isLessThan(plainReadBytes);
            });
  }

  @Test
  void subEntriesCompressedWithDifferentCompressionsShouldBeReadCorrectly() {
    List<CompressionCodecFactory> compressionCodecFactories =
        compressionCodecFactories().collect(Collectors.toList());
    int batchCount = compressionCodecFactories.size() * Compression.values().length;
    int messagesInBatch = 30;
    int messageCount = batchCount * messagesInBatch;
    AtomicInteger messageIndex = new AtomicInteger(0);
    CountDownLatch publishLatch = new CountDownLatch(batchCount);
    Set<String> publishedBodies = ConcurrentHashMap.newKeySet(messageCount);
    compressionCodecFactories.forEach(
        compressionCodecFactory -> {
          Client publisher =
              cf.get(
                  new ClientParameters()
                      .compressionCodecFactory(compressionCodecFactory)
                      .publishConfirmListener(
                          (publisherId, publishingId) -> publishLatch.countDown()));
          Response response = publisher.declarePublisher(b(0), null, stream);
          assertThat(response.isOk()).isTrue();
          for (Compression compression : Compression.values()) {
            MessageBatch messageBatch = new MessageBatch(compression);
            IntStream.range(0, messagesInBatch)
                .forEach(
                    i -> {
                      String body =
                          "compression "
                              + compression.name()
                              + " message "
                              + messageIndex.getAndIncrement();
                      messageBatch.add(
                          publisher.messageBuilder().addData(body.getBytes(UTF8)).build());
                      publishedBodies.add(body);
                    });
            publisher.publishBatches(b(0), Collections.singletonList(messageBatch));
          }
        });
    assertThat(latchAssert(publishLatch)).completes();

    compressionCodecFactories.forEach(
        compressionCodecFactory -> {
          CountDownLatch consumeLatch = new CountDownLatch(messageCount);
          Set<String> consumedBodies = ConcurrentHashMap.newKeySet(messageCount);
          Client consumer =
              cf.get(
                  new ClientParameters()
                      .compressionCodecFactory(compressionCodecFactory)
                      .chunkListener(TestUtils.credit())
                      .messageListener(
                          (subscriptionId,
                              offset,
                              chunkTimestamp,
                              committedChunkId,
                              chunkContext,
                              message) -> {
                            consumedBodies.add(new String(message.getBodyAsBinary(), UTF8));
                            consumeLatch.countDown();
                          }));

          Response response = consumer.subscribe(b(1), stream, OffsetSpecification.first(), 2);
          assertThat(response.isOk()).isTrue();
          assertThat(latchAssert(consumeLatch)).completes();
          assertThat(consumedBodies).hasSize(messageCount).hasSameSizeAs(publishedBodies);
          publishedBodies.forEach(
              publishBody -> assertThat(consumedBodies.contains(publishBody)).isTrue());
        });
  }

  private static class CountMetricsCollector implements MetricsCollector {

    private final AtomicInteger readBytes = new AtomicInteger(0);

    @Override
    public void openConnection() {}

    @Override
    public void closeConnection() {}

    @Override
    public void publish(int count) {}

    @Override
    public void publishConfirm(int count) {}

    @Override
    public void publishError(int count) {}

    @Override
    public void chunk(int entriesCount) {}

    @Override
    public void consume(long count) {}

    @Override
    public void writtenBytes(int writtenBytes) {}

    @Override
    public void readBytes(int readBytes) {
      this.readBytes.addAndGet(readBytes);
    }
  }
}
