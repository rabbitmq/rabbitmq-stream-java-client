// Copyright (c) 2021-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_3_11_11;
import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static com.rabbitmq.stream.impl.TestUtils.wrap;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SuperStreamConsumerTest {

  EventLoopGroup eventLoopGroup;

  Environment environment;

  Client configurationClient;
  int partitionCount = 3;
  String superStream;
  String[] routingKeys = null;
  TestUtils.ClientFactory cf;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
    environment = environmentBuilder.build();
    superStream = TestUtils.streamName(info);
    configurationClient = cf.get();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
    deleteSuperStreamTopology(configurationClient, superStream);
  }

  private static void publishToPartitions(
      TestUtils.ClientFactory cf, List<String> partitions, int messageCount) {
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));
    for (int i = 0; i < partitions.size(); i++) {
      assertThat(client.declarePublisher(b(i), null, partitions.get(i)).isOk()).isTrue();
    }
    for (int i = 0; i < messageCount; i++) {
      int partitionIndex = i % partitions.size();
      String partition = partitions.get(partitionIndex);
      client.publish(
          b(partitionIndex),
          Collections.singletonList(
              client.messageBuilder().addData(partition.getBytes(StandardCharsets.UTF_8)).build()));
    }
    latchAssert(publishLatch).completes();
  }

  static AutoCloseable publishToPartitions(TestUtils.ClientFactory cf, List<String> partitions) {
    Client client = cf.get();
    for (int i = 0; i < partitions.size(); i++) {
      assertThat(client.declarePublisher(b(i), null, partitions.get(i)).isOk()).isTrue();
    }
    Runnable publish =
        () -> {
          int count = 0;
          while (!Thread.currentThread().isInterrupted()) {
            int partitionIndex = count++ % partitions.size();
            String partition = partitions.get(partitionIndex);
            client.publish(
                b(partitionIndex),
                Collections.singletonList(
                    client
                        .messageBuilder()
                        .addData(partition.getBytes(StandardCharsets.UTF_8))
                        .build()));
            try {
              Thread.sleep(10);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        };
    Thread thread = new Thread(publish);
    thread.start();
    return thread::interrupt;
  }

  @Test
  void consumeAllMessagesFromAllPartitions() {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    Client client = cf.get();
    List<String> partitions = client.partitions(superStream);
    int messageCount = 10000 * partitionCount;
    publishToPartitions(cf, partitions, messageCount);
    ConcurrentMap<String, AtomicInteger> messagesReceived = new ConcurrentHashMap<>(partitionCount);
    partitions.forEach(p -> messagesReceived.put(p, new AtomicInteger(0)));
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Consumer consumer =
        environment
            .consumerBuilder()
            .superStream(superStream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  String partition = new String(message.getBodyAsBinary());
                  messagesReceived.get(partition).incrementAndGet();
                  consumeLatch.countDown();
                })
            .build();
    latchAssert(consumeLatch).completes();
    assertThat(messagesReceived).hasSize(partitionCount);
    partitions.forEach(
        p -> {
          assertThat(messagesReceived).containsKey(p);
          assertThat(messagesReceived.get(p).get()).isEqualTo(messageCount / partitionCount);
        });
    consumer.close();
  }

  @Test
  void manualOffsetTrackingShouldStoreOnAllPartitions() {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    Client client = cf.get();
    List<String> partitions = client.partitions(superStream);
    int messageCount = 10000 * partitionCount;
    publishToPartitions(cf, partitions, messageCount);
    ConcurrentMap<String, AtomicInteger> messagesReceived = new ConcurrentHashMap<>(partitionCount);
    ConcurrentMap<String, Long> lastOffsets = new ConcurrentHashMap<>(partitionCount);
    partitions.forEach(
        p -> {
          messagesReceived.put(p, new AtomicInteger(0));
        });
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    String consumerName = "my-app";
    AtomicInteger totalCount = new AtomicInteger();
    Consumer consumer =
        environment
            .consumerBuilder()
            .superStream(superStream)
            .offset(OffsetSpecification.first())
            .name(consumerName)
            .manualTrackingStrategy()
            .builder()
            .messageHandler(
                (context, message) -> {
                  String partition = new String(message.getBodyAsBinary());
                  messagesReceived.get(partition).incrementAndGet();
                  lastOffsets.put(partition, context.offset());
                  totalCount.incrementAndGet();
                  if (totalCount.get() % 50 == 0) {
                    context.storeOffset();
                  }
                  consumeLatch.countDown();
                })
            .build();
    latchAssert(consumeLatch).completes();
    assertThat(messagesReceived).hasSize(partitionCount);
    partitions.forEach(
        p -> {
          assertThat(messagesReceived).containsKey(p);
          assertThat(messagesReceived.get(p).get()).isEqualTo(messageCount / partitionCount);
        });
    // checking stored offsets are big enough
    // offset near the end (the message count per partition minus a few messages)
    long almostLastOffset = messageCount / partitionCount - messageCount / (partitionCount * 10);
    partitions.forEach(
        wrap(
            p -> {
              waitAtMost(
                  () -> {
                    QueryOffsetResponse response = client.queryOffset(consumerName, p);
                    return response.isOk() && response.getOffset() > almostLastOffset;
                  });
            }));
    consumer.close();
  }

  @Test
  void autoOffsetTrackingShouldStoreOnAllPartitions() {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    Client client = cf.get();
    List<String> partitions = client.partitions(superStream);
    int messageCount = 10000 * partitionCount;
    publishToPartitions(cf, partitions, messageCount);
    ConcurrentMap<String, AtomicInteger> messagesReceived = new ConcurrentHashMap<>(partitionCount);
    ConcurrentMap<String, Long> lastOffsets = new ConcurrentHashMap<>(partitionCount);
    partitions.forEach(
        p -> {
          messagesReceived.put(p, new AtomicInteger(0));
        });
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    String consumerName = "my-app";
    AtomicInteger totalCount = new AtomicInteger();
    Consumer consumer =
        environment
            .consumerBuilder()
            .superStream(superStream)
            .offset(OffsetSpecification.first())
            .name(consumerName)
            .autoTrackingStrategy()
            .messageCountBeforeStorage(messageCount / partitionCount / 50)
            .builder()
            .messageHandler(
                (context, message) -> {
                  String partition = new String(message.getBodyAsBinary());
                  messagesReceived.get(partition).incrementAndGet();
                  lastOffsets.put(partition, context.offset());
                  totalCount.incrementAndGet();
                  consumeLatch.countDown();
                })
            .build();
    latchAssert(consumeLatch).completes();
    assertThat(messagesReceived).hasSize(partitionCount);
    partitions.forEach(
        p -> {
          assertThat(messagesReceived).containsKey(p);
          assertThat(messagesReceived.get(p).get()).isEqualTo(messageCount / partitionCount);
        });
    // checking stored offsets are big enough
    // offset near the end (the message count per partition minus a few messages)
    long almostLastOffset = messageCount / partitionCount - messageCount / (partitionCount * 10);
    partitions.forEach(
        wrap(
            p -> {
              waitAtMost(
                  () -> {
                    QueryOffsetResponse response = client.queryOffset(consumerName, p);
                    return response.isOk() && response.getOffset() > almostLastOffset;
                  });
            }));
    consumer.close();
  }

  @Test
  void autoOffsetTrackingShouldStoreOffsetZero() {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    Client client = cf.get();
    List<String> partitions = client.partitions(superStream);
    int messageCount = partitionCount;
    publishToPartitions(cf, partitions, messageCount);
    ConcurrentMap<String, AtomicInteger> messagesReceived = new ConcurrentHashMap<>(partitionCount);
    ConcurrentMap<String, Long> lastOffsets = new ConcurrentHashMap<>(partitionCount);
    partitions.forEach(p -> messagesReceived.put(p, new AtomicInteger(0)));
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    String consumerName = "my-app";
    AtomicInteger totalCount = new AtomicInteger();
    Consumer consumer =
        environment
            .consumerBuilder()
            .superStream(superStream)
            .offset(OffsetSpecification.first())
            .name(consumerName)
            .autoTrackingStrategy()
            .flushInterval(Duration.ofHours(1)) // long enough
            .builder()
            .messageHandler(
                (context, message) -> {
                  String partition = new String(message.getBodyAsBinary());
                  messagesReceived.get(partition).incrementAndGet();
                  lastOffsets.put(partition, context.offset());
                  totalCount.incrementAndGet();
                  consumeLatch.countDown();
                })
            .build();
    latchAssert(consumeLatch).completes();
    assertThat(messagesReceived).hasSize(partitionCount);
    partitions.forEach(
        p -> {
          assertThat(messagesReceived).containsKey(p);
          assertThat(messagesReceived.get(p).get()).isEqualTo(messageCount / partitionCount);
        });
    consumer.close();
    partitions.forEach(
        wrap(
            p -> {
              assertThat(lastOffsets.get(p)).isZero();
              waitAtMost(
                  () -> {
                    QueryOffsetResponse response = client.queryOffset(consumerName, p);
                    return response.isOk() && response.getOffset() == lastOffsets.get(p);
                  },
                  () ->
                      format(
                          "Expecting stored offset %d on stream '%s', but got %d",
                          lastOffsets.get(p), p, client.queryOffset(consumerName, p).getOffset()));
            }));
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_3_11_11)
  void rebalancedPartitionShouldGetMessagesWhenItComesBackToOriginalConsumerInstance()
      throws Exception {
    Duration timeout = Duration.ofSeconds(60);
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    Client client = cf.get();
    List<String> partitions = client.partitions(superStream);
    try (AutoCloseable publish = publishToPartitions(cf, partitions)) {
      int messageCountBeforeStorage = 10;
      String consumerName = "my-app";
      Set<String> receivedPartitions = ConcurrentHashMap.newKeySet(partitionCount);
      Consumer consumer1 =
          environment
              .consumerBuilder()
              .superStream(superStream)
              .singleActiveConsumer()
              .offset(OffsetSpecification.first())
              .name(consumerName)
              .autoTrackingStrategy()
              .messageCountBeforeStorage(messageCountBeforeStorage)
              .builder()
              .messageHandler(
                  (context, message) -> {
                    receivedPartitions.add(context.stream());
                  })
              .build();
      waitAtMost(
          timeout,
          () -> receivedPartitions.size() == partitions.size(),
          () ->
              format(
                  "Expected to receive messages from all partitions, got %s", receivedPartitions));

      AtomicReference<String> partition = new AtomicReference<>();
      Consumer consumer2 =
          environment
              .consumerBuilder()
              .superStream(superStream)
              .singleActiveConsumer()
              .offset(OffsetSpecification.first())
              .name(consumerName)
              .autoTrackingStrategy()
              .messageCountBeforeStorage(messageCountBeforeStorage)
              .builder()
              .messageHandler(
                  (context, message) -> {
                    partition.set(context.stream());
                  })
              .build();
      waitAtMost(timeout, () -> partition.get() != null);
      consumer2.close();
      receivedPartitions.clear();
      waitAtMost(
          timeout,
          () -> receivedPartitions.size() == partitions.size(),
          () ->
              format(
                  "Expected to receive messages from all partitions, got %s", receivedPartitions));
      consumer1.close();
    }
  }
}
