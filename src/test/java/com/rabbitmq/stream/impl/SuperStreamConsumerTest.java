// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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
import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerUpdateListener.Status;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.SingleActiveConsumer;
import com.rabbitmq.stream.impl.Utils.CompositeConsumerUpdateListener;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SuperStreamConsumerTest {

  EventLoopGroup eventLoopGroup;

  Environment environment;

  Connection connection;
  int partitionCount = 3;
  String superStream;
  String[] routingKeys = null;
  TestUtils.ClientFactory cf;

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

  @BeforeEach
  void init(TestInfo info) throws Exception {
    EnvironmentBuilder environmentBuilder = Environment.builder().eventLoopGroup(eventLoopGroup);
    environmentBuilder.addressResolver(add -> localhost());
    environment = environmentBuilder.build();
    superStream = TestUtils.streamName(info);
    connection = new ConnectionFactory().newConnection();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
    if (routingKeys == null) {
      deleteSuperStreamTopology(connection, superStream, partitionCount);
    } else {
      deleteSuperStreamTopology(connection, superStream, routingKeys);
    }
    connection.close();
  }

  @Test
  void consumeAllMessagesFromAllPartitions() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitionCount);
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
  void manualOffsetTrackingShouldStoreOnAllPartitions() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitionCount);
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
        p ->
            assertThat(client.queryOffset(consumerName, p).getOffset())
                .isGreaterThan(almostLastOffset));
    consumer.close();
  }

  @Test
  void autoOffsetTrackingShouldStoreOnAllPartitions() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitionCount);
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
        p ->
            assertThat(client.queryOffset(consumerName, p).getOffset())
                .isGreaterThan(almostLastOffset));
    consumer.close();
  }

  @Test
  @SingleActiveConsumer
  void sacShouldSpreadAcrossPartitions() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitionCount);
    List<String> partitions =
        IntStream.range(0, partitionCount)
            .mapToObj(i -> superStream + "-" + i)
            .collect(Collectors.toList());
    Map<String, Status> consumerStates = new ConcurrentHashMap<>();
    String consumerName = "my-app";
    Function<String, Consumer> consumerCreator =
        consumer ->
            environment
                .consumerBuilder()
                .singleActiveConsumer()
                .superStream(superStream)
                .offset(OffsetSpecification.first())
                .name(consumerName)
                .autoTrackingStrategy()
                .builder()
                .messageHandler((context, message) -> {})
                .consumerUpdateListener(
                    context -> {
                      consumerStates.put(consumer + context.stream(), context.status());
                      return null;
                    })
                .build();

    Consumer consumer0 = consumerCreator.apply("0");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0)) == Status.ACTIVE
                && consumerStates.get("0" + partitions.get(1)) == Status.ACTIVE
                && consumerStates.get("0" + partitions.get(2)) == Status.ACTIVE);

    Consumer consumer1 = consumerCreator.apply("1");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0)) == Status.ACTIVE
                && consumerStates.get("1" + partitions.get(1)) == Status.ACTIVE
                && consumerStates.get("0" + partitions.get(2)) == Status.ACTIVE);

    Consumer consumer2 = consumerCreator.apply("2");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0)) == Status.ACTIVE
                && consumerStates.get("1" + partitions.get(1)) == Status.ACTIVE
                && consumerStates.get("2" + partitions.get(2)) == Status.ACTIVE);

    waitAtMost(() -> consumerStates.size() == 9);
    assertThat(consumerStates)
        .containsEntry("0" + partitions.get(0), Status.ACTIVE)
        .containsEntry("0" + partitions.get(1), Status.PASSIVE)
        .containsEntry("0" + partitions.get(2), Status.PASSIVE)
        .containsEntry("1" + partitions.get(0), Status.PASSIVE)
        .containsEntry("1" + partitions.get(1), Status.ACTIVE)
        .containsEntry("1" + partitions.get(2), Status.PASSIVE)
        .containsEntry("2" + partitions.get(0), Status.PASSIVE)
        .containsEntry("2" + partitions.get(1), Status.PASSIVE)
        .containsEntry("2" + partitions.get(2), Status.ACTIVE);

    consumer0.close();

    waitAtMost(
        () ->
            consumerStates.get("1" + partitions.get(0)) == Status.ACTIVE
                && consumerStates.get("2" + partitions.get(1)) == Status.ACTIVE
                && consumerStates.get("1" + partitions.get(2)) == Status.ACTIVE);

    assertThat(consumerStates)
        .containsEntry("1" + partitions.get(0), Status.ACTIVE)
        .containsEntry("1" + partitions.get(1), Status.PASSIVE)
        .containsEntry("1" + partitions.get(2), Status.ACTIVE)
        .containsEntry("2" + partitions.get(0), Status.PASSIVE)
        .containsEntry("2" + partitions.get(1), Status.ACTIVE)
        .containsEntry("2" + partitions.get(2), Status.PASSIVE);

    consumer1.close();

    waitAtMost(
        () ->
            consumerStates.get("2" + partitions.get(0)) == Status.ACTIVE
                && consumerStates.get("2" + partitions.get(1)) == Status.ACTIVE
                && consumerStates.get("2" + partitions.get(2)) == Status.ACTIVE);

    consumer2.close();
  }

  @Test
  @SingleActiveConsumer
  void sacAutoOffsetTrackingShouldStoreOnRelanbancing() throws Exception {
    declareSuperStreamTopology(connection, superStream, partitionCount);
    int messageCount = 5_000;
    AtomicInteger messageWaveCount = new AtomicInteger();
    int superConsumerCount = 3;
    List<String> partitions =
        IntStream.range(0, partitionCount)
            .mapToObj(i -> superStream + "-" + i)
            .collect(Collectors.toList());
    Map<String, Boolean> consumerStates =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessages =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessagesPerPartitions =
        new ConcurrentHashMap<>(partitionCount);
    IntStream.range(0, superConsumerCount)
        .mapToObj(String::valueOf)
        .forEach(
            consumer ->
                partitions.forEach(
                    partition -> {
                      consumerStates.put(consumer + partition, false);
                      receivedMessages.put(consumer + partition, new AtomicInteger(0));
                    }));
    Map<String, Long> lastReceivedOffsets = new ConcurrentHashMap<>();
    partitions.forEach(
        partition -> {
          lastReceivedOffsets.put(partition, 0L);
          receivedMessagesPerPartitions.put(partition, new AtomicInteger(0));
        });
    Runnable publishOnAllPartitions =
        () -> {
          partitions.forEach(
              partition -> TestUtils.publishAndWaitForConfirms(cf, messageCount, partition));
          messageWaveCount.incrementAndGet();
        };
    String consumerName = "my-app";

    Function<String, Consumer> consumerCreator =
        consumer -> {
          CompositeConsumerUpdateListener consumerUpdateListener =
              new CompositeConsumerUpdateListener();
          consumerUpdateListener.add(
              context -> {
                consumerStates.put(consumer + context.stream(), context.status() == Status.ACTIVE);
                return null;
              });
          return environment
              .consumerBuilder()
              .singleActiveConsumer()
              .superStream(superStream)
              .offset(OffsetSpecification.first())
              .name(consumerName)
              .autoTrackingStrategy()
              .builder()
              .consumerUpdateListener(consumerUpdateListener)
              .messageHandler(
                  (context, message) -> {
                    lastReceivedOffsets.put(context.stream(), context.offset());
                    receivedMessagesPerPartitions.get(context.stream()).incrementAndGet();
                    receivedMessages.get(consumer + context.stream()).incrementAndGet();
                  })
              .build();
        };

    Consumer consumer0 = consumerCreator.apply("0");
    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("0" + partitions.get(1))
                && consumerStates.get("0" + partitions.get(2)));

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("0" + partitions.get(0)).get() == messageCount
                && receivedMessages.get("0" + partitions.get(1)).get() == messageCount
                && receivedMessages.get("0" + partitions.get(2)).get() == messageCount);

    Consumer consumer1 = consumerCreator.apply("1");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("1" + partitions.get(1))
                && consumerStates.get("0" + partitions.get(2)));

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("0" + partitions.get(0)).get() == messageCount * 2
                && receivedMessages.get("1" + partitions.get(1)).get() == messageCount
                && receivedMessages.get("0" + partitions.get(2)).get() == messageCount * 2);

    Consumer consumer2 = consumerCreator.apply("2");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("1" + partitions.get(1))
                && consumerStates.get("2" + partitions.get(2)));

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("0" + partitions.get(0)).get() == messageCount * 3
                && receivedMessages.get("1" + partitions.get(1)).get() == messageCount * 2
                && receivedMessages.get("2" + partitions.get(2)).get() == messageCount);

    consumer0.close();

    waitAtMost(
        () ->
            consumerStates.get("1" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("1" + partitions.get(2)));

    assertThat(consumerStates)
        .containsEntry("0" + partitions.get(0), true) // not changed after closing
        .containsEntry("0" + partitions.get(1), false) // not changed after closing
        .containsEntry("0" + partitions.get(2), false) // not changed after closing
        .containsEntry("1" + partitions.get(0), true) // now active
        .containsEntry("1" + partitions.get(1), false) // changed after rebalancing
        .containsEntry("1" + partitions.get(2), true) // now active
        .containsEntry("2" + partitions.get(0), false) // not changed
        .containsEntry("2" + partitions.get(1), true) // now active
        .containsEntry("2" + partitions.get(2), false); // changed after rebalancing

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("1" + partitions.get(0)).get() == messageCount
                && receivedMessages.get("2" + partitions.get(1)).get() == messageCount
                && receivedMessages.get("1" + partitions.get(2)).get() == messageCount);

    consumer1.close();

    waitAtMost(
        () ->
            consumerStates.get("2" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("2" + partitions.get(2)));

    assertThat(consumerStates)
        .containsEntry("0" + partitions.get(0), true) // not changed after closing
        .containsEntry("0" + partitions.get(1), false) // not changed after closing
        .containsEntry("0" + partitions.get(2), false) // not changed after closing
        .containsEntry("1" + partitions.get(0), true) // not changed after closing
        .containsEntry("1" + partitions.get(1), false) // not changed after closing
        .containsEntry("1" + partitions.get(2), true) // not changed after closing
        .containsEntry("2" + partitions.get(0), true) // now active
        .containsEntry("2" + partitions.get(1), true) // now active
        .containsEntry("2" + partitions.get(2), true); // now active

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("2" + partitions.get(0)).get() == messageCount
                && receivedMessages.get("2" + partitions.get(1)).get() == messageCount * 2
                && receivedMessages.get("2" + partitions.get(2)).get() == messageCount * 2);

    consumer2.close();

    assertThat(messageWaveCount).hasValue(5);
    assertThat(
            receivedMessages.values().stream()
                .map(AtomicInteger::get)
                .mapToInt(Integer::intValue)
                .sum())
        .isEqualTo(messageCount * partitionCount * 5);
    int expectedMessageCountPerPartition = messageCount * messageWaveCount.get();
    receivedMessagesPerPartitions
        .values()
        .forEach(v -> assertThat(v).hasValue(expectedMessageCountPerPartition));
    Client c = cf.get();
    partitions.forEach(
        partition ->
            assertThat(lastReceivedOffsets.get(partition))
                .isGreaterThanOrEqualTo(expectedMessageCountPerPartition)
                .isEqualTo(c.queryOffset(consumerName, partition)));
  }
}
