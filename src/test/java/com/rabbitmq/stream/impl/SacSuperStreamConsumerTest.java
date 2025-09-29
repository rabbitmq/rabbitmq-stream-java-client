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

import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.namedBiConsumer;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerUpdateListener;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast311Condition;
import com.rabbitmq.stream.impl.TestUtils.CallableBooleanSupplier;
import com.rabbitmq.stream.impl.TestUtils.SingleActiveConsumer;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import com.rabbitmq.stream.impl.Utils.CompositeConsumerUpdateListener;
import io.netty.channel.EventLoopGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({BrokerVersionAtLeast311Condition.class})
@StreamTestInfrastructure
@SingleActiveConsumer
public class SacSuperStreamConsumerTest {
  EventLoopGroup eventLoopGroup;

  Environment environment;

  Client configurationClient;
  int partitionCount = 3;
  String superStream;
  TestUtils.ClientFactory cf;

  @BeforeEach
  void init(TestInfo info) {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
    environment = environmentBuilder.build();
    superStream = TestUtils.streamName(info);
    configurationClient = cf.get();
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
  }

  @AfterEach
  void tearDown() {
    environment.close();
    deleteSuperStreamTopology(configurationClient, superStream);
  }

  @Test
  void sacShouldSpreadAcrossPartitions() throws Exception {
    List<String> partitions =
        IntStream.range(0, partitionCount).mapToObj(i -> superStream + "-" + i).collect(toList());
    Map<String, Boolean> consumerStates = new ConcurrentHashMap<>();
    int superConsumerCount = 3;
    IntStream.range(0, superConsumerCount)
        .mapToObj(String::valueOf)
        .forEach(
            consumer ->
                partitions.forEach(
                    partition -> {
                      consumerStates.put(consumer + partition, false);
                    }));
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
                      consumerStates.put(consumer + context.stream(), context.isActive());
                      return null;
                    })
                .build();

    Consumer consumer0 = consumerCreator.apply("0");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("0" + partitions.get(1))
                && consumerStates.get("0" + partitions.get(2)));

    Consumer consumer1 = consumerCreator.apply("1");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("1" + partitions.get(1))
                && consumerStates.get("0" + partitions.get(2)));

    Consumer consumer2 = consumerCreator.apply("2");

    waitAtMost(
        () ->
            consumerStates.get("0" + partitions.get(0))
                && consumerStates.get("1" + partitions.get(1))
                && consumerStates.get("2" + partitions.get(2)));

    // all 3 from sub 0 activated, then only 1 from sub 1 & sub 2
    assertThat(consumerStates)
        .containsEntry("0" + partitions.get(0), Boolean.TRUE)
        .containsEntry("0" + partitions.get(1), Boolean.FALSE)
        .containsEntry("0" + partitions.get(2), Boolean.FALSE)
        .containsEntry("1" + partitions.get(1), Boolean.TRUE)
        .containsEntry("2" + partitions.get(2), Boolean.TRUE);

    consumer0.close();

    waitAtMost(
        () ->
            consumerStates.get("1" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("1" + partitions.get(2)));

    assertThat(consumerStates)
        .containsEntry("1" + partitions.get(0), Boolean.TRUE)
        .containsEntry("1" + partitions.get(1), Boolean.FALSE)
        .containsEntry("1" + partitions.get(2), Boolean.TRUE)
        .containsEntry("2" + partitions.get(1), Boolean.TRUE)
        .containsEntry("2" + partitions.get(2), Boolean.FALSE);

    consumer1.close();

    waitAtMost(
        () ->
            consumerStates.get("2" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("2" + partitions.get(2)));

    consumer2.close();
  }

  @Test
  void sacAutoOffsetTrackingShouldStoreOnRelanbancing() throws Exception {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    int messageCount = 5_000;
    AtomicInteger messageWaveCount = new AtomicInteger();
    int superConsumerCount = 3;
    List<String> partitions =
        IntStream.range(0, partitionCount).mapToObj(i -> superStream + "-" + i).collect(toList());
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
                consumerStates.put(consumer + context.stream(), context.isActive());
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
        .containsEntry(
            "0" + partitions.get(0), false) // client library notifies the listener on closing
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
        .containsEntry(
            "0" + partitions.get(0), false) // client library notifies the listener on closing
        .containsEntry("0" + partitions.get(1), false) // not changed after closing
        .containsEntry("0" + partitions.get(2), false) // not changed after closing
        .containsEntry(
            "1" + partitions.get(0), false) // client library notifies the listener on closing
        .containsEntry("1" + partitions.get(1), false) // not changed after closing
        .containsEntry(
            "1" + partitions.get(2), false) // client library notifies the listener on closing
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
                .isGreaterThanOrEqualTo(expectedMessageCountPerPartition - 1)
                .isEqualTo(c.queryOffset(consumerName, partition).getOffset()));
  }

  @Test
  void sacManualOffsetTrackingShouldTakeOverOnRelanbancing() throws Exception {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    int messageCount = 5_000;
    int storeEvery = messageCount / 100;
    AtomicInteger messageWaveCount = new AtomicInteger();
    int superConsumerCount = 3;
    List<String> partitions =
        IntStream.range(0, partitionCount).mapToObj(i -> superStream + "-" + i).collect(toList());
    Map<String, Boolean> consumerStates =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessages =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessagesPerPartitions =
        new ConcurrentHashMap<>(partitionCount);
    Map<String, Map<String, Consumer>> consumers = new ConcurrentHashMap<>(superConsumerCount);
    IntStream.range(0, superConsumerCount)
        .mapToObj(String::valueOf)
        .forEach(
            consumer ->
                partitions.forEach(
                    partition -> {
                      consumers.put(consumer, new ConcurrentHashMap<>(partitionCount));
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

    OffsetSpecification initialOffsetSpecification = OffsetSpecification.first();
    Function<String, Consumer> consumerCreator =
        consumer -> {
          AtomicInteger received = new AtomicInteger();
          ConsumerUpdateListener consumerUpdateListener =
              context -> {
                consumers.get(consumer).putIfAbsent(context.stream(), context.consumer());
                consumerStates.put(consumer + context.stream(), context.isActive());
                OffsetSpecification offsetSpecification = null;
                if (context.isActive()) {
                  try {
                    long storedOffset = context.consumer().storedOffset() + 1;
                    offsetSpecification = OffsetSpecification.offset(storedOffset);
                  } catch (NoOffsetException e) {
                    offsetSpecification = initialOffsetSpecification;
                  }
                } else {
                  long lastReceivedOffset = lastReceivedOffsets.get(context.stream());
                  context.consumer().store(lastReceivedOffset);
                  waitUntil(() -> context.consumer().storedOffset() == lastReceivedOffset);
                }
                return offsetSpecification;
              };
          return environment
              .consumerBuilder()
              .singleActiveConsumer()
              .superStream(superStream)
              .offset(initialOffsetSpecification)
              .name(consumerName)
              .manualTrackingStrategy()
              .builder()
              .consumerUpdateListener(consumerUpdateListener)
              .messageHandler(
                  (context, message) -> {
                    if (received.incrementAndGet() % storeEvery == 0) {
                      context.storeOffset();
                    }
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

    java.util.function.Consumer<String> storeLastProcessedOffsets =
        consumerIndex -> {
          consumers.get(consumerIndex).entrySet().stream()
              .filter(
                  partitionToInnerConsumer -> {
                    // we take only the inner consumers that are active
                    return consumerStates.get(consumerIndex + partitionToInnerConsumer.getKey());
                  })
              .forEach(
                  entry -> {
                    String partition = entry.getKey();
                    Consumer consumer = entry.getValue();
                    long offset = lastReceivedOffsets.get(partition);
                    consumer.store(offset);
                    waitUntil(() -> consumer.storedOffset() == offset);
                  });
        };
    storeLastProcessedOffsets.accept("0");
    consumer0.close();

    waitAtMost(
        () ->
            consumerStates.get("1" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("1" + partitions.get(2)));

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("1" + partitions.get(0)).get() == messageCount
                && receivedMessages.get("2" + partitions.get(1)).get() == messageCount
                && receivedMessages.get("1" + partitions.get(2)).get() == messageCount);

    storeLastProcessedOffsets.accept("1");
    consumer1.close();

    waitAtMost(
        () ->
            consumerStates.get("2" + partitions.get(0))
                && consumerStates.get("2" + partitions.get(1))
                && consumerStates.get("2" + partitions.get(2)));

    publishOnAllPartitions.run();

    waitAtMost(
        () ->
            receivedMessages.get("2" + partitions.get(0)).get() == messageCount
                && receivedMessages.get("2" + partitions.get(1)).get() == messageCount * 2
                && receivedMessages.get("2" + partitions.get(2)).get() == messageCount * 2);

    storeLastProcessedOffsets.accept("2");
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
                .isGreaterThanOrEqualTo(expectedMessageCountPerPartition - 1)
                .isEqualTo(c.queryOffset(consumerName, partition).getOffset()));
  }

  @Test
  void sacExternalOffsetTrackingShouldTakeOverOnRelanbancing() throws Exception {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    int messageCount = 5_000;
    AtomicInteger messageWaveCount = new AtomicInteger();
    int superConsumerCount = 3;
    List<String> partitions =
        IntStream.range(0, partitionCount).mapToObj(i -> superStream + "-" + i).collect(toList());
    Map<String, Boolean> consumerStates =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessages =
        new ConcurrentHashMap<>(partitionCount * superConsumerCount);
    Map<String, AtomicInteger> receivedMessagesPerPartitions =
        new ConcurrentHashMap<>(partitionCount);
    Map<String, Map<String, Consumer>> consumers = new ConcurrentHashMap<>(superConsumerCount);
    IntStream.range(0, superConsumerCount)
        .mapToObj(String::valueOf)
        .forEach(
            consumer ->
                partitions.forEach(
                    partition -> {
                      consumers.put(consumer, new ConcurrentHashMap<>(partitionCount));
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

    OffsetSpecification initialOffsetSpecification = OffsetSpecification.first();
    Function<String, Consumer> consumerCreator =
        consumer -> {
          ConsumerUpdateListener consumerUpdateListener =
              context -> {
                consumers.get(consumer).putIfAbsent(context.stream(), context.consumer());
                consumerStates.put(consumer + context.stream(), context.isActive());
                OffsetSpecification offsetSpecification = null;
                if (context.isActive()) {
                  Long lastReceivedOffset = lastReceivedOffsets.get(context.stream());
                  offsetSpecification =
                      lastReceivedOffset == null
                          ? initialOffsetSpecification
                          : OffsetSpecification.offset(lastReceivedOffset + 1);
                }
                return offsetSpecification;
              };
          return environment
              .consumerBuilder()
              .singleActiveConsumer()
              .superStream(superStream)
              .offset(initialOffsetSpecification)
              .name(consumerName)
              .noTrackingStrategy()
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
    partitions.forEach(
        partition ->
            assertThat(lastReceivedOffsets.get(partition))
                .isGreaterThanOrEqualTo(expectedMessageCountPerPartition - 1));
  }

  @Test
  void consumerGroupsOnSameSuperStreamShouldBeIndependent() {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    int messageCount = 20_000;
    int consumerGroupsCount = 3;
    List<String> consumerNames =
        IntStream.range(0, consumerGroupsCount).mapToObj(i -> "my-app-" + i).collect(toList());
    List<String> partitions =
        IntStream.range(0, partitionCount).mapToObj(i -> superStream + "-" + i).collect(toList());
    Map<String, AtomicInteger> receivedMessages = new ConcurrentHashMap<>(consumerGroupsCount);
    Map<String, AtomicInteger> receivedMessagesPerPartitions =
        new ConcurrentHashMap<>(consumerGroupsCount * partitionCount);
    Map<String, Long> lastReceivedOffsets = new ConcurrentHashMap<>();
    partitions.forEach(
        partition -> {
          lastReceivedOffsets.put(partition, 0L);
          receivedMessagesPerPartitions.put(partition, new AtomicInteger(0));
        });

    List<Consumer> consumers = new ArrayList<>(consumerGroupsCount * partitionCount);
    consumerNames.forEach(
        consumerName -> {
          receivedMessages.put(consumerName, new AtomicInteger(0));
          partitions.forEach(
              partition -> {
                receivedMessagesPerPartitions.put(partition + consumerName, new AtomicInteger(0));
                Consumer consumer =
                    environment
                        .consumerBuilder()
                        .singleActiveConsumer()
                        .superStream(superStream)
                        .offset(OffsetSpecification.first())
                        .name(consumerName)
                        .autoTrackingStrategy()
                        .builder()
                        .messageHandler(
                            (context, message) -> {
                              lastReceivedOffsets.put(
                                  context.stream() + consumerName, context.offset());
                              receivedMessagesPerPartitions
                                  .get(context.stream() + consumerName)
                                  .incrementAndGet();
                              receivedMessages.get(consumerName).incrementAndGet();
                            })
                        .build();
                consumers.add(consumer);
              });
        });

    partitions.forEach(
        partition -> TestUtils.publishAndWaitForConfirms(cf, messageCount, partition));

    int messageTotal = messageCount * partitionCount;
    consumerNames.forEach(
        consumerName -> waitUntil(() -> receivedMessages.get(consumerName).get() == messageTotal));

    consumerNames.forEach(
        consumerName -> {
          // summing the received messages for the consumer group name
          assertThat(
                  receivedMessagesPerPartitions.entrySet().stream()
                      .filter(e -> e.getKey().endsWith(consumerName))
                      .mapToInt(e -> e.getValue().get())
                      .sum())
              .isEqualTo(messageTotal);
        });

    Collections.reverse(consumers);
    consumers.forEach(Consumer::close);

    Client c = cf.get();
    consumerNames.forEach(
        consumerName -> {
          partitions.forEach(
              partition -> {
                assertThat(c.queryOffset(consumerName, partition).getOffset())
                    .isEqualTo(lastReceivedOffsets.get(partition + consumerName));
              });
        });
  }

  public static Stream<java.util.function.BiConsumer<String, Consumer>>
      activeConsumerShouldGetUpdateNotificationAfterDisruption() {
    return Stream.of(
        namedBiConsumer((s, c) -> Cli.killConnection(connectionName(s, c)), "kill connection"),
        namedBiConsumer((s, c) -> Cli.restartStream(s), "restart stream"),
        namedBiConsumer((s, c) -> c.close(), "close consumer"));
  }

  @ParameterizedTest
  @MethodSource
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void activeConsumerShouldGetUpdateNotificationAfterDisruption(
      java.util.function.BiConsumer<String, Consumer> disruption) {
    declareSuperStreamTopology(configurationClient, superStream, partitionCount);
    String partition = superStream + "-0";

    String consumerName = "foo";
    Function<java.util.function.Consumer<ConsumerUpdateListener.Context>, ConsumerUpdateListener>
        filteringListener =
            action ->
                (ConsumerUpdateListener)
                    context -> {
                      if (partition.equals(context.stream())) {
                        action.accept(context);
                      }
                      return OffsetSpecification.next();
                    };

    Sync consumer1Active = sync();
    Sync consumer1Inactive = sync();

    Consumer consumer1 =
        environment
            .consumerBuilder()
            .singleActiveConsumer()
            .superStream(superStream)
            .name(consumerName)
            .noTrackingStrategy()
            .consumerUpdateListener(
                filteringListener.apply(
                    context -> {
                      if (context.isActive()) {
                        consumer1Active.down();
                      } else {
                        consumer1Inactive.down();
                      }
                    }))
            .messageHandler((context, message) -> {})
            .build();

    Sync consumer2Active = sync();
    Sync consumer2Inactive = sync();
    environment
        .consumerBuilder()
        .singleActiveConsumer()
        .superStream(superStream)
        .name(consumerName)
        .noTrackingStrategy()
        .consumerUpdateListener(
            filteringListener.apply(
                context -> {
                  if (!context.isActive()) {
                    consumer2Inactive.down();
                  }
                }))
        .messageHandler((context, message) -> {})
        .build();

    assertThat(consumer1Active).completes();
    assertThat(consumer2Inactive).hasNotCompleted();
    assertThat(consumer1Inactive).hasNotCompleted();
    assertThat(consumer2Active).hasNotCompleted();

    disruption.accept(partition, consumer1);

    assertThat(consumer2Inactive).hasNotCompleted();
    assertThat(consumer1Inactive).completes();
  }

  private static void waitUntil(CallableBooleanSupplier action) {
    try {
      waitAtMost(action);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String connectionName(String partition, Consumer consumer) {
    return ((StreamConsumer) ((SuperStreamConsumer) consumer).consumer(partition))
        .subscriptionConnectionName();
  }
}
