// Copyright (c) 2022-2025 Broadcom. All Rights Reserved.
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
import static com.rabbitmq.stream.impl.TestUtils.namedConsumer;
import static com.rabbitmq.stream.impl.TestUtils.publishAndWaitForConfirms;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast311Condition;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import io.netty.channel.EventLoopGroup;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith({
  TestUtils.StreamTestInfrastructureExtension.class,
  BrokerVersionAtLeast311Condition.class
})
@TestUtils.SingleActiveConsumer
public class SacStreamConsumerTest {

  String stream;
  EventLoopGroup eventLoopGroup;
  TestUtils.ClientFactory cf;
  Environment environment;

  @BeforeEach
  void init() {
    EnvironmentBuilder environmentBuilder =
        Environment.builder()
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder()
            .maxConsumersByConnection(1);
    environment = environmentBuilder.build();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @Test
  void autoTrackingSecondConsumerShouldTakeOverWhereTheFirstOneLeftOff() throws Exception {
    int messageCount = 10000;
    Map<Integer, AtomicInteger> receivedMessages = new ConcurrentHashMap<>();
    receivedMessages.put(0, new AtomicInteger(0));
    receivedMessages.put(1, new AtomicInteger(0));
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    String consumerName = "foo";
    Consumer consumer1 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  receivedMessages.get(0).incrementAndGet();
                })
            .offset(OffsetSpecification.first())
            .autoTrackingStrategy()
            .builder()
            .build();

    Consumer consumer2 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  receivedMessages.get(1).incrementAndGet();
                })
            .offset(OffsetSpecification.first())
            .autoTrackingStrategy()
            .builder()
            .build();

    publishAndWaitForConfirms(cf, messageCount, stream);
    waitAtMost(() -> receivedMessages.getOrDefault(0, new AtomicInteger(0)).get() == messageCount);

    assertThat(lastReceivedOffset).hasPositiveValue();
    assertThat(receivedMessages.get(1)).hasValue(0);

    long firstWaveLimit = lastReceivedOffset.get();
    consumer1.close();

    publishAndWaitForConfirms(cf, messageCount, stream);

    waitAtMost(() -> receivedMessages.getOrDefault(0, new AtomicInteger(1)).get() == messageCount);
    assertThat(lastReceivedOffset).hasValueGreaterThan(firstWaveLimit);
    assertThat(receivedMessages.get(0)).hasValue(messageCount);

    consumer2.close();
  }

  @Test
  void manualTrackingSecondConsumerShouldTakeOverWhereTheFirstOneLeftOff() throws Exception {
    int messageCount = 10000;
    int storeEvery = 1000;
    AtomicInteger consumer1MessageCount = new AtomicInteger(0);
    AtomicInteger consumer2MessageCount = new AtomicInteger(0);
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    String consumerName = "foo";

    BiConsumer<MessageHandler.Context, AtomicInteger> handler =
        (ctx, count) -> {
          lastReceivedOffset.set(ctx.offset());
          if (count.incrementAndGet() % storeEvery == 0) {
            ctx.storeOffset();
          }
        };

    Consumer consumer1 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler((context, message) -> handler.accept(context, consumer1MessageCount))
            .offset(OffsetSpecification.first())
            .manualTrackingStrategy()
            .builder()
            .build();

    Consumer consumer2 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler((context, message) -> handler.accept(context, consumer2MessageCount))
            .offset(OffsetSpecification.first())
            .manualTrackingStrategy()
            .builder()
            .build();

    publishAndWaitForConfirms(cf, messageCount, stream);
    waitAtMost(() -> consumer1MessageCount.get() == messageCount);

    assertThat(lastReceivedOffset).hasPositiveValue();
    assertThat(consumer2MessageCount).hasValue(0);

    long firstWaveLimit = lastReceivedOffset.get();

    consumer1.store(firstWaveLimit);
    waitAtMost(() -> consumer1.storedOffset() == firstWaveLimit);

    consumer1.close();

    publishAndWaitForConfirms(cf, messageCount, stream);

    waitAtMost(() -> consumer2MessageCount.get() == messageCount);
    assertThat(lastReceivedOffset).hasValueGreaterThan(firstWaveLimit);
    assertThat(consumer1MessageCount).hasValue(messageCount);

    consumer2.close();
  }

  @Test
  void externalTrackingSecondConsumerShouldTakeOverWhereTheFirstOneLeftOff() throws Exception {
    int messageCount = 10000;
    Map<Integer, AtomicInteger> receivedMessages = new ConcurrentHashMap<>();
    receivedMessages.put(0, new AtomicInteger(0));
    receivedMessages.put(1, new AtomicInteger(0));
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    String consumerName = "foo";
    Consumer consumer1 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  receivedMessages.get(0).incrementAndGet();
                })
            .offset(OffsetSpecification.first())
            .noTrackingStrategy()
            .consumerUpdateListener(context -> OffsetSpecification.offset(lastReceivedOffset.get()))
            .build();

    Consumer consumer2 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .singleActiveConsumer()
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  receivedMessages.get(1).incrementAndGet();
                })
            .offset(OffsetSpecification.first())
            .noTrackingStrategy()
            .consumerUpdateListener(context -> OffsetSpecification.offset(lastReceivedOffset.get()))
            .build();

    publishAndWaitForConfirms(cf, messageCount, stream);
    waitAtMost(() -> receivedMessages.getOrDefault(0, new AtomicInteger(0)).get() == messageCount);

    assertThat(lastReceivedOffset).hasPositiveValue();
    assertThat(receivedMessages.get(1)).hasValue(0);

    long firstWaveLimit = lastReceivedOffset.get();
    consumer1.close();

    publishAndWaitForConfirms(cf, messageCount, stream);

    waitAtMost(() -> receivedMessages.getOrDefault(0, new AtomicInteger(1)).get() == messageCount);
    assertThat(lastReceivedOffset).hasValueGreaterThan(firstWaveLimit);
    assertThat(receivedMessages.get(0)).hasValue(messageCount);

    consumer2.close();

    // nothing stored on the server side
    assertThat(cf.get().queryOffset(consumerName, stream).getOffset()).isZero();
  }

  public static Stream<java.util.function.Consumer<Consumer>>
      activeConsumerShouldGetUpdateNotificationAfterDisruption() {
    return Stream.of(
        namedConsumer(consumer -> Cli.killConnection(connectionName(consumer)), "kill connection"),
        namedConsumer(consumer -> Cli.restartStream(stream(consumer)), "restart stream"),
        namedConsumer(Consumer::close, "close consumer"));
  }

  @ParameterizedTest
  @MethodSource
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void activeConsumerShouldGetUpdateNotificationAfterDisruption(
      java.util.function.Consumer<Consumer> disruption) {
    String consumerName = "foo";
    Sync consumer1Active = sync();
    Sync consumer1Inactive = sync();
    Consumer consumer1 =
        environment.consumerBuilder().stream(stream)
            .name(consumerName)
            .noTrackingStrategy()
            .singleActiveConsumer()
            .consumerUpdateListener(
                context -> {
                  if (context.isActive()) {
                    consumer1Active.down();
                  } else {
                    consumer1Inactive.down();
                  }
                  return OffsetSpecification.next();
                })
            .messageHandler((context, message) -> {})
            .build();

    Sync consumer2Active = sync();
    Sync consumer2Inactive = sync();
    environment.consumerBuilder().stream(stream)
        .name(consumerName)
        .noTrackingStrategy()
        .singleActiveConsumer()
        .consumerUpdateListener(
            context -> {
              if (!context.isActive()) {
                consumer2Inactive.down();
              }
              return OffsetSpecification.next();
            })
        .messageHandler((context, message) -> {})
        .build();

    assertThat(consumer1Active).completes();
    assertThat(consumer2Inactive).hasNotCompleted();
    assertThat(consumer1Inactive).hasNotCompleted();
    assertThat(consumer2Active).hasNotCompleted();

    disruption.accept(consumer1);

    assertThat(consumer2Inactive).hasNotCompleted();
    assertThat(consumer1Inactive).completes();
  }

  private static String connectionName(Consumer consumer) {
    return ((StreamConsumer) consumer).subscriptionConnectionName();
  }

  private static String stream(Consumer consumer) {
    return ((StreamConsumer) consumer).stream();
  }
}
