// Copyright (c) 2021-2024 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Cli.diskAlarm;
import static com.rabbitmq.stream.Cli.memoryAlarm;
import static com.rabbitmq.stream.impl.TestUtils.ExceptionConditions.responseCode;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@TestUtils.DisabledIfRabbitMqCtlNotSet
@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class AlarmsTest {

  static EventLoopGroup eventLoopGroup;

  EnvironmentBuilder environmentBuilder;

  String stream;
  Environment env;

  @BeforeAll
  static void initAll() {
    eventLoopGroup = new NioEventLoopGroup();
  }

  @AfterAll
  static void afterAll() throws Exception {
    eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
  }

  @BeforeEach
  void init() {
    environmentBuilder = Environment.builder();
    env = environmentBuilder.netty().eventLoopGroup(eventLoopGroup).environmentBuilder().build();
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  @Test
  void creatingPublisherWhenDiskAlarmIsOnShouldFail() throws Exception {
    try (AutoCloseable alarm = diskAlarm()) {
      assertThatThrownBy(() -> env.producerBuilder().stream(stream).build())
          .isInstanceOf(StreamException.class)
          .has(responseCode(Constants.RESPONSE_CODE_PRECONDITION_FAILED));
    }
  }

  @Test
  void publishingShouldBeBlockedWhenDiskAlarmIsOn() throws Exception {
    int messageCount = 50_000;
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(messageCount));

    ConfirmationHandler confirmationHandler = confirmationStatus -> latch.get().countDown();
    Producer producer = env.producerBuilder().stream(stream).build();
    range(0, messageCount)
        .forEach(i -> producer.send(producer.messageBuilder().build(), confirmationHandler));

    assertThat(latchAssert(latch.get())).completes();
    try (AutoCloseable alarm = diskAlarm()) {
      latch.set(new CountDownLatch(messageCount));
      new Thread(
              () ->
                  range(0, messageCount)
                      .forEach(
                          i ->
                              producer.send(
                                  producer.messageBuilder().build(), confirmationHandler)))
          .start();

      assertThat(latchAssert(latch.get())).doesNotComplete(Duration.ofSeconds(5));
    }
    assertThat(latchAssert(latch.get())).completes();

    producer.close();

    latch.set(new CountDownLatch(messageCount * 2));
    Consumer consumer =
        env.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> latch.get().countDown())
            .build();

    assertThat(latchAssert(latch.get())).completes();
    consumer.close();
  }

  @Test
  void creatingPublisherWhenMemoryAlarmIsOnShouldSucceed() throws Exception {
    try (AutoCloseable alarm = memoryAlarm()) {
      Producer producer = env.producerBuilder().stream(stream).build();
      producer.close();
    }
  }

  @Test
  void publishingWhenMemoryAlarmIsOnShouldWork() throws Exception {
    int messageCount = 50_000;
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(messageCount));

    ConfirmationHandler confirmationHandler = confirmationStatus -> latch.get().countDown();
    Producer producer = env.producerBuilder().stream(stream).build();
    range(0, messageCount)
        .forEach(i -> producer.send(producer.messageBuilder().build(), confirmationHandler));

    assertThat(latchAssert(latch.get())).completes();
    try (AutoCloseable alarm = memoryAlarm()) {
      latch.set(new CountDownLatch(messageCount));
      new Thread(
              () ->
                  range(0, messageCount)
                      .forEach(
                          i ->
                              producer.send(
                                  producer.messageBuilder().build(), confirmationHandler)))
          .start();

      assertThat(latchAssert(latch.get())).completes();
    }
    assertThat(latchAssert(latch.get())).completes();

    producer.close();

    latch.set(new CountDownLatch(messageCount * 2));
    Consumer consumer =
        env.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> latch.get().countDown())
            .build();

    assertThat(latchAssert(latch.get())).completes();
    consumer.close();
  }

  @Test
  void diskAlarmShouldNotPreventConsumption() throws Exception {
    int messageCount = 50_000;
    AtomicReference<CountDownLatch> latch = new AtomicReference<>(new CountDownLatch(messageCount));

    ConfirmationHandler confirmationHandler = confirmationStatus -> latch.get().countDown();
    Producer producer = env.producerBuilder().stream(stream).build();
    range(0, messageCount)
        .forEach(i -> producer.send(producer.messageBuilder().build(), confirmationHandler));

    assertThat(latchAssert(latch)).completes();
    producer.close();

    try (AutoCloseable alarm = diskAlarm()) {
      latch.set(new CountDownLatch(messageCount));
      Consumer consumer =
          env.consumerBuilder().stream(stream)
              .offset(OffsetSpecification.first())
              .messageHandler((context, message) -> latch.get().countDown())
              .build();
      assertThat(latchAssert(latch)).completes();
      consumer.close();
    }
  }
}
