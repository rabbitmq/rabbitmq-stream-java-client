// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@DisabledIfFilteringNotSupported
@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class FilteringTest {

  private static final Duration CONDITION_TIMEOUT = Duration.ofSeconds(5);

  static final int messageCount = 10_000;

  EventLoopGroup eventLoopGroup;

  Environment environment;

  String stream;

  @BeforeEach
  void init() throws Exception {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
    environmentBuilder.addressResolver(add -> localhost());
    environment = environmentBuilder.build();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @ParameterizedTest
  @ValueSource(strings = "foo")
  @NullSource
  void publishConsume(String producerName) throws Exception {
    repeatIfFailure(
        () -> {
          List<String> filterValues = new ArrayList<>(Arrays.asList("apple", "banana", "pear"));
          Map<String, AtomicInteger> filterValueCount = new HashMap<>();
          Random random = new Random();

          Runnable insert =
              () ->
                  publish(
                      messageCount,
                      producerName,
                      () -> {
                        String filterValue = filterValues.get(random.nextInt(filterValues.size()));
                        filterValueCount
                            .computeIfAbsent(filterValue, k -> new AtomicInteger())
                            .incrementAndGet();
                        return filterValue;
                      });
          insert.run();

          // second wave of messages, with only one, new filter value
          String newFilterValue = "orange";
          filterValues.clear();
          filterValues.add(newFilterValue);
          insert.run();

          AtomicInteger receivedMessageCount = new AtomicInteger(0);
          AtomicInteger filteredConsumedMessageCount = new AtomicInteger(0);
          try (Consumer ignored =
              consumerBuilder()
                  .filter()
                  .values(newFilterValue)
                  .postFilter(
                      m -> {
                        receivedMessageCount.incrementAndGet();
                        return newFilterValue.equals(m.getProperties().getGroupId());
                      })
                  .builder()
                  .messageHandler(
                      (context, message) -> filteredConsumedMessageCount.incrementAndGet())
                  .build()) {
            int expectedCount = filterValueCount.get(newFilterValue).get();
            waitAtMost(
                CONDITION_TIMEOUT, () -> filteredConsumedMessageCount.get() == expectedCount);
            assertThat(receivedMessageCount).hasValueLessThan(messageCount * 2);
          }
        });
  }

  @ParameterizedTest
  @ValueSource(strings = "foo")
  @NullSource
  void publishWithNullFilterValuesShouldBePossible(String producerName) throws Exception {
    repeatIfFailure(
        () -> {
          publish(messageCount, producerName, () -> null);

          CountDownLatch consumeLatch = new CountDownLatch(messageCount);
          try (Consumer ignored =
              consumerBuilder().messageHandler((ctx, msg) -> consumeLatch.countDown()).build()) {
            latchAssert(consumeLatch).completes(CONDITION_TIMEOUT);
          }
        });
  }

  @ParameterizedTest
  @CsvSource({"foo,true", "foo,false", ",true", ",false"})
  void matchUnfilteredShouldReturnNullFilteredValueAndFilteredValues(
      String producerName, boolean matchUnfiltered) throws Exception {
    repeatIfFailure(
        () -> {
          publish(messageCount, producerName, () -> null);

          List<String> filterValues = new ArrayList<>(Arrays.asList("apple", "banana", "pear"));
          Map<String, AtomicInteger> filterValueCount = new HashMap<>();
          Random random = new Random();
          publish(
              messageCount,
              producerName,
              () -> {
                String filterValue = filterValues.get(random.nextInt(filterValues.size()));
                filterValueCount
                    .computeIfAbsent(filterValue, k -> new AtomicInteger())
                    .incrementAndGet();
                return filterValue;
              });

          publish(messageCount, producerName, () -> null);

          AtomicInteger receivedMessageCount = new AtomicInteger(0);
          Set<String> receivedFilterValues = ConcurrentHashMap.newKeySet();
          try (Consumer ignored =
              consumerBuilder()
                  .filter()
                  .values(filterValues.get(0))
                  .matchUnfiltered(matchUnfiltered)
                  .postFilter(m -> true)
                  .builder()
                  .messageHandler(
                      (ctx, msg) -> {
                        receivedFilterValues.add(
                            msg.getProperties().getGroupId() == null
                                ? "null"
                                : msg.getProperties().getGroupId());
                        receivedMessageCount.incrementAndGet();
                      })
                  .build()) {
            int expected;
            if (matchUnfiltered) {
              expected = messageCount * 2;
            } else {
              expected = messageCount;
            }
            waitAtMost(CONDITION_TIMEOUT, () -> receivedMessageCount.get() >= expected);
          }
        });
  }

  @Test
  void setFilterSizeOnCreation(TestInfo info) {
    String s = streamName(info);
    this.environment.streamCreator().stream(s).filterSize(128).create();
    this.environment.deleteStream(s);
    assertThatThrownBy(() -> this.environment.streamCreator().filterSize(15))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> this.environment.streamCreator().filterSize(256))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private ProducerBuilder producerBuilder() {
    return this.environment.producerBuilder().stream(stream);
  }

  private ConsumerBuilder consumerBuilder() {
    return this.environment.consumerBuilder().stream(stream).offset(OffsetSpecification.first());
  }

  private static final AtomicLong PUBLISHING_SEQUENCE = new AtomicLong(0);

  private void publish(
      int messageCount, String producerName, Supplier<String> filterValueSupplier) {
    Producer producer =
        producerBuilder()
            .name(producerName)
            .filterValue(m -> m.getProperties().getGroupId())
            .build();
    CountDownLatch latch = new CountDownLatch(messageCount);
    ConfirmationHandler confirmationHandler = ctx -> latch.countDown();
    IntStream.range(0, messageCount)
        .forEach(
            ignored ->
                producer.send(
                    producer
                        .messageBuilder()
                        .publishingId(PUBLISHING_SEQUENCE.getAndIncrement())
                        .properties()
                        .groupId(filterValueSupplier.get())
                        .messageBuilder()
                        .build(),
                    confirmationHandler));
    latchAssert(latch).completes(CONDITION_TIMEOUT);
    producer.close();
  }

  private static void repeatIfFailure(RunnableWithException test) throws Exception {
    int executionCount = 0;
    Throwable lastException = null;
    while (executionCount < 5) {
      try {
        test.run();
        return;
      } catch (Exception | AssertionError e) {
        executionCount++;
        lastException = e;
      }
    }
    if (lastException instanceof Error) {
      throw new RuntimeException(lastException);
    } else {
      throw (Exception) lastException;
    }
  }
}
