// Copyright (c) 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.*;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.Consumer;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
  ClientFactory cf;

  @BeforeEach
  void init() throws Exception {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
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

  @Test
  void publishConsumeAmqp() throws Exception {
    int messageCount = 1000;
    repeatIfFailure(
        () -> {
          List<String> filterValues = new ArrayList<>(Arrays.asList("apple", "banana", "pear"));
          Map<String, AtomicInteger> filterValueCount = new HashMap<>();
          Random random = new Random();

          try (Connection c = new ConnectionFactory().newConnection()) {
            Callable<Void> insert =
                () -> {
                  publishAmqp(
                      c,
                      messageCount,
                      () -> {
                        String filterValue = filterValues.get(random.nextInt(filterValues.size()));
                        filterValueCount
                            .computeIfAbsent(filterValue, k -> new AtomicInteger())
                            .incrementAndGet();
                        return filterValue;
                      });
                  return null;
                };
            insert.call();

            // second wave of messages, with only one, new filter value
            String newFilterValue = "orange";
            filterValues.clear();
            filterValues.add(newFilterValue);
            insert.call();

            AtomicInteger receivedMessageCount = new AtomicInteger(0);
            AtomicInteger filteredConsumedMessageCount = new AtomicInteger(0);
            Channel ch = c.createChannel();
            ch.basicQos(10);
            Map<String, Object> arguments = new HashMap<>();
            arguments.put("x-stream-filter", newFilterValue);
            arguments.put("x-stream-offset", 0);
            ch.basicConsume(
                stream,
                false,
                arguments,
                new DefaultConsumer(ch) {
                  @Override
                  public void handleDelivery(
                      String consumerTag,
                      Envelope envelope,
                      AMQP.BasicProperties properties,
                      byte[] body)
                      throws IOException {
                    receivedMessageCount.incrementAndGet();
                    String filterValue =
                        properties.getHeaders().get("x-stream-filter-value").toString();
                    if (newFilterValue.equals(filterValue)) {
                      filteredConsumedMessageCount.incrementAndGet();
                    }
                    ch.basicAck(envelope.getDeliveryTag(), false);
                  }
                });
            int expectedCount = filterValueCount.get(newFilterValue).get();
            waitAtMost(
                CONDITION_TIMEOUT, () -> filteredConsumedMessageCount.get() == expectedCount);
            assertThat(receivedMessageCount).hasValueLessThan(messageCount * 2);
          }
        });
  }

  @Test
  void superStream(TestInfo info) throws Exception {
    repeatIfFailure(
        () -> {
          String superStream = TestUtils.streamName(info);
          int partitionCount = 3;
          Client configurationClient = cf.get();
          try {
            declareSuperStreamTopology(configurationClient, superStream, partitionCount);

            Producer producer =
                environment
                    .producerBuilder()
                    .superStream(superStream)
                    .routing(msg -> msg.getApplicationProperties().get("customerId").toString())
                    .producerBuilder()
                    .filterValue(msg -> msg.getApplicationProperties().get("type").toString())
                    .build();

            List<String> customers =
                IntStream.range(0, 10)
                    .mapToObj(ignored -> UUID.randomUUID().toString())
                    .collect(toList());
            Random random = new Random();

            List<String> filterValues = new ArrayList<>(Arrays.asList("invoice", "order", "claim"));
            Map<String, AtomicInteger> filterValueCount = new HashMap<>();
            AtomicReference<CountDownLatch> latch =
                new AtomicReference<>(new CountDownLatch(messageCount));
            Runnable insert =
                () -> {
                  ConfirmationHandler confirmationHandler = ctx -> latch.get().countDown();
                  IntStream.range(0, messageCount)
                      .forEach(
                          ignored -> {
                            String filterValue =
                                filterValues.get(random.nextInt(filterValues.size()));
                            filterValueCount
                                .computeIfAbsent(filterValue, k -> new AtomicInteger())
                                .incrementAndGet();
                            producer.send(
                                producer
                                    .messageBuilder()
                                    .applicationProperties()
                                    .entry(
                                        "customerId",
                                        customers.get(random.nextInt(customers.size())))
                                    .entry("type", filterValue)
                                    .messageBuilder()
                                    .build(),
                                confirmationHandler);
                          });
                  latchAssert(latch).completes(CONDITION_TIMEOUT);
                };

            insert.run();

            // second wave of messages, with only one, new filter value
            filterValues.clear();
            String filterValue = "refund";
            filterValues.add(filterValue);
            latch.set(new CountDownLatch(messageCount));
            insert.run();

            AtomicInteger receivedMessageCount = new AtomicInteger(0);
            AtomicInteger filteredConsumedMessageCount = new AtomicInteger(0);
            Consumer consumer =
                environment
                    .consumerBuilder()
                    .superStream(superStream)
                    .offset(OffsetSpecification.first())
                    .filter()
                    .values(filterValue)
                    .postFilter(
                        msg -> {
                          receivedMessageCount.incrementAndGet();
                          return filterValue.equals(msg.getApplicationProperties().get("type"));
                        })
                    .builder()
                    .messageHandler(
                        (context, message) -> {
                          filteredConsumedMessageCount.incrementAndGet();
                        })
                    .build();

            int expectedCount = filterValueCount.get(filterValue).get();
            waitAtMost(
                CONDITION_TIMEOUT, () -> filteredConsumedMessageCount.get() == expectedCount);
            assertThat(receivedMessageCount).hasValueLessThan(messageCount * 2);
          } finally {
            deleteSuperStreamTopology(configurationClient, superStream);
          }
        });
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

  private void publishAmqp(Connection c, int messageCount, Supplier<String> filterValueSupplier)
      throws Exception {
    try (Channel ch = c.createChannel()) {
      ch.confirmSelect();
      for (int i = 0; i < messageCount; i++) {
        ch.basicPublish(
            "",
            stream,
            new Builder()
                .headers(singletonMap("x-stream-filter-value", filterValueSupplier.get()))
                .build(),
            null);
      }
      ch.waitForConfirmsOrDie();
    }
  }
}
