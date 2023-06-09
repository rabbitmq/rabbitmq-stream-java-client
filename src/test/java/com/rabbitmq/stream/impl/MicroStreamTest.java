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

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class MicroStreamTest {

  static final Charset UTF8 = StandardCharsets.UTF_8;
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

  @Test
  void publishConsume() throws Exception {
    int messageCount = 10_000;
    Producer producer =
        environment.producerBuilder().stream(stream)
            .filter(m -> m.getProperties().getGroupId())
            .build();

    List<String> filterValues = new ArrayList<>(Arrays.asList("apple", "banana", "pear"));
    Map<String, AtomicInteger> filterValueCount = new HashMap<>();
    Random random = new Random();

    AtomicReference<CountDownLatch> publishLatch =
        new AtomicReference<>(new CountDownLatch(messageCount));
    ConfirmationHandler confirmationHandler = confirmationStatus -> publishLatch.get().countDown();
    Runnable insert =
        () -> {
          IntStream.range(0, messageCount)
              .forEach(
                  i -> {
                    String filterValue = filterValues.get(random.nextInt(filterValues.size()));
                    filterValueCount
                        .computeIfAbsent(filterValue, k -> new AtomicInteger())
                        .incrementAndGet();
                    producer.send(
                        producer
                            .messageBuilder()
                            .properties()
                            .groupId(filterValue)
                            .messageBuilder()
                            .build(),
                        confirmationHandler);
                  });
        };
    insert.run();
    latchAssert(publishLatch).completes();

    // second wave of messages, with only one, new filter value
    String newFilterValue = "orange";
    filterValues.clear();
    filterValues.add(newFilterValue);
    publishLatch.set(new CountDownLatch(messageCount));
    insert.run();
    assertThat(latchAssert(publishLatch)).completes();

    AtomicInteger receivedMessageCount = new AtomicInteger(0);
    AtomicInteger filteredConsumedMessageCount = new AtomicInteger(0);
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .filter()
        .values(newFilterValue)
        .filter(
            m -> {
              receivedMessageCount.incrementAndGet();
              return newFilterValue.equals(m.getProperties().getGroupId());
            })
        .builder()
        .messageHandler((context, message) -> filteredConsumedMessageCount.incrementAndGet())
        .build();

    int expectedCount = filterValueCount.get(newFilterValue).get();
    waitAtMost(() -> filteredConsumedMessageCount.get() == expectedCount);
    assertThat(receivedMessageCount).hasValueLessThan(messageCount * 2);
  }
}
