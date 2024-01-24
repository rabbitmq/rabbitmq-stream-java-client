// Copyright (c) 2021-2023 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SuperStreamTest {

  EventLoopGroup eventLoopGroup;

  Environment environment;

  TestUtils.ClientFactory cf;
  Client configurationClient;
  int partitions = 3;
  String superStream;
  String[] routingKeys = null;

  @BeforeEach
  void init(TestInfo info) {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
    environment = environmentBuilder.build();
    configurationClient = cf.get();
    superStream = TestUtils.streamName(info);
  }

  @AfterEach
  void tearDown() {
    environment.close();
    deleteSuperStreamTopology(configurationClient, superStream);
  }

  @Test
  void allMessagesSentWithHashRoutingShouldBeThenConsumed() {
    int messageCount = 10_000 * partitions;
    declareSuperStreamTopology(configurationClient, superStream, partitions);
    Producer producer =
        environment
            .producerBuilder()
            .superStream(superStream)
            .routing(message -> message.getProperties().getMessageIdAsString())
            .producerBuilder()
            .build();

    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer
                        .messageBuilder()
                        .properties()
                        .messageId(UUID.randomUUID().toString())
                        .messageBuilder()
                        .build(),
                    confirmationStatus -> publishLatch.countDown()));

    assertThat(latchAssert(publishLatch)).completes(5);

    AtomicInteger totalCount = new AtomicInteger(0);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    environment
        .consumerBuilder()
        .superStream(superStream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message) -> {
              totalCount.incrementAndGet();
              consumeLatch.countDown();
            })
        .build();

    latchAssert(consumeLatch).completes();
    assertThat(totalCount.get()).isEqualTo(messageCount);
  }

  @Test
  void allMessagesSentWithRoutingKeyRoutingShouldBeThenConsumed() {
    int messageCount = 10_000 * partitions;
    routingKeys = new String[] {"amer", "emea", "apac"};
    declareSuperStreamTopology(configurationClient, superStream, routingKeys);
    Producer producer =
        environment
            .producerBuilder()
            .superStream(superStream)
            .routing(message -> message.getApplicationProperties().get("region").toString())
            .key()
            .producerBuilder()
            .build();

    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer
                        .messageBuilder()
                        .applicationProperties()
                        .entry("region", routingKeys[i % routingKeys.length])
                        .messageBuilder()
                        .build(),
                    confirmationStatus -> publishLatch.countDown()));

    assertThat(latchAssert(publishLatch)).completes(5);

    AtomicInteger totalCount = new AtomicInteger(0);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    environment
        .consumerBuilder()
        .superStream(superStream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message) -> {
              totalCount.incrementAndGet();
              consumeLatch.countDown();
            })
        .build();

    latchAssert(consumeLatch).completes();
    assertThat(totalCount.get()).isEqualTo(messageCount);
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void allMessagesForSameUserShouldEndUpInSamePartition() {
    int messageCount = 10_000 * partitions;
    int userCount = 10;
    declareSuperStreamTopology(configurationClient, superStream, partitions);

    AtomicInteger totalReceivedCount = new AtomicInteger(0);
    // <partition>.<user> => count
    Map<String, AtomicLong> receivedMessagesPerPartitionUserCount =
        new ConcurrentHashMap<>(userCount);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    CountDownLatch consumersReadyLatch = new CountDownLatch(0);
    IntStream.range(0, partitions)
        .forEach(
            i -> {
              environment
                  .consumerBuilder()
                  .superStream(superStream)
                  .name("app-1")
                  .singleActiveConsumer()
                  .offset(OffsetSpecification.first())
                  .consumerUpdateListener(
                      context -> {
                        if (context.isActive() && i == partitions - 1) {
                          // a consumer in the last composite consumer gets activated
                          consumersReadyLatch.countDown();
                        }
                        return OffsetSpecification.first();
                      })
                  .messageHandler(
                      (context, message) -> {
                        String user =
                            new String(message.getProperties().getUserId(), StandardCharsets.UTF_8);
                        receivedMessagesPerPartitionUserCount
                            .computeIfAbsent(context.stream() + "." + user, s -> new AtomicLong(0))
                            .incrementAndGet();
                        totalReceivedCount.incrementAndGet();
                        consumeLatch.countDown();
                      })
                  .build();
            });

    assertThat(latchAssert(consumersReadyLatch)).completes();

    Producer producer =
        environment
            .producerBuilder()
            .superStream(superStream)
            .routing(
                message -> new String(message.getProperties().getUserId(), StandardCharsets.UTF_8))
            .producerBuilder()
            .build();

    List<String> users =
        IntStream.range(0, userCount).mapToObj(i -> "user" + i).collect(Collectors.toList());
    Map<String, AtomicLong> messagePerUserCount =
        users.stream().collect(Collectors.toMap(u -> u, u -> new AtomicLong(0)));
    Random random = new Random();

    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              String user = users.get(random.nextInt(userCount));
              messagePerUserCount.get(user).incrementAndGet();
              producer.send(
                  producer
                      .messageBuilder()
                      .properties()
                      .userId(user.getBytes(StandardCharsets.UTF_8))
                      .messageBuilder()
                      .build(),
                  confirmationStatus -> publishLatch.countDown());
            });

    assertThat(latchAssert(publishLatch)).completes(5);

    latchAssert(consumeLatch).completes();
    assertThat(totalReceivedCount.get()).isEqualTo(messageCount);
    assertThat(receivedMessagesPerPartitionUserCount).hasSize(userCount);

    Set<String> partitionNames =
        IntStream.range(0, partitions)
            .mapToObj(i -> superStream + "-" + i)
            .collect(Collectors.toSet());
    AtomicLong total = new AtomicLong();
    Set<String> passedUsers = new HashSet<>();
    receivedMessagesPerPartitionUserCount.forEach(
        (key, count) -> {
          // <partition>.<user> => count
          String partition = key.split("\\.")[0];
          String user = key.split("\\.")[1];
          assertThat(partition).isIn(partitionNames);
          assertThat(user).isIn(users);

          assertThat(passedUsers).doesNotContain(user);
          passedUsers.add(user);

          total.addAndGet(count.get());
        });
    assertThat(total).hasValue(messageCount);
  }
}
