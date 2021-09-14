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

import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder.RoutingType;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import io.netty.channel.EventLoopGroup;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class SuperStreamProducerTest {

  EventLoopGroup eventLoopGroup;

  Environment environment;

  Connection connection;
  int partitions = 3;
  String superStream;
  String[] routingKeys = null;
  TestUtils.ClientFactory cf;

  @BeforeEach
  void init(TestInfo info) throws Exception {
    EnvironmentBuilder environmentBuilder = Environment.builder().eventLoopGroup(eventLoopGroup);
    environmentBuilder.addressResolver(add -> localhost());
    environment = environmentBuilder.build();
    connection = new ConnectionFactory().newConnection();
    superStream = TestUtils.streamName(info);
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
    if (routingKeys == null) {
      deleteSuperStreamTopology(connection, superStream, partitions);
    } else {
      deleteSuperStreamTopology(connection, superStream, routingKeys);
    }
    connection.close();
  }

  @Test
  @BrokerVersionAtLeast("3.9.6")
  void allMessagesSentToSuperStreamWithHashRoutingShouldBeThenConsumed() throws Exception {
    int messageCount = 10_000;
    declareSuperStreamTopology(connection, superStream, partitions);
    Producer producer =
        environment.producerBuilder().stream(superStream)
            .routing(message -> message.getProperties().getMessageIdAsString(), RoutingType.HASH)
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

    Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    AtomicLong totalCount = new AtomicLong(0);
    IntStream.range(0, partitions)
        .forEach(
            i -> {
              String stream = superStream + "-" + i;
              AtomicLong streamCount = new AtomicLong(0);
              counts.put(stream, streamCount);
              environment.consumerBuilder().stream(stream)
                  .offset(OffsetSpecification.first())
                  .messageHandler(
                      (context, message) -> {
                        streamCount.incrementAndGet();
                        totalCount.incrementAndGet();
                      })
                  .build();
            });

    waitAtMost(10, () -> totalCount.get() == messageCount);

    assertThat(counts.values().stream().map(AtomicLong::get))
        .hasSize(partitions)
        .doesNotContain(0L);
    assertThat(counts.values().stream().map(AtomicLong::get).reduce(0L, Long::sum))
        .isEqualTo(messageCount);
  }

  @Test
  @BrokerVersionAtLeast("3.9.6")
  void allMessagesSentToSuperStreamWithRoutingKeyRoutingShouldBeThenConsumed() throws Exception {
    int messageCount = 10_000;
    routingKeys = new String[] {"amer", "emea", "apac"};
    declareSuperStreamTopology(connection, superStream, routingKeys);
    Producer producer =
        environment.producerBuilder().stream(superStream)
            .routing(
                message -> message.getApplicationProperties().get("region").toString(),
                RoutingType.KEY)
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

    Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    AtomicLong totalCount = new AtomicLong(0);
    for (String routingKey : routingKeys) {
      String stream = superStream + "-" + routingKey;
      AtomicLong streamCount = new AtomicLong(0);
      counts.put(stream, streamCount);
      environment.consumerBuilder().stream(stream)
          .offset(OffsetSpecification.first())
          .messageHandler(
              (context, message) -> {
                streamCount.incrementAndGet();
                totalCount.incrementAndGet();
              })
          .build();
    }
    waitAtMost(10, () -> totalCount.get() == messageCount);

    assertThat(counts.values().stream().map(AtomicLong::get))
        .hasSameSizeAs(routingKeys)
        .doesNotContain(0L);
    assertThat(counts.values().stream().map(AtomicLong::get).reduce(0L, Long::sum))
        .isEqualTo(messageCount);
  }

  @Test
  @BrokerVersionAtLeast("3.9.6")
  void getLastPublishingIdShouldReturnLowestValue() throws Exception {
    int messageCount = 10_000;
    declareSuperStreamTopology(connection, superStream, partitions);
    String producerName = "super-stream-application";
    Producer producer =
        environment.producerBuilder().name(producerName).stream(superStream)
            .routing(message -> message.getProperties().getMessageIdAsString(), RoutingType.HASH)
            .build();

    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer
                        .messageBuilder()
                        .publishingId(i)
                        .properties()
                        .messageId(UUID.randomUUID().toString())
                        .messageBuilder()
                        .build(),
                    confirmationStatus -> publishLatch.countDown()));

    assertThat(latchAssert(publishLatch)).completes(5);

    long lastPublishingId = producer.getLastPublishingId();
    assertThat(lastPublishingId).isNotZero();
    Client client = cf.get();
    IntStream.range(0, partitions)
        .mapToObj(i -> superStream + "-" + i)
        .forEach(
            stream -> {
              long publishingId = client.queryPublisherSequence(producerName, stream);
              assertThat(publishingId).isGreaterThanOrEqualTo(lastPublishingId);
            });

    Map<String, AtomicLong> counts = new ConcurrentHashMap<>();
    AtomicLong totalCount = new AtomicLong(0);
    IntStream.range(0, partitions)
        .mapToObj(i -> superStream + "-" + i)
        .forEach(
            stream -> {
              AtomicLong streamCount = new AtomicLong(0);
              counts.put(stream, streamCount);
              environment.consumerBuilder().stream(stream)
                  .offset(OffsetSpecification.first())
                  .messageHandler(
                      (context, message) -> {
                        streamCount.incrementAndGet();
                        totalCount.incrementAndGet();
                      })
                  .build();
            });

    waitAtMost(10, () -> totalCount.get() == messageCount);

    assertThat(counts.values().stream().map(AtomicLong::get))
        .hasSize(partitions)
        .doesNotContain(0L);
    assertThat(counts.values().stream().map(AtomicLong::get).reduce(0L, Long::sum))
        .isEqualTo(messageCount);
  }
}
