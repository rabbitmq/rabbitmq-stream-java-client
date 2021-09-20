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
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import io.netty.channel.EventLoopGroup;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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

  Connection connection;
  int partitions = 3;
  String superStream;
  String[] routingKeys = null;

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
      //      deleteSuperStreamTopology(connection, superStream, partitions);
    } else {
      //      deleteSuperStreamTopology(connection, superStream, routingKeys);
    }
    connection.close();
  }

  @Test
  @BrokerVersionAtLeast("3.9.6")
  void allMessagesSentWithHashRoutingShouldBeThenConsumed() throws Exception {
    int messageCount = 10_000 * partitions;
    declareSuperStreamTopology(connection, superStream, partitions);
    Producer producer =
        environment.producerBuilder().stream(superStream)
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
  @BrokerVersionAtLeast("3.9.6")
  void allMessagesSentWithRoutingKeyRoutingShouldBeThenConsumed() throws Exception {
    int messageCount = 10_000 * partitions;
    routingKeys = new String[] {"amer", "emea", "apac"};
    declareSuperStreamTopology(connection, superStream, routingKeys);
    Producer producer =
        environment.producerBuilder().stream(superStream)
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
}
