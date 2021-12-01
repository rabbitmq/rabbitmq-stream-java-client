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
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
}
