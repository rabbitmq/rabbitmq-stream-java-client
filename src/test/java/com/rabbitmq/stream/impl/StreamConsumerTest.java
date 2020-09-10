// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamConsumerTest {

  static final Duration RECOVERY_DELAY = Duration.ofSeconds(2);

  String stream;
  EventLoopGroup eventLoopGroup;
  TestUtils.ClientFactory cf;

  Environment environment;

  static Stream<java.util.function.Consumer<Object>> consumerShouldKeepConsumingAfterDisruption() {
    return Stream.of(
        //                TestUtils.namedTask(o -> {
        //                    Host.killStreamLeaderProcess(o.toString());
        //
        // Thread.sleep(DefaultClientSubscriptions.METADATA_UPDATE_DEFAULT_INITIAL_DELAY.toMillis());
        //                }, "stream leader process is killed"),
        TestUtils.namedTask(
            o -> Host.killConnection("rabbitmq-stream-consumer"), "consumer connection is killed")
        //                TestUtils.namedTask(o -> {
        //                    try {
        //                        Host.rabbitmqctl("stop_app");
        //                        Thread.sleep(1000L);
        //                    } finally {
        //                        Host.rabbitmqctl("start_app");
        //                    }
        //                    Thread.sleep(RECOVERY_DELAY.toMillis() * 2);
        //                }, "broker is restarted")
        );
  }

  @BeforeEach
  void init() {
    environment =
        Environment.builder()
            .eventLoopGroup(eventLoopGroup)
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(RECOVERY_DELAY))
            .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @Test
  void consume() throws Exception {
    int messageCount = 100_000;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    IntStream.range(0, messageCount)
        .forEach(
            i ->
                client.publish(
                    stream,
                    (byte) 1,
                    Collections.singletonList(
                        client.messageBuilder().addData("".getBytes()).build())));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);

    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .messageHandler((offset, message) -> consumeLatch.countDown())
            .build();

    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

    consumer.close();
  }

  @Test
  void creatingConsumerOnNonExistingStreamShouldThrowException() {
    String nonExistingStream = UUID.randomUUID().toString();
    assertThatThrownBy(
            () -> {
              environment.consumerBuilder().stream(nonExistingStream)
                  .messageHandler((offset, message) -> {})
                  .build();
            })
        .isInstanceOf(StreamDoesNotExistException.class)
        .hasMessageContaining(nonExistingStream)
        .extracting("stream")
        .isEqualTo(nonExistingStream);
  }

  @Test
  void consumerShouldBeClosedWhenStreamGetsDeleted() throws Exception {
    String s = UUID.randomUUID().toString();
    environment.streamCreator().stream(s).create();

    int messageCount = 10_000;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Producer producer = environment.producerBuilder().stream(s).build();
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatch.countDown()));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    StreamConsumer consumer =
        (StreamConsumer)
            environment.consumerBuilder().stream(s)
                .messageHandler((offset, message) -> consumeLatch.countDown())
                .build();

    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

    assertThat(consumer.isOpen()).isTrue();

    environment.deleteStream(s);

    TestUtils.waitAtMost(10, () -> !consumer.isOpen());

    assertThat(consumer.isOpen()).isFalse();
  }

  @ParameterizedTest
  @MethodSource
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void consumerShouldKeepConsumingAfterDisruption(java.util.function.Consumer<Object> disruption)
      throws Exception {
    String s = UUID.randomUUID().toString();
    environment.streamCreator().stream(s).create();
    try {
      int messageCount = 10_000;
      CountDownLatch publishLatch = new CountDownLatch(messageCount);
      Producer producer = environment.producerBuilder().stream(s).build();
      IntStream.range(0, messageCount)
          .forEach(
              i ->
                  producer.send(
                      producer.messageBuilder().addData("".getBytes()).build(),
                      confirmationStatus -> publishLatch.countDown()));

      assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
      producer.close();

      AtomicInteger receivedMessageCount = new AtomicInteger(0);
      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      CountDownLatch consumeLatchSecondWave = new CountDownLatch(messageCount * 2);
      StreamConsumer consumer =
          (StreamConsumer)
              environment.consumerBuilder().stream(s)
                  .messageHandler(
                      (offset, message) -> {
                        receivedMessageCount.incrementAndGet();
                        consumeLatch.countDown();
                        consumeLatchSecondWave.countDown();
                      })
                  .build();

      assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

      assertThat(consumer.isOpen()).isTrue();

      disruption.accept(s);

      Client client = cf.get();
      TestUtils.waitAtMost(
          10,
          () -> {
            Client.StreamMetadata metadata = client.metadata(s).get(s);
            return metadata.getLeader() != null || !metadata.getReplicas().isEmpty();
          });

      CountDownLatch publishLatchSecondWave = new CountDownLatch(messageCount);
      Producer producerSecondWave = environment.producerBuilder().stream(s).build();
      IntStream.range(0, messageCount)
          .forEach(
              i ->
                  producerSecondWave.send(
                      producerSecondWave.messageBuilder().addData("".getBytes()).build(),
                      confirmationStatus -> publishLatchSecondWave.countDown()));

      assertThat(publishLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
      producerSecondWave.close();

      assertThat(consumeLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(receivedMessageCount.get())
          .isBetween(messageCount * 2, messageCount * 2 + 1); // there can be a duplicate
      assertThat(consumer.isOpen()).isTrue();

      consumer.close();
    } finally {
      environment.deleteStream(s);
    }
  }
}
