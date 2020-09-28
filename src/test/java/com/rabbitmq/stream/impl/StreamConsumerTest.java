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

import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamDoesNotExistException;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
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
  static final Duration TOPOLOGY_DELAY = Duration.ofSeconds(2);

  String stream;
  EventLoopGroup eventLoopGroup;

  TestUtils.ClientFactory cf;

  Environment environment;

  @BeforeEach
  void init() {
    environment =
        Environment.builder()
            .eventLoopGroup(eventLoopGroup)
            .recoveryBackOffDelayPolicy(
                BackOffDelayPolicy.fixedWithInitialDelay(RECOVERY_DELAY, RECOVERY_DELAY))
            .topologyUpdateBackOffDelayPolicy(
                BackOffDelayPolicy.fixedWithInitialDelay(TOPOLOGY_DELAY, TOPOLOGY_DELAY))
            .build();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @Test
  void nameShouldBeSetIfCommitStrategyIsSet() {
    List<UnaryOperator<ConsumerBuilder>> configurers =
        Arrays.asList(
            consumerBuilder -> consumerBuilder.autoCommitStrategy().builder(),
            consumerBuilder -> consumerBuilder.manualCommitStrategy().builder());
    configurers.forEach(
        configurer -> {
          assertThatThrownBy(
                  () -> configurer.apply(environment.consumerBuilder().stream(stream)).build())
              .isInstanceOf(IllegalArgumentException.class);
        });
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

  @Test
  void committingConsumerShouldRestartWhereItLeftOff() throws Exception {
    Producer producer = environment.producerBuilder().stream(stream).build();

    int messageCountFirstWave = 10_000;
    int messageCountSecondWave = 5_000;
    int messageCount = messageCountFirstWave + messageCountSecondWave;

    CountDownLatch latchConfirmFirstWave = new CountDownLatch(messageCountFirstWave);
    CountDownLatch latchConfirmSecondWave = new CountDownLatch(messageCount);

    ConfirmationHandler confirmationHandler =
        confirmationStatus -> {
          latchConfirmFirstWave.countDown();
          latchConfirmSecondWave.countDown();
        };

    AtomicLong messageIdSequence = new AtomicLong();

    java.util.function.Consumer<Integer> messageSending =
        messageCountToSend -> {
          IntStream.range(0, messageCountToSend)
              .forEach(
                  i ->
                      producer.send(
                          producer
                              .messageBuilder()
                              .addData("".getBytes())
                              .properties()
                              .messageId(messageIdSequence.getAndIncrement())
                              .messageBuilder()
                              .build(),
                          confirmationHandler));
        };

    messageSending.accept(messageCountFirstWave);

    assertThat(latchAssert(latchConfirmFirstWave)).completes();

    int commitEvery = 100;
    AtomicInteger consumedMessageCount = new AtomicInteger();
    AtomicReference<Consumer> consumerReference = new AtomicReference<>();
    AtomicLong lastCommittedOffset = new AtomicLong(0);
    AtomicLong lastProcessedMessage = new AtomicLong(0);

    AtomicInteger commitCount = new AtomicInteger(0);
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .name("application-1")
            .messageHandler(
                (context, message) -> {
                  consumedMessageCount.incrementAndGet();
                  lastProcessedMessage.set(message.getProperties().getMessageIdAsLong());
                  if (consumedMessageCount.get() % commitEvery == 0) {
                    context.commit();
                    lastCommittedOffset.set(context.offset());
                    commitCount.incrementAndGet();
                  }
                })
            .build();

    consumerReference.set(consumer);

    waitAtMost(10, () -> consumedMessageCount.get() == messageCountFirstWave);

    assertThat(lastCommittedOffset.get()).isPositive();

    consumer.close();

    messageSending.accept(messageCountSecondWave);

    assertThat(latchAssert(latchConfirmSecondWave)).completes();

    AtomicLong firstOffset = new AtomicLong(0);
    consumer =
        environment.consumerBuilder().stream(stream)
            .name("application-1")
            .messageHandler(
                (context, message) -> {
                  firstOffset.compareAndSet(0, context.offset());
                  if (message.getProperties().getMessageIdAsLong() > lastProcessedMessage.get()) {
                    consumedMessageCount.incrementAndGet();
                  }
                })
            .build();

    waitAtMost(
        3,
        () -> consumedMessageCount.get() == messageCount,
        () -> "Expected " + consumedMessageCount.get() + " to reach " + messageCount);

    // there will be the tracking records after the first wave of messages,
    // messages offset won't be contiguous
    assertThat(firstOffset.get()).isGreaterThanOrEqualTo(lastCommittedOffset.get());

    consumer.close();
  }

  static Stream<java.util.function.Consumer<Object>> consumerShouldKeepConsumingAfterDisruption() {
    return Stream.of(
        TestUtils.namedTask(
            o -> {
              Host.killStreamLeaderProcess(o.toString());
              Thread.sleep(TOPOLOGY_DELAY.toMillis());
            },
            "stream leader process is killed"),
        TestUtils.namedTask(
            o -> Host.killConnection("rabbitmq-stream-consumer"), "consumer connection is killed"),
        TestUtils.namedTask(
            o -> {
              try {
                Host.rabbitmqctl("stop_app");
                Thread.sleep(1000L);
              } finally {
                Host.rabbitmqctl("start_app");
              }
              Thread.sleep(RECOVERY_DELAY.toMillis() * 2);
            },
            "broker is restarted"));
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

  @Test
  void autoCommitShouldPeriodicallyAndAfterInactivity() throws Exception {
    AtomicInteger messageCount = new AtomicInteger(0);
    int commitEvery = 10_000;
    String reference = "ref-1";
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    Consumer consumer =
        environment.consumerBuilder().name(reference).stream(stream)
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  messageCount.incrementAndGet();
                })
            .autoCommitStrategy()
            .flushInterval(Duration.ofSeconds(1).plusMillis(100))
            .messageCountBeforeCommit(commitEvery)
            .builder()
            .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    IntStream.range(0, commitEvery * 2)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> {}));

    waitAtMost(5, () -> messageCount.get() == commitEvery * 2);

    Client client = cf.get();
    waitAtMost(5, () -> client.queryOffset(reference, stream) == lastReceivedOffset.get());

    int extraMessages = commitEvery / 10;
    IntStream.range(0, extraMessages)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> {}));

    waitAtMost(5, () -> messageCount.get() == commitEvery * 2 + extraMessages);

    waitAtMost(5, () -> client.queryOffset(reference, stream) == lastReceivedOffset.get());
  }
}
