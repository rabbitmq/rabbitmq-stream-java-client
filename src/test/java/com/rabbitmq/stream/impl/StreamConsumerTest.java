// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.ConsumerFlowStrategy.creditWhenHalfMessagesProcessed;
import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.*;
import static com.rabbitmq.stream.impl.TestUtils.CountDownLatchConditions.completed;
import static java.lang.String.format;
import static java.util.Collections.synchronizedList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ConsumerInfo;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfRabbitMqCtlNotSet;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamConsumerTest {

  static final Duration RECOVERY_DELAY = Duration.ofSeconds(2);
  static final Duration TOPOLOGY_DELAY = Duration.ofSeconds(2);
  static volatile Duration recoveryInitialDelay;
  String stream;
  EventLoopGroup eventLoopGroup;
  TestUtils.ClientFactory cf;
  Environment environment;

  static Stream<java.util.function.Consumer<Object>> consumerShouldKeepConsumingAfterDisruption() {
    return Stream.of(
        TestUtils.namedTask(
            o -> {
              Cli.killStreamLeaderProcess(o.toString());
              Thread.sleep(TOPOLOGY_DELAY.toMillis());
            },
            "stream leader process is killed"),
        TestUtils.namedTask(
            o -> Cli.killConnection("rabbitmq-stream-consumer-0"), "consumer connection is killed"),
        TestUtils.namedTask(
            o -> {
              try {
                Cli.rabbitmqctl("stop_app");
                Thread.sleep(1000L);
              } finally {
                Cli.rabbitmqctl("start_app");
              }
              Thread.sleep(recoveryInitialDelay.toMillis() * 2);
            },
            "broker is restarted"));
  }

  @BeforeEach
  void init() {
    if (Cli.isOnDocker()) {
      // with a containerized broker in bridged network mode, the client should not
      // reconnect too soon, as it would see the port still open but would not get any response.
      // This then provokes some cascading timeouts in the test.
      recoveryInitialDelay = Duration.ofSeconds(10);
    } else {
      recoveryInitialDelay = RECOVERY_DELAY;
    }
    EnvironmentBuilder environmentBuilder = environmentBuilder();
    environment = environmentBuilder.build();
  }

  private EnvironmentBuilder environmentBuilder() {
    return Environment.builder()
        .netty()
        .eventLoopGroup(eventLoopGroup)
        .environmentBuilder()
        .recoveryBackOffDelayPolicy(
            BackOffDelayPolicy.fixedWithInitialDelay(recoveryInitialDelay, RECOVERY_DELAY))
        .topologyUpdateBackOffDelayPolicy(
            BackOffDelayPolicy.fixedWithInitialDelay(TOPOLOGY_DELAY, TOPOLOGY_DELAY));
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @Test
  void nameShouldBeSetIfTrackingStrategyIsSet() {
    List<UnaryOperator<ConsumerBuilder>> configurers =
        Arrays.asList(
            consumerBuilder -> consumerBuilder.autoTrackingStrategy().builder(),
            consumerBuilder -> consumerBuilder.manualTrackingStrategy().builder());
    configurers.forEach(
        configurer -> {
          assertThatThrownBy(
                  () -> configurer.apply(environment.consumerBuilder().stream(stream)).build())
              .isInstanceOf(IllegalArgumentException.class);
        });
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_0)
  void committedOffsetShouldBeSet() throws Exception {
    int messageCount = 20_000;
    publishAndWaitForConfirms(cf, messageCount, this.stream);

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    AtomicLong committedOffset = new AtomicLong();
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  committedOffset.set(context.committedChunkId());
                  consumeLatch.countDown();
                })
            .build();

    org.assertj.core.api.Assertions.assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    org.assertj.core.api.Assertions.assertThat(committedOffset.get()).isNotZero();

    consumer.close();
  }

  @Test
  void consume() throws Exception {
    int messageCount = 100_000;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    client.declarePublisher(b(1), null, stream);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                client.publish(
                    b(1),
                    Collections.singletonList(
                        client.messageBuilder().addData("".getBytes()).build())));

    org.assertj.core.api.Assertions.assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);

    AtomicLong chunkTimestamp = new AtomicLong();
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  chunkTimestamp.set(context.timestamp());
                  consumeLatch.countDown();
                })
            .build();

    org.assertj.core.api.Assertions.assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    org.assertj.core.api.Assertions.assertThat(chunkTimestamp.get()).isNotZero();

    consumer.close();
  }

  @Test
  void consumeWithAsyncConsumerFlowControl() throws Exception {
    int messageCount = 100_000;
    publishAndWaitForConfirms(cf, messageCount, stream);

    ConsumerBuilder consumerBuilder =
        environment.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .flow()
            .strategy(creditWhenHalfMessagesProcessed(1))
            .builder();

    List<MessageHandler.Context> messageContexts = synchronizedList(new ArrayList<>());

    int processingLimit = messageCount / 2;
    AtomicInteger receivedMessageCount = new AtomicInteger();
    AtomicReference<IntFunction<Boolean>> processingCondition =
        new AtomicReference<>(count -> count <= processingLimit);

    consumerBuilder =
        consumerBuilder.messageHandler(
            (context, message) -> {
              receivedMessageCount.incrementAndGet();
              if (processingCondition.get().apply(receivedMessageCount.get())) {
                context.processed();
              } else {
                messageContexts.add(context);
              }
            });
    Consumer consumer = consumerBuilder.build();

    waitAtMost(() -> receivedMessageCount.get() >= processingLimit);
    waitUntilStable(receivedMessageCount::get);

    org.assertj.core.api.Assertions.assertThat(receivedMessageCount)
        .hasValueGreaterThanOrEqualTo(processingLimit)
        .hasValueLessThan(messageCount);

    processingCondition.set(ignored -> true);
    messageContexts.forEach(MessageHandler.Context::processed);
    waitAtMost(() -> receivedMessageCount.get() == messageCount);

    consumer.close();
  }

  @Test
  void asynchronousProcessingWithFlowControl() {
    int messageCount = 100_000;
    publishAndWaitForConfirms(cf, messageCount, stream);
    ExecutorService executorService = Executors.newFixedThreadPool(Utils.AVAILABLE_PROCESSORS);
    try {
      CountDownLatch latch = new CountDownLatch(messageCount);
      environment.consumerBuilder().stream(stream)
          .offset(OffsetSpecification.first())
          .flow()
          .strategy(creditWhenHalfMessagesProcessed(1))
          .builder()
          .messageHandler(
              (ctx, message) ->
                  executorService.submit(
                      () -> {
                        latch.countDown();
                        ctx.processed();
                      }))
          .build();
      org.assertj.core.api.Assertions.assertThat(latch).is(completed());
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  void closeOnCondition() throws Exception {
    int messageCount = 50_000;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    client.declarePublisher(b(1), null, stream);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                client.publish(
                    b(1),
                    Collections.singletonList(
                        client.messageBuilder().addData("".getBytes()).build())));

    org.assertj.core.api.Assertions.assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    int messagesToProcess = 20_000;

    CountDownLatch consumeLatch = new CountDownLatch(1);
    AtomicInteger receivedMessages = new AtomicInteger();
    AtomicInteger processedMessages = new AtomicInteger();

    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  if (receivedMessages.incrementAndGet() <= messagesToProcess) {
                    processedMessages.incrementAndGet();
                  }
                  if (receivedMessages.get() == messagesToProcess) {
                    consumeLatch.countDown();
                  }
                })
            .build();

    org.assertj.core.api.Assertions.assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    consumer.close();
    org.assertj.core.api.Assertions.assertThat(processedMessages).hasValue(messagesToProcess);
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
  void consumerShouldBeClosedWhenStreamGetsDeleted(TestInfo info) throws Exception {
    String s = streamName(info);
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

    org.assertj.core.api.Assertions.assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    StreamConsumer consumer =
        (StreamConsumer)
            environment.consumerBuilder().stream(s)
                .offset(OffsetSpecification.first())
                .messageHandler((offset, message) -> consumeLatch.countDown())
                .build();

    org.assertj.core.api.Assertions.assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

    org.assertj.core.api.Assertions.assertThat(consumer.isOpen()).isTrue();

    environment.deleteStream(s);

    TestUtils.waitAtMost(10, () -> !consumer.isOpen());
    org.assertj.core.api.Assertions.assertThat(consumer.isOpen()).isFalse();
  }

  @Test
  void manualTrackingConsumerShouldRestartWhereItLeftOff() throws Exception {
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

    org.assertj.core.api.Assertions.assertThat(latchAssert(latchConfirmFirstWave)).completes();

    int storeEvery = 100;
    AtomicInteger consumedMessageCount = new AtomicInteger();
    AtomicReference<Consumer> consumerReference = new AtomicReference<>();
    AtomicLong lastStoredOffset = new AtomicLong(0);
    AtomicLong lastProcessedMessage = new AtomicLong(0);

    AtomicInteger storeCount = new AtomicInteger(0);
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .name("application-1")
            .manualTrackingStrategy()
            .checkInterval(Duration.ZERO)
            .builder()
            .messageHandler(
                (context, message) -> {
                  consumedMessageCount.incrementAndGet();
                  lastProcessedMessage.set(message.getProperties().getMessageIdAsLong());
                  if (consumedMessageCount.get() % storeEvery == 0) {
                    context.storeOffset();
                    lastStoredOffset.set(context.offset());
                    storeCount.incrementAndGet();
                  }
                })
            .build();

    ConsumerInfo consumerInfo = MonitoringTestUtils.extract(consumer);
    org.assertj.core.api.Assertions.assertThat(consumerInfo.getId()).isGreaterThanOrEqualTo(0);
    org.assertj.core.api.Assertions.assertThat(consumerInfo.getStream()).isEqualTo(stream);
    org.assertj.core.api.Assertions.assertThat(consumerInfo.getSubscriptionClient())
        .contains(" -> localhost:5552");
    org.assertj.core.api.Assertions.assertThat(consumerInfo.getTrackingClient())
        .contains(" -> localhost:5552");

    consumerReference.set(consumer);

    waitAtMost(10, () -> consumedMessageCount.get() == messageCountFirstWave);

    org.assertj.core.api.Assertions.assertThat(lastStoredOffset.get()).isPositive();

    consumer.close();

    messageSending.accept(messageCountSecondWave);

    org.assertj.core.api.Assertions.assertThat(latchAssert(latchConfirmSecondWave)).completes();

    AtomicLong firstOffset = new AtomicLong(0);
    consumer =
        environment.consumerBuilder().stream(stream)
            .name("application-1")
            .manualTrackingStrategy()
            .checkInterval(Duration.ZERO)
            .builder()
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
    // messages offset won't be contiguous, so it's not an exact match
    org.assertj.core.api.Assertions.assertThat(firstOffset.get())
        .isGreaterThanOrEqualTo(lastStoredOffset.get());

    consumer.close();
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void consumerShouldReUseInitialOffsetSpecificationAfterDisruptionIfNoMessagesReceived() {
    int messageCountFirstWave = 10_000;
    Producer producer = environment.producerBuilder().stream(stream).build();

    // send a first wave of messages, they should be consumed later
    CountDownLatch publishLatch = new CountDownLatch(messageCountFirstWave);
    IntStream.range(0, messageCountFirstWave)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("first wave".getBytes()).build(),
                    confirmationStatus -> publishLatch.countDown()));

    latchAssert(publishLatch).completes();

    // setting up the consumer, offset spec "next", it should only consume messages of the second
    // wave
    AtomicInteger consumedCount = new AtomicInteger(0);
    CountDownLatch consumeLatch = new CountDownLatch(1);
    Set<String> bodies = ConcurrentHashMap.newKeySet(10);
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.next())
        .messageHandler(
            (context, message) -> {
              String body = new String(message.getBodyAsBinary());
              bodies.add(body);
              if (body.contains("second wave")) {
                consumeLatch.countDown();
              }
            })
        .build();

    // killing the consumer connection to trigger an internal restart
    Cli.killConnection("rabbitmq-stream-consumer-0");

    // no messages should have been received
    org.assertj.core.api.Assertions.assertThat(consumedCount.get()).isZero();

    // starting the second wave, it sends a message every 100 ms
    AtomicBoolean keepPublishing = new AtomicBoolean(true);
    new Thread(
            () -> {
              while (keepPublishing.get()) {
                producer.send(
                    producer.messageBuilder().addData("second wave".getBytes()).build(),
                    confirmationStatus -> publishLatch.countDown());
                waitMs(100);
              }
            })
        .start();

    // the consumer should restart consuming with its initial offset spec, "next"
    try {
      latchAssert(consumeLatch).completes(recoveryInitialDelay.multipliedBy(2));
      org.assertj.core.api.Assertions.assertThat(bodies).hasSize(1).contains("second wave");
    } finally {
      keepPublishing.set(false);
    }
  }

  @ParameterizedTest
  @MethodSource
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void consumerShouldKeepConsumingAfterDisruption(
      java.util.function.Consumer<Object> disruption, TestInfo info) throws Exception {
    String s = streamName(info);
    environment.streamCreator().stream(s).create();
    StreamConsumer consumer = null;
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

      org.assertj.core.api.Assertions.assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
      producer.close();

      AtomicInteger receivedMessageCount = new AtomicInteger(0);
      CountDownLatch consumeLatch = new CountDownLatch(messageCount);
      CountDownLatch consumeLatchSecondWave = new CountDownLatch(messageCount * 2);
      consumer =
          (StreamConsumer)
              environment.consumerBuilder().stream(s)
                  .offset(OffsetSpecification.first())
                  .messageHandler(
                      (offset, message) -> {
                        receivedMessageCount.incrementAndGet();
                        consumeLatch.countDown();
                        consumeLatchSecondWave.countDown();
                      })
                  .build();

      org.assertj.core.api.Assertions.assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();

      org.assertj.core.api.Assertions.assertThat(consumer.isOpen()).isTrue();

      disruption.accept(s);

      Client client = cf.get();
      TestUtils.waitAtMost(
          recoveryInitialDelay.plusSeconds(2),
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

      org.assertj.core.api.Assertions.assertThat(publishLatchSecondWave.await(10, TimeUnit.SECONDS))
          .isTrue();
      producerSecondWave.close();

      latchAssert(consumeLatchSecondWave).completes(recoveryInitialDelay.plusSeconds(2));
      org.assertj.core.api.Assertions.assertThat(receivedMessageCount.get())
          .isBetween(messageCount * 2, messageCount * 2 + 1); // there can be a duplicate
      org.assertj.core.api.Assertions.assertThat(consumer.isOpen()).isTrue();

    } finally {
      if (consumer != null) {
        consumer.close();
      }
      environment.deleteStream(s);
    }
  }

  @Test
  void autoTrackingShouldStorePeriodicallyAndAfterInactivity() throws Exception {
    AtomicInteger messageCount = new AtomicInteger(0);
    int storeEvery = 10_000;
    String reference = "ref-1";
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    environment.consumerBuilder().name(reference).stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message) -> {
              lastReceivedOffset.set(context.offset());
              messageCount.incrementAndGet();
            })
        .autoTrackingStrategy()
        .flushInterval(Duration.ofSeconds(1).plusMillis(100))
        .messageCountBeforeStorage(storeEvery)
        .builder()
        .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    IntStream.range(0, storeEvery * 2)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> {}));

    waitAtMost(5, () -> messageCount.get() == storeEvery * 2);

    Client client = cf.get();
    waitAtMost(
        5, () -> client.queryOffset(reference, stream).getOffset() == lastReceivedOffset.get());

    int extraMessages = storeEvery / 10;
    IntStream.range(0, extraMessages)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> {}));

    waitAtMost(5, () -> messageCount.get() == storeEvery * 2 + extraMessages);

    waitAtMost(
        5, () -> client.queryOffset(reference, stream).getOffset() == lastReceivedOffset.get());
  }

  @Test
  void autoTrackingShouldStoreOffsetZeroAfterInactivity() throws Exception {
    String reference = "ref-1";
    AtomicLong lastReceivedOffset = new AtomicLong(-1);
    environment.consumerBuilder().name(reference).stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler((context, message) -> lastReceivedOffset.set(context.offset()))
        .autoTrackingStrategy()
        .flushInterval(Duration.ofSeconds(1).plusMillis(100))
        .builder()
        .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {});

    waitAtMost(() -> lastReceivedOffset.get() == 0);

    Client client = cf.get();
    waitAtMost(
        5,
        () -> {
          QueryOffsetResponse response = client.queryOffset(reference, stream);
          return response.isOk() && response.getOffset() == lastReceivedOffset.get();
        });
  }

  @Test
  void autoTrackingShouldStoreAfterClosing() throws Exception {
    int storeEvery = 10_000;
    int messageCount = storeEvery * 5 - 100;
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    String reference = "ref-1";
    AtomicLong lastReceivedOffset = new AtomicLong(0);
    Consumer consumer =
        environment.consumerBuilder().name(reference).stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                  consumeLatch.countDown();
                })
            .autoTrackingStrategy()
            .flushInterval(Duration.ofHours(1)) // long flush interval
            .messageCountBeforeStorage(storeEvery)
            .builder()
            .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> {}));

    latchAssert(consumeLatch).completes();
    consumer.close();

    Client client = cf.get();
    waitAtMost(
        5,
        () -> {
          QueryOffsetResponse response = client.queryOffset(reference, stream);
          // The field used to track and store the offset on closing may not be
          // up-to-date if the consumer closes "too fast", so checking with 1 unit behind.
          // This field is updated just after the message handler callback.
          return response.isOk()
              && (response.getOffset() == lastReceivedOffset.get()
                  || response.getOffset() == lastReceivedOffset.get() - 1);
        },
        () ->
            format(
                "Expecting stored offset %d to be equal to last received offset %d",
                client.queryOffset(reference, stream).getOffset(), lastReceivedOffset.get()));
  }

  @Test
  void autoTrackingShouldStoreOffsetZeroOnClosing() throws Exception {
    String reference = "ref-1";
    AtomicLong lastReceivedOffset = new AtomicLong(-1);
    Consumer consumer =
        environment.consumerBuilder().name(reference).stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) -> {
                  lastReceivedOffset.set(context.offset());
                })
            .autoTrackingStrategy()
            .flushInterval(Duration.ofHours(1)) // long flush interval
            .builder()
            .build();

    Producer producer = environment.producerBuilder().stream(stream).build();
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(), confirmationStatus -> {});
    waitAtMost(() -> lastReceivedOffset.get() == 0);
    consumer.close();
    Client client = cf.get();
    waitAtMost(
        5,
        () -> {
          QueryOffsetResponse response = client.queryOffset(reference, stream);
          return response.isOk() && response.getOffset() == lastReceivedOffset.get();
        },
        () ->
            format(
                "Expecting stored offset %d to be equal to last received offset %d",
                client.queryOffset(reference, stream).getOffset(), lastReceivedOffset.get()));
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void externalOffsetTrackingWithSubscriptionListener() throws Exception {
    AtomicInteger subscriptionListenerCallCount = new AtomicInteger(0);
    AtomicInteger receivedMessages = new AtomicInteger(0);
    AtomicLong offsetTracking = new AtomicLong(0);
    AtomicBoolean started = new AtomicBoolean(false);
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .subscriptionListener(
            subscriptionContext -> {
              subscriptionListenerCallCount.incrementAndGet();
              OffsetSpecification offsetSpecification =
                  started.get()
                      ? OffsetSpecification.offset(offsetTracking.get() + 1)
                      : subscriptionContext.offsetSpecification();
              subscriptionContext.offsetSpecification(offsetSpecification);
            })
        .messageHandler(
            (context, message) -> {
              receivedMessages.incrementAndGet();
              offsetTracking.set(context.offset());
              started.set(true);
            })
        .build();

    int messageCount = 10_000;
    Producer producer = environment.producerBuilder().stream(stream).build();
    Runnable publish =
        () ->
            IntStream.range(0, messageCount)
                .forEach(
                    i ->
                        producer.send(
                            producer.messageBuilder().addData("".getBytes()).build(),
                            confirmationStatus -> {}));

    publish.run();

    waitAtMost(5, () -> receivedMessages.get() == messageCount);
    org.assertj.core.api.Assertions.assertThat(offsetTracking.get())
        .isGreaterThanOrEqualTo(messageCount - 1);

    Cli.killConnection("rabbitmq-stream-consumer-0");
    waitAtMost(
        recoveryInitialDelay.multipliedBy(2), () -> subscriptionListenerCallCount.get() == 2);

    publish.run();
    waitAtMost(5, () -> receivedMessages.get() == messageCount * 2);
    org.assertj.core.api.Assertions.assertThat(offsetTracking.get())
        .isGreaterThanOrEqualTo(messageCount * 2 - 1);
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void duplicatesWhenResubscribeAfterDisconnectionWithLongFlushInterval() throws Exception {
    AtomicInteger receivedMessages = new AtomicInteger(0);
    int storeEvery = 10_000;
    String reference = "ref-1";
    AtomicBoolean receivedPoison = new AtomicBoolean(false);
    environment.consumerBuilder().name(reference).stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message) -> {
              receivedMessages.incrementAndGet();
              if ("poison".equals(new String(message.getBodyAsBinary()))) {
                receivedPoison.set(true);
              }
            })
        .autoTrackingStrategy()
        .flushInterval(Duration.ofMinutes(60)) // long flush interval
        .messageCountBeforeStorage(storeEvery)
        .builder()
        .build();

    AtomicInteger publishedMessages = new AtomicInteger(0);
    Producer producer = environment.producerBuilder().stream(stream).build();
    IntConsumer publish =
        messagesToPublish -> {
          publishedMessages.addAndGet(messagesToPublish);
          IntStream.range(0, messagesToPublish)
              .forEach(
                  i ->
                      producer.send(
                          producer.messageBuilder().addData("".getBytes()).build(),
                          confirmationStatus -> {}));
        };
    publish.accept(storeEvery * 2 - 100);
    waitAtMost(5, () -> receivedMessages.get() == publishedMessages.get());
    Cli.killConnection("rabbitmq-stream-consumer-0");

    publish.accept(storeEvery * 2);
    waitAtMost(
        () -> {
          producer.send(
              producer.messageBuilder().addData("poison".getBytes()).build(),
              confirmationStatus -> {});
          publishedMessages.incrementAndGet();
          return receivedPoison.get();
        });

    // we have duplicates because the last stored value is behind and the re-subscription uses it
    org.assertj.core.api.Assertions.assertThat(receivedMessages)
        .hasValueGreaterThan(publishedMessages.get());
  }

  @Test
  @DisabledIfRabbitMqCtlNotSet
  void useSubscriptionListenerToRestartExactlyWhereDesired() throws Exception {
    AtomicInteger subscriptionListenerCallCount = new AtomicInteger(0);
    AtomicInteger receivedMessages = new AtomicInteger(0);
    AtomicLong offsetTracking = new AtomicLong(0);
    AtomicBoolean started = new AtomicBoolean(false);
    int storeEvery = 10_000;
    String reference = "ref-1";
    CountDownLatch poisonLatch = new CountDownLatch(1);
    environment.consumerBuilder().name(reference).stream(stream)
        .offset(OffsetSpecification.first())
        .subscriptionListener(
            subscriptionContext -> {
              subscriptionListenerCallCount.getAndIncrement();
              OffsetSpecification offsetSpecification =
                  started.get()
                      ? OffsetSpecification.offset(offsetTracking.get() + 1)
                      : subscriptionContext.offsetSpecification();
              subscriptionContext.offsetSpecification(offsetSpecification);
            })
        .messageHandler(
            (context, message) -> {
              receivedMessages.incrementAndGet();
              offsetTracking.set(context.offset());
              started.set(true);
              if ("poison".equals(new String(message.getBodyAsBinary()))) {
                poisonLatch.countDown();
              }
            })
        .autoTrackingStrategy()
        .flushInterval(Duration.ofMinutes(60)) // long flush interval
        .messageCountBeforeStorage(storeEvery)
        .builder()
        .build();

    AtomicInteger publishedMessages = new AtomicInteger(0);
    Producer producer = environment.producerBuilder().stream(stream).build();
    IntConsumer publish =
        messagesToPublish -> {
          publishedMessages.addAndGet(messagesToPublish);
          IntStream.range(0, messagesToPublish)
              .forEach(
                  i ->
                      producer.send(
                          producer.messageBuilder().addData("".getBytes()).build(),
                          confirmationStatus -> {}));
        };
    publish.accept(storeEvery * 2 - 100);
    waitAtMost(5, () -> receivedMessages.get() == publishedMessages.get());
    Cli.killConnection("rabbitmq-stream-consumer-0");

    publish.accept(storeEvery * 2);
    producer.send(
        producer.messageBuilder().addData("poison".getBytes()).build(), confirmationStatus -> {});
    latchAssert(poisonLatch).completes(recoveryInitialDelay.plusSeconds(2));
    // no duplicates because the custom offset tracking overrides the stored offset in the
    // subscription listener
    org.assertj.core.api.Assertions.assertThat(receivedMessages)
        .hasValue(publishedMessages.get() + 1);
  }

  @Test
  void offsetZeroShouldBeStored() throws Exception {
    String ref = "ref-1";
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .name(ref)
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> {})
            .manualTrackingStrategy()
            .checkInterval(Duration.ZERO)
            .builder()
            .build();
    assertThatThrownBy(() -> consumer.storedOffset()).isInstanceOf(NoOffsetException.class);
    consumer.store(0);
    waitAtMost(() -> consumer.storedOffset() == 0);
  }

  @Test
  void methodsShouldThrowExceptionWhenConsumerIsClosed() {
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .messageHandler((context, message) -> {})
            .build();
    consumer.close();
    ThrowingCallable[] calls =
        new ThrowingCallable[] {() -> consumer.store(1), () -> consumer.storedOffset()};
    Arrays.stream(calls)
        .forEach(call -> assertThatThrownBy(call).isInstanceOf(IllegalStateException.class));
  }

  @Test
  void creationShouldFailWithDetailsWhenUnknownHost() {
    Address localhost = localhost();
    // first connection is locator
    AtomicInteger connectionCount = new AtomicInteger(0);
    EnvironmentBuilder builder =
        environmentBuilder()
            .host(localhost.host())
            .port(localhost.port())
            .netty()
            .bootstrapCustomizer(b -> b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1_000))
            .environmentBuilder()
            .addressResolver(
                n ->
                    connectionCount.getAndIncrement() == 0
                        ? n
                        : new Address(UUID.randomUUID().toString(), Client.DEFAULT_PORT));
    try (Environment env = builder.build()) {
      assertThatThrownBy(
              () ->
                  env.consumerBuilder().stream(stream)
                      .messageHandler((context, message) -> {})
                      .build())
          .hasMessageContaining(
              "https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#understanding-connection-logic")
          .hasMessageContaining(
              "https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#with-a-load-balancer")
          .cause()
          .isInstanceOfAny(ConnectTimeoutException.class, UnknownHostException.class);
    }
  }

  @Test
  void resetOffsetTrackingFromEnvironment() {
    int messageCount = 100;
    publishAndWaitForConfirms(cf, messageCount, stream);
    String reference = "app";
    Sync sync = sync(messageCount);
    AtomicLong lastOffset = new AtomicLong(0);
    Supplier<Consumer> consumerSupplier =
        () ->
            environment.consumerBuilder().stream(stream)
                .name(reference)
                .offset(OffsetSpecification.first())
                .messageHandler(
                    (context, message) -> {
                      lastOffset.set(context.offset());
                      sync.down();
                    })
                .autoTrackingStrategy()
                .builder()
                .build();
    // consumer gets the initial message batch and stores the offset on closing
    Consumer consumer = consumerSupplier.get();
    assertThat(sync).completes();
    consumer.close();

    // we'll publish 1 more message and make sure the consumers only consumes that one
    // (because it restarts where it left off)
    long limit = lastOffset.get();
    sync.reset(1);
    consumer = consumerSupplier.get();

    publishAndWaitForConfirms(cf, 1, stream);

    assertThat(sync).completes();
    org.assertj.core.api.Assertions.assertThat(lastOffset).hasValueGreaterThan(limit);
    consumer.close();

    // we reset the offset to 0, the consumer should restart from the beginning
    environment.storeOffset(reference, stream, 0);
    sync.reset(messageCount + 1);
    consumer = consumerSupplier.get();

    assertThat(sync).completes();
    consumer.close();
  }

  @Test
  void asynchronousProcessingWithInMemoryQueue(TestInfo info) {
    int messageCount = 100_000;
    publishAndWaitForConfirms(cf, messageCount, stream);

    CountDownLatch latch = new CountDownLatch(messageCount);

    MessageHandler handler = (ctx, msg) -> latch.countDown();
    DispatchingMessageHandler dispatchingHandler =
        new DispatchingMessageHandler(
            handler, ThreadUtils.threadFactory(info.getTestMethod().get().getName()));

    try {
      environment.consumerBuilder().stream(stream)
          .offset(OffsetSpecification.first())
          .flow()
          .strategy(creditWhenHalfMessagesProcessed(1))
          .builder()
          .messageHandler(dispatchingHandler)
          .build();
      org.assertj.core.api.Assertions.assertThat(latch).is(completed());
    } finally {
      dispatchingHandler.close();
    }
  }

  private static final class DispatchingMessageHandler implements MessageHandler, AutoCloseable {

    private final MessageHandler delegate;
    private final BlockingQueue<ContextMessageWrapper> queue = new ArrayBlockingQueue<>(10_000);
    private final Thread t;

    private DispatchingMessageHandler(MessageHandler delegate, ThreadFactory tf) {
      this.delegate = delegate;
      t =
          tf.newThread(
              () -> {
                try {
                  while (!Thread.currentThread().isInterrupted()) {
                    ContextMessageWrapper item = queue.poll(10, TimeUnit.SECONDS);
                    if (item != null) {
                      try {
                        this.delegate.handle(item.ctx, item.msg());
                      } finally {
                        item.ctx.processed();
                      }
                    }
                  }
                } catch (InterruptedException e) {
                  // finish the thread
                }
              });
      t.start();
    }

    @Override
    public void handle(Context context, Message message) {
      this.queue.add(new ContextMessageWrapper(context, message));
    }

    @Override
    public void close() {
      this.t.interrupt();
    }
  }

  private static final class ContextMessageWrapper {

    private final MessageHandler.Context ctx;
    private final Message msg;

    private ContextMessageWrapper(MessageHandler.Context ctx, Message msg) {
      this.ctx = ctx;
      this.msg = msg;
    }

    private MessageHandler.Context ctx() {
      return this.ctx;
    }

    private Message msg() {
      return this.msg;
    }
  }
}
