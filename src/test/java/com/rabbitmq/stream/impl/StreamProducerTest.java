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

import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.impl.MonitoringTestUtils.ProducerInfo;
import com.rabbitmq.stream.impl.StreamProducer.Status;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import wiremock.org.checkerframework.checker.units.qual.A;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamProducerTest {

  String stream;
  EventLoopGroup eventLoopGroup;

  Environment environment;

  TestUtils.ClientFactory cf;

  @BeforeEach
  void init() {
    EnvironmentBuilder environmentBuilder = environmentBuilder();
    environment = environmentBuilder.build();
  }

  private EnvironmentBuilder environmentBuilder() {
    return Environment.builder()
        .netty()
        .eventLoopGroup(eventLoopGroup)
        .environmentBuilder()
        .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)))
        .topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)));
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  private static AtomicLong rate() {
    AtomicLong count = new AtomicLong();
    AtomicLong tick = new AtomicLong(System.nanoTime());

    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(
            () -> {
              long now = System.nanoTime();
              long before = tick.getAndSet(now);
              long elapsed = now - before;
              long sent = count.getAndSet(0);
              System.out.println("Rate " + (sent * 1_000_000_000L / elapsed) + " msg/s");
            },
            1,
            1,
            TimeUnit.SECONDS);
    return count;
  }

  @Test
  void test() {
    AtomicLong count = rate();
    Producer producer = environment.producerBuilder().stream(stream)
        .maxUnconfirmedMessages(10)
        .build();

    while(true) {
      producer.send(producer.messageBuilder().build(), s -> { });
      count.incrementAndGet();
    }

  }

  @Test
  void client() throws Exception {
    int permits = 10;
    Semaphore semaphore = new Semaphore(permits);
    Client client = cf.get(new Client.ClientParameters().publishConfirmListener(new Client.PublishConfirmListener() {
      @Override
      public void handle(byte publisherId, long publishingId) {
        semaphore.release();
      }
    }));

    byte pubId = (byte) 0;
    client.declarePublisher(pubId, null, stream);

    AtomicLong count = rate();

    List<Message> messages = IntStream.range(0, permits).mapToObj(ignored -> client
        .messageBuilder()
        .addData("hello".getBytes(StandardCharsets.UTF_8))
        .build()).collect(Collectors.toList());
    while (true) {
      semaphore.acquire(permits);
      client.publish(pubId, messages);
      count.addAndGet(permits);
    }

  }

  @Test
  void send() throws Exception {
    int batchSize = 10;
    int messageCount = 10 * batchSize + 1; // don't want a multiple of batch size
    Sync confirmSync = sync(messageCount);
    Producer producer = environment.producerBuilder().stream(stream).batchSize(batchSize).build();
    AtomicLong count = new AtomicLong(0);
    AtomicLong sequence = new AtomicLong(0);
    Set<Long> idsSent = ConcurrentHashMap.newKeySet(messageCount);
    Set<Long> idsConfirmed = ConcurrentHashMap.newKeySet(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              long id = sequence.getAndIncrement();
              idsSent.add(id);
              producer.send(
                  producer
                      .messageBuilder()
                      .properties()
                      .messageId(id)
                      .messageBuilder()
                      .addData("".getBytes())
                      .build(),
                  confirmationStatus -> {
                    idsConfirmed.add(
                        confirmationStatus.getMessage().getProperties().getMessageIdAsLong());
                    count.incrementAndGet();
                    confirmSync.down();
                  });
            });
    assertThat(confirmSync).completes();
    assertThat(idsSent).hasSameSizeAs(idsConfirmed);
    idsSent.forEach(idSent -> assertThat(idsConfirmed).contains(idSent));

    ProducerInfo info = MonitoringTestUtils.extract(producer);
    assertThat(info.getId()).isGreaterThanOrEqualTo(0);
    assertThat(info.getStream()).isEqualTo(stream);
    assertThat(info.getPublishingClient()).contains(" -> localhost:5552");
  }

  @Test
  void sendWithMultipleProducers() throws Exception {
    int batchSize = 10;
    int messageCount = 1_000 * batchSize + 1; // don't want a multiple of batch size
    int nbProducers = 20;
    Map<String, CountDownLatch> publishLatches = new ConcurrentHashMap<>(nbProducers);
    Map<String, Producer> producers = new ConcurrentHashMap<>(nbProducers);
    List<String> producerNames =
        IntStream.range(0, nbProducers)
            .mapToObj(
                i -> {
                  String producerName = UUID.randomUUID().toString();
                  publishLatches.put(producerName, new CountDownLatch(messageCount));
                  producers.put(
                      producerName,
                      environment.producerBuilder().stream(stream).batchSize(batchSize).build());
                  return producerName;
                })
            .collect(Collectors.toList());

    AtomicLong count = new AtomicLong(0);
    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      producerNames.forEach(
          name -> {
            CountDownLatch publishLatch = publishLatches.get(name);
            Producer producer = producers.get(name);
            Runnable publishRunnable =
                () -> {
                  IntStream.range(0, messageCount)
                      .forEach(
                          i -> {
                            producer.send(
                                producer.messageBuilder().addData(name.getBytes()).build(),
                                confirmationStatus -> {
                                  count.incrementAndGet();
                                  publishLatch.countDown();
                                });
                          });
                };
            executorService.submit(publishRunnable);
          });

      for (CountDownLatch publishLatch : publishLatches.values()) {
        boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
        assertThat(completed).isTrue();
      }
    } finally {
      executorService.shutdownNow();
    }
  }

  @Test
  void sendWithSubEntryBatches() throws Exception {
    int batchSize = 100;
    int messagesInBatch = 10;
    int messageCount = 1_000 * batchSize + 1; // don't want a multiple of batch size
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Producer producer =
        environment.producerBuilder().stream(stream)
            .subEntrySize(messagesInBatch)
            .batchSize(batchSize)
            .build();
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              producer.send(
                  producer.messageBuilder().addData("".getBytes()).build(),
                  confirmationStatus -> {
                    publishLatch.countDown();
                  });
            });
    boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
  }

  @Test
  void sendToNonExistingStreamShouldReturnUnconfirmedStatus() throws Exception {
    Client client = cf.get();
    String s = UUID.randomUUID().toString();
    Client.Response response = client.create(s);
    assertThat(response.isOk()).isTrue();

    Producer producer = environment.producerBuilder().stream(s).build();

    response = client.delete(s);
    assertThat(response.isOk()).isTrue();

    // it must close
    waitAtMost(10, () -> !((StreamProducer) producer).isOpen());

    CountDownLatch confirmationLatch = new CountDownLatch(1);
    AtomicReference<ConfirmationStatus> confirmationStatusReference = new AtomicReference<>();
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(),
        confirmationStatus -> {
          confirmationStatusReference.set(confirmationStatus);
          confirmationLatch.countDown();
        });

    assertThat(confirmationLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(confirmationStatusReference.get()).isNotNull();
    assertThat(confirmationStatusReference.get().isConfirmed()).isFalse();
    assertThat(confirmationStatusReference.get().getCode())
        .isEqualTo(Constants.CODE_PRODUCER_CLOSED);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 10})
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void shouldRecoverAfterConnectionIsKilled(int subEntrySize) throws Exception {
    Producer producer =
        environment.producerBuilder().subEntrySize(subEntrySize).stream(stream).build();

    AtomicInteger published = new AtomicInteger(0);
    AtomicInteger confirmed = new AtomicInteger(0);
    AtomicInteger errored = new AtomicInteger(0);

    AtomicBoolean canPublish = new AtomicBoolean(true);
    Thread publishThread =
        new Thread(
            () -> {
              ConfirmationHandler confirmationHandler =
                  confirmationStatus -> {
                    if (confirmationStatus.isConfirmed()) {
                      confirmed.incrementAndGet();
                    } else {
                      errored.incrementAndGet();
                    }
                  };
              while (true) {
                try {
                  if (canPublish.get()) {
                    producer.send(
                        producer
                            .messageBuilder()
                            .addData("".getBytes(StandardCharsets.UTF_8))
                            .build(),
                        confirmationHandler);
                    published.incrementAndGet();
                  } else {
                    Thread.sleep(500);
                  }
                } catch (InterruptedException | StreamException e) {
                  // OK
                }
              }
            });
    publishThread.start();

    Thread.sleep(1000L);

    Cli.killConnection("rabbitmq-stream-producer-0");

    waitAtMost(() -> ((StreamProducer) producer).status() == Status.NOT_AVAILABLE);
    canPublish.set(false);

    assertThat(confirmed.get()).isPositive();
    waitAtMost(
        5,
        () -> confirmed.get() + errored.get() == published.get(),
        () ->
            format(
                "confirmed %d / errored %d / published %d, %d + %d = %d != %d, difference %d",
                confirmed.get(),
                errored.get(),
                published.get(),
                confirmed.get(),
                errored.get(),
                (confirmed.get() + errored.get()),
                published.get(),
                (published.get() - (confirmed.get() + errored.get()))));
    assertThat(confirmed.get() + errored.get()).isEqualTo(published.get());

    waitAtMost(() -> ((StreamProducer) producer).status() == StreamProducer.Status.RUNNING);

    int confirmedAfterUnavailability = confirmed.get();
    int errorAfterUnavailability = errored.get();

    canPublish.set(true);

    waitAtMost(
        () -> confirmed.get() > confirmedAfterUnavailability * 2,
        () ->
            format(
                "Confirmed %d, confirmed after unavailability %d, expecting twice as many",
                confirmed.get(), confirmedAfterUnavailability));

    assertThat(errored.get()).isEqualTo(errorAfterUnavailability);

    canPublish.set(false);
    publishThread.interrupt();

    waitAtMost(
        () -> confirmed.get() + errored.get() == published.get(),
        () ->
            format(
                "Confirmed %d, errored %d, published %d, expecting %d, but got %d",
                confirmed.get(),
                errored.get(),
                published.get(),
                published.get(),
                (confirmed.get() + errored.get())));

    CountDownLatch consumeLatch = new CountDownLatch(confirmed.get());
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (offset, message) -> {
              consumeLatch.countDown();
            })
        .build();
    latchAssert(consumeLatch).completes();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void producerShouldBeClosedWhenStreamIsDeleted(int subEntrySize, TestInfo info) throws Exception {
    String s = streamName(info);
    environment.streamCreator().stream(s).create();

    StreamProducer producer =
        (StreamProducer) environment.producerBuilder().subEntrySize(subEntrySize).stream(s).build();

    AtomicInteger published = new AtomicInteger(0);
    AtomicInteger confirmed = new AtomicInteger(0);
    AtomicInteger errored = new AtomicInteger(0);
    Set<Number> errorCodes = ConcurrentHashMap.newKeySet();

    AtomicBoolean continuePublishing = new AtomicBoolean(true);
    Thread publishThread =
        new Thread(
            () -> {
              ConfirmationHandler confirmationHandler =
                  confirmationStatus -> {
                    if (confirmationStatus.isConfirmed()) {
                      confirmed.incrementAndGet();
                    } else {
                      errored.incrementAndGet();
                      errorCodes.add(confirmationStatus.getCode());
                    }
                  };
              while (continuePublishing.get()) {
                try {
                  producer.send(
                      producer
                          .messageBuilder()
                          .addData("".getBytes(StandardCharsets.UTF_8))
                          .build(),
                      confirmationHandler);
                  published.incrementAndGet();
                } catch (StreamException e) {
                  // OK
                }
              }
            });
    publishThread.start();

    waitAtMost(() -> confirmed.get() > 100);
    int confirmedNow = confirmed.get();
    waitAtMost(() -> confirmed.get() > confirmedNow + 1000);

    assertThat(producer.isOpen()).isTrue();

    environment.deleteStream(s);

    waitAtMost(() -> !producer.isOpen());
    continuePublishing.set(false);
    waitAtMost(
        () -> !errorCodes.isEmpty(),
        () -> "The producer should have received negative publish confirms");
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void messagesShouldBeDeDuplicatedWhenUsingNameAndPublishingId(int subEntrySize) throws Exception {
    int lineCount = 50_000;
    int firstWaveLineCount = lineCount / 5;
    int backwardCount = firstWaveLineCount / 10;
    SortedSet<Integer> document = new TreeSet<>();
    IntStream.range(0, lineCount).forEach(document::add);
    Producer producer =
        environment.producerBuilder().name("producer-1").stream(stream)
            .subEntrySize(subEntrySize)
            .build();

    Sync confirmSync = sync(firstWaveLineCount);
    ConfirmationHandler confirmationHandler = confirmationStatus -> confirmSync.down();
    Consumer<Integer> publishMessage =
        i ->
            producer.send(
                producer
                    .messageBuilder()
                    .publishingId(i)
                    .addData(String.valueOf(i).getBytes())
                    .build(),
                confirmationHandler);
    // publish the first wave
    document.headSet(firstWaveLineCount).forEach(publishMessage);

    assertThat(confirmSync).completes();

    confirmSync.reset(lineCount - firstWaveLineCount + backwardCount);

    // publish the rest, but with some overlap from the first wave
    document.tailSet(firstWaveLineCount - backwardCount).forEach(publishMessage);

    assertThat(confirmSync).completes();

    CountDownLatch consumeLatch = new CountDownLatch(lineCount);
    AtomicInteger consumed = new AtomicInteger();
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (offset, message) -> {
              consumed.incrementAndGet();
              consumeLatch.countDown();
            })
        .build();
    assertThat(consumeLatch.await(5, TimeUnit.SECONDS)).isTrue();
    if (subEntrySize == 1) {
      assertThat(consumed.get()).isEqualTo(lineCount);
    } else {
      // if we are using sub-entries, we cannot avoid duplicates.
      // here, a sub-entry in the second wave, right at the end of the re-submitted
      // values will contain those duplicates, because its publishing ID will be
      // the one of its last message, so the server will accept the whole sub-entry,
      // including the duplicates.
      assertThat(consumed.get()).isBetween(lineCount, lineCount + subEntrySize);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void newIncarnationOfProducerCanQueryItsLastPublishingId(int subEntrySize) throws Exception {
    Producer p =
        environment.producerBuilder().name("producer-1").stream(stream)
            .subEntrySize(subEntrySize)
            .build();

    AtomicReference<Producer> producer = new AtomicReference<>(p);

    AtomicLong publishingSequence = new AtomicLong(0);
    AtomicLong lastConfirmed = new AtomicLong(-1);
    ConfirmationHandler confirmationHandler =
        confirmationStatus -> {
          if (confirmationStatus.isConfirmed()) {
            lastConfirmed.set(confirmationStatus.getMessage().getPublishingId());
          }
        };

    AtomicBoolean canPublish = new AtomicBoolean(true);
    Runnable publish =
        () -> {
          while (canPublish.get()) {
            producer
                .get()
                .send(
                    producer
                        .get()
                        .messageBuilder()
                        .publishingId(publishingSequence.getAndIncrement())
                        .addData(String.valueOf(publishingSequence.get()).getBytes())
                        .build(),
                    confirmationHandler);
          }
        };
    new Thread(publish).start();

    Thread.sleep(1000L);
    canPublish.set(false);
    waitAtMost(10, () -> publishingSequence.get() == lastConfirmed.get() + 1);
    assertThat(lastConfirmed.get()).isPositive();

    producer.get().close();

    p =
        environment.producerBuilder().name("producer-1").stream(stream)
            .subEntrySize(subEntrySize)
            .build();
    producer.set(p);

    long lastPublishingId = producer.get().getLastPublishingId();
    assertThat(lastPublishingId).isEqualTo(lastConfirmed.get());

    canPublish.set(true);
    new Thread(publish).start();

    Thread.sleep(1000L);
    canPublish.set(false);

    waitAtMost(10, () -> publishingSequence.get() == lastConfirmed.get() + 1);
    assertThat(lastConfirmed.get()).isGreaterThan(lastPublishingId);

    CountDownLatch consumeLatch = new CountDownLatch((int) (lastConfirmed.get() + 1));
    AtomicInteger consumed = new AtomicInteger();
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (offset, message) -> {
              consumed.incrementAndGet();
              consumeLatch.countDown();
            })
        .build();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(1000);
    assertThat(consumed.get()).isEqualTo(lastConfirmed.get() + 1);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void firstMessagesShouldNotBeFilteredOutWhenNamedProducerRestarts(int subEntrySize, TestInfo info)
      throws Exception {
    int messageCount = 10_000;
    String producerName = info.getTestMethod().get().getName();
    AtomicReference<Producer> producer =
        new AtomicReference<>(
            environment.producerBuilder().name(producerName).subEntrySize(subEntrySize).stream(
                    stream)
                .build());

    AtomicReference<CountDownLatch> publishLatch =
        new AtomicReference<>(new CountDownLatch(messageCount));

    IntConsumer publishing =
        i ->
            producer
                .get()
                .send(
                    producer.get().messageBuilder().addData("".getBytes()).build(),
                    confirmationStatus -> publishLatch.get().countDown());

    IntStream.range(0, messageCount).forEach(publishing);
    assertThat(publishLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    producer.get().close();

    publishLatch.set(new CountDownLatch(messageCount));
    producer.set(
        environment.producerBuilder().name(producerName).subEntrySize(subEntrySize).stream(stream)
            .build());

    IntStream.range(0, messageCount).forEach(publishing);
    assertThat(publishLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    producer.get().close();

    CountDownLatch consumeLatch = new CountDownLatch(messageCount * 2);
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler((ctx, msg) -> consumeLatch.countDown())
        .build();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
  }

  @Test
  void subEntryBatchesSentCompressedShouldBeConsumedProperly() {
    int messagePerProducer = 10000;
    int messageCount = Compression.values().length * messagePerProducer;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    ConfirmationHandler confirmationHandler = confirmationStatus -> publishLatch.countDown();
    AtomicInteger messageIndex = new AtomicInteger(0);
    Set<String> publishedBodies = ConcurrentHashMap.newKeySet(messageCount);
    for (Compression compression : Compression.values()) {
      Producer producer =
          environment.producerBuilder().stream(stream)
              .subEntrySize(100)
              .compression(compression)
              .build();
      IntStream.range(0, messagePerProducer)
          .forEach(
              i -> {
                String body =
                    "compression "
                        + compression.name()
                        + " message "
                        + messageIndex.getAndIncrement();
                producer.send(
                    producer
                        .messageBuilder()
                        .addData(body.getBytes(StandardCharsets.UTF_8))
                        .build(),
                    confirmationHandler);
                publishedBodies.add(body);
              });
    }

    assertThat(latchAssert(publishLatch)).completes();

    Set<String> consumedBodies = ConcurrentHashMap.newKeySet(messageCount);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    environment.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message) -> {
              consumedBodies.add(new String(message.getBodyAsBinary(), StandardCharsets.UTF_8));
              consumeLatch.countDown();
            })
        .build();

    assertThat(latchAssert(consumeLatch)).completes();
    assertThat(consumedBodies).isNotEmpty().hasSameSizeAs(publishedBodies);
    publishedBodies.forEach(
        publishBody -> assertThat(consumedBodies.contains(publishBody)).isTrue());
  }

  @Test
  void methodsShouldThrowExceptionWhenProducerIsClosed() {
    Producer producer = environment.producerBuilder().stream(stream).build();
    producer.close();
    assertThatThrownBy(producer::getLastPublishingId).isInstanceOf(IllegalStateException.class);
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
      assertThatThrownBy(() -> env.producerBuilder().stream(stream).build())
          .hasMessageContaining(
              "https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#understanding-connection-logic")
          .hasMessageContaining(
              "https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/#with-a-load-balancer")
          .cause()
          .isInstanceOfAny(ConnectTimeoutException.class, UnknownHostException.class);
    }
  }
}
