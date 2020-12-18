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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.StreamProducer.Status;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamProducerTest {

  String stream;
  EventLoopGroup eventLoopGroup;

  Environment environment;

  TestUtils.ClientFactory cf;

  @BeforeEach
  void init() {
    EnvironmentBuilder environmentBuilder =
        Environment.builder()
            .eventLoopGroup(eventLoopGroup)
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)))
            .topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)));
    ((StreamEnvironmentBuilder) environmentBuilder).hostResolver(h -> "localhost");
    environment = environmentBuilder.build();
  }

  @AfterEach
  void tearDown() throws Exception {
    environment.close();
  }

  @Test
  void send() throws Exception {
    int batchSize = 100;
    int messageCount = 10_000 * batchSize + 1; // don't want a multiple of batch size
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Producer producer = environment.producerBuilder().stream(stream).batchSize(batchSize).build();
    AtomicLong count = new AtomicLong(0);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              producer.send(
                  producer.messageBuilder().addData("".getBytes()).build(),
                  confirmationStatus -> {
                    count.incrementAndGet();
                    publishLatch.countDown();
                  });
            });
    boolean completed = publishLatch.await(10, TimeUnit.SECONDS);
    assertThat(completed).isTrue();
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

    Host.killConnection("rabbitmq-stream-producer");

    waitAtMost(10, () -> ((StreamProducer) producer).status() == Status.NOT_AVAILABLE);
    canPublish.set(false);

    assertThat(confirmed.get()).isPositive();
    waitAtMost(
        5,
        () -> confirmed.get() + errored.get() == published.get(),
        () ->
            String.format(
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

    waitAtMost(10, () -> ((StreamProducer) producer).status() == StreamProducer.Status.RUNNING);

    int confirmedAfterUnavailability = confirmed.get();
    int errorAfterUnavailability = errored.get();

    canPublish.set(true);

    waitAtMost(10, () -> confirmed.get() > confirmedAfterUnavailability * 2);

    assertThat(errored.get()).isEqualTo(errorAfterUnavailability);

    canPublish.set(false);
    publishThread.interrupt();

    waitAtMost(10, () -> confirmed.get() + errored.get() == published.get());

    CountDownLatch consumeLatch = new CountDownLatch(confirmed.get());
    environment.consumerBuilder().stream(stream)
        .messageHandler(
            (offset, message) -> {
              consumeLatch.countDown();
            })
        .build();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void messagesShouldBeDeDuplicatedWhenUsingNameAndPublishingId(int subEntrySize) throws Exception {
    int lineCount = 50_000;
    int firstWaveLineCount = lineCount / 5;
    int backwardCount = firstWaveLineCount / 10;
    SortedSet<Integer> document = new TreeSet<>();
    IntStream.range(0, lineCount).forEach(i -> document.add(i));
    Producer producer =
        environment.producerBuilder().name("producer-1").stream(stream)
            .subEntrySize(subEntrySize)
            .build();

    AtomicReference<CountDownLatch> latch =
        new AtomicReference<>(new CountDownLatch(firstWaveLineCount));
    ConfirmationHandler confirmationHandler = confirmationStatus -> latch.get().countDown();
    Consumer<Integer> publishMessage =
        i ->
            producer.send(
                producer
                    .messageBuilder()
                    .publishingId(i)
                    .addData(String.valueOf(i).getBytes())
                    .build(),
                confirmationHandler);
    document.headSet(firstWaveLineCount).forEach(publishMessage);

    assertThat(latch.get().await(10, TimeUnit.SECONDS)).isTrue();

    latch.set(new CountDownLatch(lineCount - firstWaveLineCount + backwardCount));

    document.tailSet(firstWaveLineCount - backwardCount).forEach(publishMessage);

    assertThat(latch.get().await(5, TimeUnit.SECONDS)).isTrue();

    CountDownLatch consumeLatch = new CountDownLatch(lineCount);
    AtomicInteger consumed = new AtomicInteger();
    environment.consumerBuilder().stream(stream)
        .messageHandler(
            (offset, message) -> {
              consumed.incrementAndGet();
              consumeLatch.countDown();
            })
        .build();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(1000);
    // if we are using sub-entries, we cannot avoid duplicates.
    // here, a sub-entry in the second wave, right at the end of the re-submitted
    // values will contain those duplicates, because its publishing ID will be
    // the one of its last message, so the server will accept the whole sub-entry,
    // including the duplicates.
    assertThat(consumed.get()).isEqualTo(lineCount + backwardCount % subEntrySize);
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
}
