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

import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamProducerTest {

  String stream;
  EventLoopGroup eventLoopGroup;

  Environment environment;

  TestUtils.ClientFactory cf;

  @BeforeEach
  void init() {
    environment =
        Environment.builder()
            .eventLoopGroup(eventLoopGroup)
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(2)))
            .build();
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
        .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
  }

  @Test
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void shouldRecoverAfterConnectionIsKilled() throws Exception {
    Producer producer = environment.producerBuilder().stream(stream).build();

    AtomicInteger published = new AtomicInteger(0);
    AtomicInteger confirmed = new AtomicInteger(0);
    AtomicInteger errored = new AtomicInteger(0);

    AtomicBoolean canPublish = new AtomicBoolean(true);
    CountDownLatch unavailabilityLatch = new CountDownLatch(1);
    Thread publishThread =
        new Thread(
            () -> {
              ConfirmationHandler confirmationHandler =
                  confirmationStatus -> {
                    if (confirmationStatus.isConfirmed()) {
                      confirmed.incrementAndGet();
                    } else {
                      errored.incrementAndGet();
                      if (confirmationStatus.getCode() == Constants.CODE_PRODUCER_NOT_AVAILABLE) {
                        unavailabilityLatch.countDown();
                      }
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

    assertThat(unavailabilityLatch.await(10, TimeUnit.SECONDS)).isTrue();
    canPublish.set(false);

    assertThat(((StreamProducer) producer).status()).isEqualTo(StreamProducer.Status.NOT_AVAILABLE);

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
    int confirmedAfterUnavailability = confirmed.get();
    int errorAfterUnavailability = errored.get();

    waitAtMost(10, () -> ((StreamProducer) producer).status() == StreamProducer.Status.RUNNING);

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
}
