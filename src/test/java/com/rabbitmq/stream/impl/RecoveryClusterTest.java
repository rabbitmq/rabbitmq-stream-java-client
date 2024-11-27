// Copyright (c) 2024 Broadcom. All Rights Reserved.
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
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toList;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestUtils.DisabledIfNotCluster
@StreamTestInfrastructure
public class RecoveryClusterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryClusterTest.class);

  static List<String> nodes;
  static final List<String> URIS =
      IntStream.range(5552, 5555)
          .mapToObj(p -> "rabbitmq-stream://localhost:" + p)
          .collect(toList());
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY = BackOffDelayPolicy.fixed(ofSeconds(3));
  Environment environment;
  TestInfo testInfo;

  @BeforeAll
  static void initAll() {
    nodes = Cli.nodes();
  }

  @BeforeEach
  void init(TestInfo info) {
    environment =
        Environment.builder()
            .uris(URIS)
            .recoveryBackOffDelayPolicy(BACK_OFF_DELAY_POLICY)
            .topologyUpdateBackOffDelayPolicy(BACK_OFF_DELAY_POLICY)
            .build();
    this.testInfo = info;
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @Test
  void clusterRestart() {
    int streamCount = 10;
    List<String> streams =
        IntStream.range(0, streamCount)
            .mapToObj(i -> TestUtils.streamName(testInfo) + "-" + i)
            .collect(toList());
    streams.forEach(s -> environment.streamCreator().stream(s).create());
    try {
      List<ProducerState> producers =
          streams.stream().map(s -> new ProducerState(s, environment)).collect(toList());
      List<ConsumerState> consumers =
          streams.stream().map(s -> new ConsumerState(s, environment)).collect(toList());

      producers.forEach(ProducerState::start);

      List<Sync> syncs = producers.stream().map(p -> p.waitForNewMessages(100)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());

      syncs = consumers.stream().map(c -> c.waitForNewMessages(100)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());

      nodes.forEach(
          n -> {
            LOGGER.info("Restarting node {}...", n);
            Cli.restartNode(n);
            LOGGER.info("Restarted node {}.", n);
          });
      LOGGER.info("Rebalancing...");
      Cli.rebalance();
      LOGGER.info("Rebalancing over.");

      syncs = producers.stream().map(p -> p.waitForNewMessages(100)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());

      syncs = consumers.stream().map(c -> c.waitForNewMessages(100)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes());

      producers.forEach(ProducerState::close);
      consumers.forEach(ConsumerState::close);
    } finally {
      streams.forEach(s -> environment.deleteStream(s));
    }
  }

  private static class ProducerState implements AutoCloseable {

    private static final byte[] BODY = "hello".getBytes(StandardCharsets.UTF_8);

    private final String stream;
    private final Producer producer;
    final RateLimiter limiter = RateLimiter.create(1000);
    Thread task;
    final AtomicBoolean stopped = new AtomicBoolean(false);
    final AtomicInteger acceptedCount = new AtomicInteger();
    final AtomicReference<Runnable> postConfirmed = new AtomicReference<>(() -> {});

    private ProducerState(String stream, Environment environment) {
      this.stream = stream;
      this.producer = environment.producerBuilder().stream(stream).build();
    }

    void start() {
      ConfirmationHandler confirmationHandler =
          confirmationStatus -> {
            if (confirmationStatus.isConfirmed()) {
              acceptedCount.incrementAndGet();
              postConfirmed.get().run();
            }
          };
      task =
          Executors.defaultThreadFactory()
              .newThread(
                  () -> {
                    while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
                      try {
                        this.limiter.acquire(1);
                        this.producer.send(
                            producer.messageBuilder().addData(BODY).build(), confirmationHandler);
                      } catch (Exception e) {

                      }
                    }
                  });
      task.start();
    }

    Sync waitForNewMessages(int messageCount) {
      Sync sync = sync(messageCount);
      AtomicInteger count = new AtomicInteger();
      this.postConfirmed.set(
          () -> {
            if (count.incrementAndGet() == messageCount) {
              this.postConfirmed.set(() -> {});
            }
            sync.down();
          });
      return sync;
    }

    @Override
    public void close() {
      stopped.set(true);
      task.interrupt();
      producer.close();
    }
  }

  private static class ConsumerState implements AutoCloseable {

    private final String stream;
    private final Consumer consumer;
    final AtomicInteger receivedCount = new AtomicInteger();
    final AtomicReference<Runnable> postHandle = new AtomicReference<>(() -> {});

    private ConsumerState(String stream, Environment environment) {
      this.stream = stream;
      this.consumer =
          environment.consumerBuilder().stream(stream)
              .offset(OffsetSpecification.first())
              .messageHandler(
                  (ctx, m) -> {
                    receivedCount.incrementAndGet();
                    postHandle.get().run();
                  })
              .build();
    }

    Sync waitForNewMessages(int messageCount) {
      Sync sync = sync(messageCount);
      AtomicInteger count = new AtomicInteger();
      this.postHandle.set(
          () -> {
            if (count.incrementAndGet() == messageCount) {
              this.postHandle.set(() -> {});
            }
            sync.down();
          });
      return sync;
    }

    @Override
    public void close() {
      this.consumer.close();
    }
  }
}
