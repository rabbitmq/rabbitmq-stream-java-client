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
import static com.rabbitmq.stream.impl.LoadBalancerClusterTest.LOAD_BALANCER_ADDRESS;
import static com.rabbitmq.stream.impl.TestUtils.newLoggerLevel;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;
import static com.rabbitmq.stream.impl.Tuples.pair;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import com.rabbitmq.stream.impl.Tuples.Pair;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestUtils.DisabledIfNotCluster
@StreamTestInfrastructure
public class RecoveryClusterTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryClusterTest.class);

  private static final Duration ASSERTION_TIMEOUT = Duration.ofSeconds(20);
  // give some slack before first recovery attempt, especially on Docker
  static final Duration RECOVERY_INITIAL_DELAY = Duration.ofSeconds(10);
  static final Duration RECOVERY_DELAY = Duration.ofSeconds(2);
  static List<String> nodes;
  static final List<String> URIS =
      range(5552, 5555).mapToObj(p -> "rabbitmq-stream://localhost:" + p).collect(toList());
  static final BackOffDelayPolicy BACK_OFF_DELAY_POLICY =
      BackOffDelayPolicy.fixedWithInitialDelay(RECOVERY_INITIAL_DELAY, RECOVERY_DELAY);
  Environment environment;
  TestInfo testInfo;
  EventLoopGroup eventLoopGroup;
  EnvironmentBuilder environmentBuilder;
  static List<Level> logLevels;
  static List<Class<?>> logClasses =
      List.of(
          //          ProducersCoordinator.class,
          //          ConsumersCoordinator.class,
          AsyncRetry.class, StreamEnvironment.class, ScheduledExecutorServiceWrapper.class);
  ScheduledExecutorService scheduledExecutorService;

  @BeforeAll
  static void initAll() {
    nodes = Cli.nodes();
    logLevels = logClasses.stream().map(c -> newLoggerLevel(c, Level.DEBUG)).collect(toList());
  }

  @BeforeEach
  void init(TestInfo info) {
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    LOGGER.info("Available processors: {}", availableProcessors);
    ThreadFactory threadFactory = threadFactory("rabbitmq-stream-environment-scheduler-");
    scheduledExecutorService = Executors.newScheduledThreadPool(availableProcessors, threadFactory);
    // add some debug log messages
    scheduledExecutorService = new ScheduledExecutorServiceWrapper(scheduledExecutorService);
    environmentBuilder =
        Environment.builder()
            .recoveryBackOffDelayPolicy(BACK_OFF_DELAY_POLICY)
            .topologyUpdateBackOffDelayPolicy(BACK_OFF_DELAY_POLICY)
            .scheduledExecutorService(scheduledExecutorService)
            .requestedHeartbeat(Duration.ofSeconds(3))
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder();
    this.testInfo = info;
  }

  @AfterEach
  void tearDown() {
    if (environment != null) {
      environment.close();
    }
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
  }

  @AfterAll
  static void tearDownAll() {
    if (logLevels != null) {
      Streams.zip(logClasses.stream(), logLevels.stream(), Tuples::pair)
          .forEach(t -> newLoggerLevel(t.v1(), t.v2()));
    }
  }

  @ParameterizedTest
  @CsvSource({
    "false,false",
    "true,true",
    "true,false",
  })
  void clusterRestart(boolean useLoadBalancer, boolean forceLeader) throws InterruptedException {
    LOGGER.info(
        "Cluster restart test, use load balancer {}, force leader {}",
        useLoadBalancer,
        forceLeader);
    int streamCount = 10;
    int producerCount = streamCount * 2;
    int consumerCount = streamCount * 2;

    if (useLoadBalancer) {
      environmentBuilder
          .host(LOAD_BALANCER_ADDRESS.host())
          .port(LOAD_BALANCER_ADDRESS.port())
          .addressResolver(addr -> LOAD_BALANCER_ADDRESS)
          .forceLeaderForProducers(forceLeader)
          .locatorConnectionCount(URIS.size());
      Duration nodeRetryDelay = Duration.ofMillis(100);
      // to make the test faster
      ((StreamEnvironmentBuilder) environmentBuilder).producerNodeRetryDelay(nodeRetryDelay);
      ((StreamEnvironmentBuilder) environmentBuilder).consumerNodeRetryDelay(nodeRetryDelay);
    } else {
      environmentBuilder.uris(URIS);
    }

    environment =
        environmentBuilder
            .netty()
            .bootstrapCustomizer(
                b -> {
                  b.option(
                      ChannelOption.CONNECT_TIMEOUT_MILLIS,
                      (int) BACK_OFF_DELAY_POLICY.delay(0).toMillis());
                })
            .environmentBuilder()
            .maxProducersByConnection(producerCount / 4)
            .maxConsumersByConnection(consumerCount / 4)
            .build();
    List<String> streams =
        range(0, streamCount)
            .mapToObj(i -> TestUtils.streamName(testInfo) + "-" + i)
            .collect(toList());
    streams.forEach(s -> environment.streamCreator().stream(s).create());
    List<ProducerState> producers = Collections.emptyList();
    List<ConsumerState> consumers = Collections.emptyList();
    try {
      producers =
          range(0, producerCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(i % streams.size());
                    boolean dynamicBatch = i % 2 == 0;
                    return new ProducerState(s, dynamicBatch, environment);
                  })
              .collect(toList());
      consumers =
          range(0, consumerCount)
              .mapToObj(
                  i -> {
                    String s = streams.get(i % streams.size());
                    return new ConsumerState(s, environment);
                  })
              .collect(toList());

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

      Thread.sleep(BACK_OFF_DELAY_POLICY.delay(0).toMillis());

      List<Pair<String, Sync>> streamsSyncs =
          producers.stream()
              .map(p -> pair(p.stream(), p.waitForNewMessages(1000)))
              .collect(toList());
      streamsSyncs.forEach(
          p -> {
            LOGGER.info("Checking publisher to {} still publishes", p.v1());
            assertThat(p.v2()).completes(ASSERTION_TIMEOUT);
            LOGGER.info("Publisher to {} still publishes", p.v1());
          });

      streamsSyncs =
          consumers.stream()
              .map(c -> pair(c.stream(), c.waitForNewMessages(1000)))
              .collect(toList());
      streamsSyncs.forEach(
          p -> {
            LOGGER.info("Checking consumer from {} still consumes", p.v1());
            assertThat(p.v2()).completes(ASSERTION_TIMEOUT);
            LOGGER.info("Consumer from {} still consumes", p.v1());
          });

      Map<String, Long> committedChunkIdPerStream = new LinkedHashMap<>(streamCount);
      streams.forEach(
          s ->
              committedChunkIdPerStream.put(s, environment.queryStreamStats(s).committedChunkId()));

      syncs = producers.stream().map(p -> p.waitForNewMessages(1000)).collect(toList());
      syncs.forEach(s -> assertThat(s).completes(ASSERTION_TIMEOUT));

      streams.forEach(
          s -> {
            assertThat(environment.queryStreamStats(s).committedChunkId())
                .as("Committed chunk ID did not increase")
                .isGreaterThan(committedChunkIdPerStream.get(s));
          });

    } finally {
      LOGGER.info("Environment information:");
      System.out.println(TestUtils.jsonPrettyPrint(environment.toString()));

      LOGGER.info("Closing producers");
      producers.forEach(
          p -> {
            try {
              p.close();
            } catch (Exception e) {
              LOGGER.info("Error while closing producer to '{}': {}", p.stream(), e.getMessage());
            }
          });

      LOGGER.info("Stream status...");
      streams.forEach(s -> System.out.println(Cli.streamStatus(s)));

      consumers.forEach(
          c -> {
            try {
              c.close();
            } catch (Exception e) {
              LOGGER.info("Error while closing from '{}': {}", c.stream(), e.getMessage());
            }
          });

      LOGGER.info("Deleting streams after test");
      try {
        streams.forEach(s -> environment.deleteStream(s));
      } catch (Exception e) {
        LOGGER.info("Error while deleting streams: {}", e.getMessage());
      }
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

    private ProducerState(String stream, boolean dynamicBatch, Environment environment) {
      this.stream = stream;
      this.producer =
          environment.producerBuilder().stream(stream).dynamicBatch(dynamicBatch).build();
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

    String stream() {
      return this.stream;
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

    String stream() {
      return this.stream;
    }

    @Override
    public void close() {
      this.consumer.close();
    }
  }
}
