// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT;
import static com.rabbitmq.stream.impl.ProducersCoordinator.recoverable;
import static com.rabbitmq.stream.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.stream.impl.TestUtils.answer;
import static com.rabbitmq.stream.impl.TestUtils.metadata;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.CoordinatorUtils.ClientClosedException;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import io.netty.channel.ConnectTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class ProducersCoordinatorTest {

  @Mock StreamEnvironment environment;
  @Mock Client locator;
  @Mock StreamProducer producer;
  @Mock StreamConsumer trackingConsumer;
  @Mock ClientFactory clientFactory;
  @Mock Client client;
  AutoCloseable mocks;
  ProducersCoordinator coordinator;
  ScheduledExecutorService scheduledExecutorService;

  volatile Client.ShutdownListener shutdownListener;
  volatile Client.MetadataListener metadataListener;

  static Duration ms(long ms) {
    return Duration.ofMillis(ms);
  }

  static Client.Broker leader() {
    return new Client.Broker("leader", 5552);
  }

  static Utils.BrokerWrapper leaderWrapper() {
    return new Utils.BrokerWrapper(leader(), true);
  }

  static Client.Broker leader1() {
    return new Client.Broker("leader-1", 5552);
  }

  static Client.Broker leader2() {
    return new Client.Broker("leader-2", 5552);
  }

  static List<Client.Broker> replicas() {
    return Arrays.asList(new Client.Broker("replica1", 5552), new Client.Broker("replica2", 5552));
  }

  static List<Utils.BrokerWrapper> replicaWrappers() {
    return replicas().stream().map(b -> new Utils.BrokerWrapper(b, false)).collect(toList());
  }

  @BeforeEach
  void init() {
    Client.ClientParameters clientParameters =
        new Client.ClientParameters() {
          @Override
          public Client.ClientParameters shutdownListener(
              Client.ShutdownListener shutdownListener) {
            ProducersCoordinatorTest.this.shutdownListener = shutdownListener;
            return super.shutdownListener(shutdownListener);
          }

          @Override
          public Client.ClientParameters metadataListener(
              Client.MetadataListener metadataListener) {
            ProducersCoordinatorTest.this.metadataListener = metadataListener;
            return super.metadataListener(metadataListener);
          }
        };
    mocks = MockitoAnnotations.openMocks(this);
    StreamEnvironment.Locator l = new StreamEnvironment.Locator(-1, new Address("localhost", 5555));
    l.client(locator);
    when(environment.locator()).thenReturn(l);
    when(environment.locatorOperation(any())).thenCallRealMethod();
    when(environment.clientParametersCopy()).thenReturn(clientParameters);
    when(environment.addressResolver()).thenReturn(address -> address);
    when(trackingConsumer.stream()).thenReturn("stream");
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK));
    when(client.serverAdvertisedHost()).thenReturn(leader().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader().getPort());
    when(environment.rpcTimeout()).thenReturn(Duration.ofSeconds(10));
    // a bare executor, not createScheduledExecutorService(): it must not start any thread of its
    // own just by existing, only if actually given a task, so tests that override this stub before
    // registering leave nothing running to clean up
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    coordinator =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            clientFactory,
            true,
            null);
    when(client.isOpen()).thenReturn(true);
    when(client.deletePublisher(anyByte())).thenReturn(new Response(Constants.RESPONSE_CODE_OK));
  }

  @AfterEach
  void tearDown() throws Exception {
    // just taking the opportunity to check toString() generates valid JSON
    MonitoringTestUtils.extract(coordinator);
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    mocks.close();
    coordinator.close();
  }

  @Test
  void registerShouldThrowExceptionWhenNoMetadataForTheStream() {
    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenStreamDoesNotExist() {
    when(locator.metadata("stream"))
        .thenReturn(metadata("stream", null, null, Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenMetadataResponseIsNotOk() {
    when(locator.metadata("stream")).thenReturn(metadata(null, null));
    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenNoLeader() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));
    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registerShouldAllowPublishing() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    Runnable cleanTask = coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).assign(anyByte(), eq(client));

    cleanTask.run();
  }

  @Test
  void initialRegistrationShouldNotMarkTheAgentRunning() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    when(producer.isOpen()).thenReturn(true);
    when(trackingConsumer.isOpen()).thenReturn(true);

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);

    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    // registration runs inside the agent's constructor, running() would touch unset state
    verify(producer, after(500).never()).running();
    verify(trackingConsumer, never()).running();
  }

  @Test
  void
      shouldRetryUntilGettingExactNodeWithAdvertisedHostNameClientFactoryAndNotExactNodeOnFirstTime() {
    ClientFactory cf =
        context ->
            Utils.connectToAdvertisedNodeClientFactory(clientFactory, Duration.ofMillis(1))
                .client(context);
    ProducersCoordinator c =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            cf,
            true,
            null);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    when(client.serverAdvertisedHost()).thenReturn("foo").thenReturn(leader().getHost());
    when(client.serverAdvertisedPort()).thenReturn(42).thenReturn(leader().getPort());

    try {
      Runnable cleanTask = c.registerProducer(producer, null, "stream");

      verify(clientFactory, times(2)).client(any());
      verify(producer, times(1)).assign(anyByte(), eq(client));

      cleanTask.run();
    } finally {
      c.close();
    }
  }

  @Test
  void shouldGetExactNodeImmediatelyWithAdvertisedHostNameClientFactoryAndExactNodeOnFirstTime() {
    ClientFactory cf =
        context ->
            Utils.connectToAdvertisedNodeClientFactory(clientFactory, Duration.ofMillis(1))
                .client(context);
    ProducersCoordinator c =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            cf,
            true,
            null);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    when(client.serverAdvertisedHost()).thenReturn(leader().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader().getPort());

    try {
      Runnable cleanTask = c.registerProducer(producer, null, "stream");

      verify(clientFactory, times(1)).client(any());
      verify(producer, times(1)).assign(anyByte(), eq(client));

      cleanTask.run();
    } finally {
      c.close();
    }
  }

  @Test
  void shouldRedistributeProducerAndTrackingConsumerIfConnectionIsLost() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    when(producer.isOpen()).thenReturn(true);
    when(trackingConsumer.isOpen()).thenReturn(true);

    StreamProducer producerClosedAfterDisconnection = mock(StreamProducer.class);
    when(producerClosedAfterDisconnection.isOpen()).thenReturn(false);

    CountDownLatch assignLatch = new CountDownLatch(2 + 2 + 1);
    doAnswer(answer(() -> assignLatch.countDown())).when(producer).assign(anyByte(), eq(client));
    doAnswer(answer(() -> assignLatch.countDown()))
        .when(trackingConsumer)
        .setTrackingClient(client);
    doAnswer(answer(() -> assignLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .assign(anyByte(), eq(client));

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(producer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(trackingConsumer).running();
    doAnswer(answer(() -> runningLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .running();

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);
    coordinator.registerProducer(producerClosedAfterDisconnection, null, "stream");

    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(producerClosedAfterDisconnection, times(1)).assign(anyByte(), eq(client));
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(assignLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(2)).assign(anyByte(), eq(client));
    verify(producer, times(1)).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(2)).setTrackingClient(client);
    verify(trackingConsumer, times(1)).running();
    verify(producerClosedAfterDisconnection, times(1)).unavailable();
    verify(producerClosedAfterDisconnection, times(1)).assign(anyByte(), eq(client));
    verify(producerClosedAfterDisconnection, never()).running();
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);
  }

  @Test
  void firstRecoveryAttemptShouldWaitThePolicyInitialDelay() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    // an initial delay much longer than the delay between retries, so which of the two applies to
    // the first attempt of a recovery episode is observable
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(1000), ms(10)));
    when(producer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    // still only the initial declaration: the recovery attempt is waiting out delay(0), the
    // policy's grace before reacting at all, and not delay(1)
    verify(client, after(300).times(1)).declarePublisher(anyByte(), isNull(), anyString());
    verify(client, timeout(10_000).times(2)).declarePublisher(anyByte(), isNull(), anyString());
  }

  @Test
  void producerClosedDuringItsRecoveryAssignmentShouldBeReleased() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    // open when the attempt starts, closed by the time the assignment is done
    when(producer.isOpen()).thenReturn(true, false);

    coordinator.registerProducer(producer, null, "stream");
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    verify(client, timeout(5_000).times(2)).declarePublisher(anyByte(), isNull(), anyString());
    verify(client, timeout(5_000)).deletePublisher(anyByte());
    // the new connection had the closed producer only, so it goes away once it is released
    waitAtMost(() -> coordinator.clientCount() == 0);
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();
  }

  @Test
  void producerClosedRightAfterItsRecoveryAssignmentShouldBeReleased() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    AtomicReference<Runnable> cleanTask = new AtomicReference<>();
    AtomicInteger isOpenCalls = new AtomicInteger();
    when(producer.isOpen())
        .then(
            invocation -> {
              if (isOpenCalls.incrementAndGet() == 2) {
                // closed right after the attempt checked it is still open
                cleanTask.get().run();
              }
              return true;
            });

    cleanTask.set(coordinator.registerProducer(producer, null, "stream"));
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    verify(client, timeout(5_000).times(2)).declarePublisher(anyByte(), isNull(), anyString());
    verify(client, timeout(5_000)).deletePublisher(anyByte());
    waitAtMost(() -> coordinator.clientCount() == 0);
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();
  }

  @Test
  void aSupersededAttemptShouldNotTouchTheBroker() {
    scheduledExecutorService = spy(createScheduledExecutorService(2));
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    // long enough that the parked attempt is still waiting when it gets superseded
    Duration parkedDelay = Duration.ofSeconds(2);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(50), parkedDelay));
    when(producer.isOpen()).thenReturn(true);
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenThrow(new IllegalStateException("no metadata for this attempt"))
        .thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    // the episode's first attempt fails its candidate lookup and parks
    verifyRetryScheduled(parkedDelay, 1);

    // the watchdog starts a fresh attempt, which succeeds and leaves the parked one stale
    coordinator.ageWatchdogClocksBy(parkedDelay.plusSeconds(121));
    coordinator.watchdogTick();
    verify(client, timeout(10_000).times(2)).declarePublisher(anyByte(), isNull(), anyString());

    // the parked attempt fires once its delay is up: it must not look up a candidate, and above
    // all must not declare a publisher, which would take the producer away from the fresh attempt
    verify(locator, after(parkedDelay.toMillis() + 500).times(3)).metadata("stream");
    verify(client, times(2)).declarePublisher(anyByte(), isNull(), anyString());
    verify(producer, times(1)).running();
  }

  @Test
  void shouldNotStrandAnAgentWhenAnAttemptGivesUp() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(producer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    // the first recovery ends in an error once the producer is reassigned
    doThrow(new IllegalStateException("running() failure")).doNothing().when(producer).running();

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    verify(producer, timeout(10_000).times(1)).running();

    // a disruption on the connection the producer recovered to
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    verify(producer, timeout(10_000).times(2)).running();
    verify(producer, times(3)).assign(anyByte(), eq(client));
  }

  @Test
  void shouldNotOpenTwoConnectionsToTheSameNodeForConcurrentRecoveries() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream-1")).thenReturn(metadata("stream-1", leader(), replicas()));
    when(locator.metadata("stream-2")).thenReturn(metadata("stream-2", leader(), replicas()));
    when(clientFactory.client(any()))
        .thenReturn(client)
        .thenAnswer(
            invocation -> {
              // slow enough for both recoveries to need a connection at the same time
              Thread.sleep(500);
              return client;
            });
    StreamProducer producer2 = mock(StreamProducer.class);
    when(producer.isOpen()).thenReturn(true);
    when(producer2.isOpen()).thenReturn(true);

    coordinator.registerProducer(producer, null, "stream-1");
    coordinator.registerProducer(producer2, null, "stream-2");
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    verify(producer, timeout(10_000).times(1)).running();
    verify(producer2, timeout(10_000).times(1)).running();
    verify(clientFactory, times(2)).client(any());
    assertThat(coordinator.clientCount()).isEqualTo(1);
  }

  @Test
  void aCancelledAgentDuringRecoveryShouldNotBeReassigned() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(500), ms(50)));
    AtomicBoolean open = new AtomicBoolean(true);
    when(producer.isOpen()).thenAnswer(invocation -> open.get());
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    // closed while its recovery waits out the initial delay
    open.set(false);

    verify(client, after(1000).times(1)).declarePublisher(anyByte(), isNull(), anyString());
    verify(locator, times(1)).metadata("stream");
    verify(clientFactory, times(1)).client(any());
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();
  }

  @Test
  void watchdogShouldReDispatchAStuckAgent() {
    scheduledExecutorService = spy(createScheduledExecutorService(2));
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    // a long fixed delay: the natural retry must not fire on its own during this test, so any
    // further progress can only come from the watchdog
    Duration delay = Duration.ofMinutes(10);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(delay));
    when(producer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK))
        // the recovery attempt fails and its retry is scheduled 10 minutes out
        .thenReturn(new Response(Constants.RESPONSE_CODE_PRECONDITION_FAILED))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK));

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    // the episode's own first attempt is scheduled 10 minutes out, so bring that forward too
    coordinator.ageWatchdogClocksBy(delay.plusSeconds(121));
    coordinator.watchdogTick();
    verify(client, timeout(10_000).times(2)).declarePublisher(anyByte(), isNull(), anyString());
    verifyRetryScheduled(delay, 2);

    coordinator.ageWatchdogClocksBy(delay.plusSeconds(121));
    coordinator.watchdogTick();
    verify(client, timeout(10_000).times(3)).declarePublisher(anyByte(), isNull(), anyString());
    verify(producer, timeout(10_000).times(1)).running();
  }

  @Test
  void watchdogShouldNotCutShortABackOffDelay() {
    scheduledExecutorService = spy(createScheduledExecutorService(2));
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    // longer than the watchdog's stuck threshold, so the two could conflict
    Duration delay = Duration.ofMinutes(10);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(delay));
    when(producer.isOpen()).thenReturn(true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK))
        .thenReturn(new Response(Constants.RESPONSE_CODE_PRECONDITION_FAILED))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK));

    coordinator.registerProducer(producer, null, "stream");

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    // bring the episode's first attempt forward, to get to an attempt that failed and is now
    // waiting on its retry
    coordinator.ageWatchdogClocksBy(delay.plusSeconds(121));
    coordinator.watchdogTick();
    verifyRetryScheduled(delay, 2);

    // past the stuck threshold, but nowhere near the end of the delay: waiting by design, not stuck
    coordinator.ageWatchdogClocksBy(Duration.ofSeconds(121));
    coordinator.watchdogTick();

    verify(client, after(300).times(2)).declarePublisher(anyByte(), isNull(), anyString());
  }

  @Test
  void staleAttemptLandingLastShouldNotLeaveProducerOnADeletedPublisher() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(50), Duration.ofMinutes(10)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    Client client2 = mockClient(new AtomicBoolean(true));
    CountDownLatch staleDeclareStarted = new CountDownLatch(1);
    CountDownLatch releaseStaleDeclare = new CountDownLatch(1);
    AtomicBoolean firstDeclare = new AtomicBoolean(true);
    when(client2.declarePublisher(anyByte(), isNull(), anyString()))
        .then(
            invocation -> {
              if (firstDeclare.getAndSet(false)) {
                staleDeclareStarted.countDown();
                releaseStaleDeclare.await(10, TimeUnit.SECONDS);
              }
              return new Response(Constants.RESPONSE_CODE_OK);
            });
    when(clientFactory.client(any())).thenReturn(client, client2);

    coordinator.registerProducer(producer, null, "stream");
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    assertThat(staleDeclareStarted.await(10, TimeUnit.SECONDS)).isTrue();

    // the watchdog supersedes the attempt held in its declaration, the new attempt completes
    coordinator.ageWatchdogClocksBy(Duration.ofMinutes(10).plusSeconds(121));
    coordinator.watchdogTick();
    verify(producer, timeout(10_000)).running();

    releaseStaleDeclare.countDown();

    ArgumentCaptor<Byte> declaredIds = ArgumentCaptor.forClass(Byte.class);
    verify(client2, times(2)).declarePublisher(declaredIds.capture(), isNull(), anyString());
    byte staleId = declaredIds.getAllValues().get(0);
    byte currentId = declaredIds.getAllValues().get(1);
    verify(client2, timeout(10_000)).deletePublisher(staleId);
    verify(client2, after(300).never()).deletePublisher(currentId);
    ArgumentCaptor<Byte> assignedIds = ArgumentCaptor.forClass(Byte.class);
    ArgumentCaptor<Client> assignedClients = ArgumentCaptor.forClass(Client.class);
    verify(producer, atLeastOnce()).assign(assignedIds.capture(), assignedClients.capture());
    assertThat(assignedIds.getAllValues()).last().isEqualTo(currentId);
    assertThat(assignedClients.getAllValues()).last().isSameAs(client2);
    verify(producer, times(1)).running();
  }

  @Test
  void disruptionDuringMarkOpenShouldLeaveProducerUnavailable() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    AtomicBoolean client2Open = new AtomicBoolean(true);
    Client client2 = mockClient(client2Open);
    Client client3 = mockClient(new AtomicBoolean(true));
    when(clientFactory.client(any())).thenReturn(client, client2, client3);
    List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    doAnswer(answer(() -> timeline.add("unavailable"))).when(producer).unavailable();
    AtomicBoolean firstRunning = new AtomicBoolean(true);
    doAnswer(
            answer(
                () -> {
                  timeline.add("running start");
                  if (firstRunning.getAndSet(false)) {
                    // the connection dies while running() republishes, before it sets OPEN
                    client2Open.set(false);
                    shutdownListener.handle(
                        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
                  }
                  timeline.add("running end");
                }))
        .when(producer)
        .running();

    coordinator.registerProducer(producer, null, "stream");
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    verify(producer, timeout(10_000).times(2)).running();
    InOrder inOrder = inOrder(producer);
    inOrder.verify(producer).assign(anyByte(), eq(client2));
    inOrder.verify(producer).running();
    inOrder.verify(producer).assign(anyByte(), eq(client3));
    inOrder.verify(producer).running();
    // the first reopening ends with the producer flipped back, since its OPEN may have overwritten
    // the flip of the disruption
    assertThat(timeline)
        .containsExactly(
            "unavailable",
            "running start",
            "unavailable",
            "running end",
            "unavailable",
            "running start",
            "running end");
  }

  @Test
  void olderMarkOpenShouldNotOverrideANewerAssignment() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    AtomicBoolean client2Open = new AtomicBoolean(true);
    Client client2 = mockClient(client2Open);
    Client client3 = mockClient(new AtomicBoolean(true));
    when(clientFactory.client(any())).thenReturn(client, client2, client3);
    List<String> timeline = Collections.synchronizedList(new ArrayList<>());
    doAnswer(answer(() -> timeline.add("unavailable"))).when(producer).unavailable();
    CountDownLatch olderRunningStarted = new CountDownLatch(1);
    CountDownLatch releaseOlderRunning = new CountDownLatch(1);
    AtomicBoolean firstRunning = new AtomicBoolean(true);
    doAnswer(
            invocation -> {
              timeline.add("running start");
              if (firstRunning.getAndSet(false)) {
                olderRunningStarted.countDown();
                releaseOlderRunning.await(10, TimeUnit.SECONDS);
              }
              timeline.add("running end");
              return null;
            })
        .when(producer)
        .running();

    coordinator.registerProducer(producer, null, "stream");
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    assertThat(olderRunningStarted.await(10, TimeUnit.SECONDS)).isTrue();

    // the connection dies while the older reopening is held, the producer gets a new assignment
    client2Open.set(false);
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
    verify(client3, timeout(10_000)).declarePublisher(anyByte(), isNull(), anyString());
    // the newer reopening waits for the older one
    verify(producer, after(300).times(1)).running();

    releaseOlderRunning.countDown();

    verify(producer, timeout(10_000).times(2)).running();
    ArgumentCaptor<Client> assignedClients = ArgumentCaptor.forClass(Client.class);
    verify(producer, times(3)).assign(anyByte(), assignedClients.capture());
    assertThat(assignedClients.getAllValues()).containsExactly(client, client2, client3);
    waitAtMost(() -> timeline.size() == 7);
    assertThat(timeline)
        .containsExactly(
            "unavailable",
            "running start",
            "unavailable",
            "running end",
            "unavailable",
            "running start",
            "running end");
  }

  @Test
  void producerShouldRecoverAgainIfConnectionDiesRightAfterDeclarePublisher() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    AtomicBoolean client2Open = new AtomicBoolean(true);
    Client client2 = mockClient(client2Open);
    Client client3 = mockClient(new AtomicBoolean(true));
    when(client2.declarePublisher(anyByte(), isNull(), anyString()))
        .then(
            invocation -> {
              // the broker accepted the publisher, but the connection dies before the response
              // makes it back to the attempt
              client2Open.set(false);
              shutdownListener.handle(
                  new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
              return new Response(Constants.RESPONSE_CODE_OK);
            });
    when(clientFactory.client(any())).thenReturn(client, client2, client3);

    coordinator.registerProducer(producer, null, "stream");
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    verify(client3, timeout(10_000)).declarePublisher(anyByte(), isNull(), anyString());
    verify(producer, timeout(10_000)).running();
    InOrder inOrder = inOrder(producer);
    inOrder.verify(producer).assign(anyByte(), eq(client3));
    inOrder.verify(producer).running();
    verify(producer, after(300).times(1)).running();
  }

  @Test
  void producerRegistrationShouldFailIfConnectionDiesRightAfterDeclarePublisher() throws Exception {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    AtomicBoolean clientOpen = new AtomicBoolean(true);
    Client client = mockClient(clientOpen);
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .then(
            invocation -> {
              clientOpen.set(false);
              shutdownListener.handle(
                  new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
              return new Response(Constants.RESPONSE_CODE_OK);
            });
    when(clientFactory.client(any())).thenReturn(client);

    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(ClientClosedException.class);
    verify(producer, after(300).never()).running();
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void producerShouldRecoverAgainIfStreamBecomesUnavailableDuringDeclarePublisher() {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(ms(50)));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    Client client2 = mockClient(new AtomicBoolean(true));
    AtomicBoolean firstDeclare = new AtomicBoolean(true);
    when(client2.declarePublisher(anyByte(), isNull(), anyString()))
        .then(
            invocation -> {
              if (firstDeclare.getAndSet(false)) {
                // the broker accepted the publisher, then dropped it with the stream
                metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);
              }
              return new Response(Constants.RESPONSE_CODE_OK);
            });
    when(clientFactory.client(any())).thenReturn(client, client2);

    coordinator.registerProducer(producer, null, "stream");
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    ArgumentCaptor<Byte> publisherIds = ArgumentCaptor.forClass(Byte.class);
    verify(client2, timeout(10_000).times(2))
        .declarePublisher(publisherIds.capture(), isNull(), anyString());
    verify(client2, timeout(10_000)).deletePublisher(publisherIds.getAllValues().get(0));
    verify(producer, timeout(10_000)).running();
    verify(producer, after(300).times(1)).running();
    verify(client2, times(1)).deletePublisher(anyByte());
  }

  @Test
  void producerRegistrationShouldFailIfStreamBecomesUnavailableDuringDeclarePublisher() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(producer.isOpen()).thenReturn(true);
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .then(
            invocation -> {
              metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);
              return new Response(Constants.RESPONSE_CODE_OK);
            });
    when(clientFactory.client(any())).thenReturn(client);

    assertThatThrownBy(() -> coordinator.registerProducer(producer, null, "stream"))
        .isInstanceOf(StreamNotAvailableException.class);
    verify(client, timeout(10_000)).deletePublisher(anyByte());
    verify(producer, never()).running();
  }

  @Test
  void trackingConsumerRegistrationShouldFailIfConnectionDiesRightAfterAssignment()
      throws Exception {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(trackingConsumer.isOpen()).thenReturn(true);
    AtomicBoolean clientOpen = new AtomicBoolean(true);
    Client client = mockClient(clientOpen);
    doAnswer(
            answer(
                () -> {
                  clientOpen.set(false);
                  shutdownListener.handle(
                      new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));
                }))
        .when(trackingConsumer)
        .setTrackingClient(client);
    when(clientFactory.client(any())).thenReturn(client);

    assertThatThrownBy(() -> coordinator.registerTrackingConsumer(trackingConsumer))
        .isInstanceOf(ClientClosedException.class);
    verify(trackingConsumer, after(300).never()).running();
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void trackingConsumerRegistrationShouldFailIfStreamBecomesUnavailableRightAfterAssignment()
      throws Exception {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(trackingConsumer.isOpen()).thenReturn(true);
    doAnswer(
            answer(
                () ->
                    metadataListener.handle(
                        "stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE)))
        .when(trackingConsumer)
        .setTrackingClient(client);
    when(clientFactory.client(any())).thenReturn(client);

    assertThatThrownBy(() -> coordinator.registerTrackingConsumer(trackingConsumer))
        .isInstanceOf(StreamNotAvailableException.class);
    verify(trackingConsumer, after(300).never()).running();
    // the tracking consumer was the connection's only agent
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void releasingAProducerShouldDeleteItsPublisher() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);
    ArgumentCaptor<Byte> publisherId = ArgumentCaptor.forClass(Byte.class);

    Runnable cleanTask = coordinator.registerProducer(producer, null, "stream");
    Runnable trackingConsumerCleanTask = coordinator.registerTrackingConsumer(trackingConsumer);
    verify(producer).assign(publisherId.capture(), any());

    trackingConsumerCleanTask.run();
    verify(client, never()).deletePublisher(anyByte());

    cleanTask.run();
    verify(client, times(1)).deletePublisher(publisherId.getValue());
    // a second release has nothing left to delete
    cleanTask.run();
    verify(client, times(1)).deletePublisher(anyByte());
  }

  @Test
  void shouldRecoverOnConnectionTimeout() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any()))
        .thenReturn(client)
        .thenThrow(new TimeoutStreamException("", new ConnectTimeoutException()))
        .thenReturn(client);

    when(producer.isOpen()).thenReturn(true);

    StreamProducer producer = mock(StreamProducer.class);
    when(producer.isOpen()).thenReturn(true);

    CountDownLatch runningLatch = new CountDownLatch(1);
    doAnswer(answer(runningLatch::countDown)).when(this.producer).running();

    coordinator.registerProducer(this.producer, null, "stream");

    verify(this.producer, times(1)).assign(anyByte(), eq(client));

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(this.producer, times(1)).unavailable();
    verify(this.producer, times(2)).assign(anyByte(), eq(client));
  }

  @Test
  void shouldDisposeProducerAndNotTrackingConsumerIfRecoveryTimesOut() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    // a producer is still open while it recovers
    when(producer.isOpen()).thenReturn(true);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);

    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(trackingConsumer, never()).running();
    verify(trackingConsumer, never()).closeAfterStreamDeletion();
    waitAtMost(() -> coordinator.nodesConnected() == 0);
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void shouldRedistributeProducersAndTrackingConsumersOnMetadataUpdate() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    String movingStream = "moving-stream";
    when(locator.metadata(movingStream))
        .thenReturn(metadata(movingStream, leader1(), replicas()))
        .thenReturn(metadata(movingStream, leader1(), replicas()))
        .thenReturn(metadata(movingStream, leader1(), replicas())) // for the first 3 registrations
        .thenReturn(metadata(movingStream, null, replicas()))
        .thenReturn(metadata(movingStream, leader2(), replicas()));

    // the created client is on leader1
    when(client.serverAdvertisedHost()).thenReturn(leader1().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader1().getPort());

    String fixedStream = "fixed-stream";
    when(locator.metadata(fixedStream)).thenReturn(metadata(fixedStream, leader1(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    StreamProducer movingProducer = mock(StreamProducer.class);
    StreamProducer fixedProducer = mock(StreamProducer.class);
    StreamConsumer movingTrackingConsumer = mock(StreamConsumer.class);
    StreamConsumer fixedTrackingConsumer = mock(StreamConsumer.class);
    when(movingTrackingConsumer.stream()).thenReturn(movingStream);
    when(fixedTrackingConsumer.stream()).thenReturn(fixedStream);

    StreamProducer producerClosedAfterDisconnection = mock(StreamProducer.class);
    when(producerClosedAfterDisconnection.isOpen()).thenReturn(false);

    CountDownLatch assignLatch = new CountDownLatch(2 + 2 + 1);

    when(fixedProducer.isOpen()).thenReturn(true);
    when(movingProducer.isOpen()).thenReturn(true);
    when(movingTrackingConsumer.isOpen()).thenReturn(true);
    when(fixedTrackingConsumer.isOpen()).thenReturn(true);

    doAnswer(answer(() -> assignLatch.countDown()))
        .when(movingProducer)
        .assign(anyByte(), eq(client));

    doAnswer(answer(() -> assignLatch.countDown()))
        .when(movingTrackingConsumer)
        .setTrackingClient(client);

    doAnswer(answer(() -> assignLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .assign(anyByte(), eq(client));

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(movingProducer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(movingTrackingConsumer).running();

    coordinator.registerProducer(movingProducer, null, movingStream);
    coordinator.registerProducer(fixedProducer, null, fixedStream);
    coordinator.registerProducer(producerClosedAfterDisconnection, null, movingStream);
    coordinator.registerTrackingConsumer(movingTrackingConsumer);
    coordinator.registerTrackingConsumer(fixedTrackingConsumer);

    verify(movingProducer, times(1)).assign(anyByte(), eq(client));
    verify(fixedProducer, times(1)).assign(anyByte(), eq(client));
    verify(producerClosedAfterDisconnection, times(1)).assign(anyByte(), eq(client));
    verify(movingTrackingConsumer, times(1)).setTrackingClient(client);
    verify(fixedTrackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    // the created client is on leader2
    when(client.serverAdvertisedHost()).thenReturn(leader2().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader2().getPort());

    metadataListener.handle(movingStream, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(assignLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(movingProducer, times(1)).unavailable();
    verify(movingProducer, times(2)).assign(anyByte(), eq(client));
    verify(movingProducer, times(1)).running();
    verify(movingTrackingConsumer, times(1)).unavailable();
    verify(movingTrackingConsumer, times(2)).setTrackingClient(client);
    verify(movingTrackingConsumer, times(1)).running();

    verify(producerClosedAfterDisconnection, times(1)).unavailable();
    verify(producerClosedAfterDisconnection, times(1)).assign(anyByte(), eq(client));
    verify(producerClosedAfterDisconnection, never()).running();

    verify(fixedProducer, never()).unavailable();
    verify(fixedProducer, times(1)).assign(anyByte(), eq(client));
    verify(fixedProducer, never()).running();
    verify(fixedTrackingConsumer, never()).unavailable();
    verify(fixedTrackingConsumer, times(1)).setTrackingClient(client);
    verify(fixedTrackingConsumer, never()).running();
    assertThat(coordinator.nodesConnected()).isEqualTo(2);
    assertThat(coordinator.clientCount()).isEqualTo(2);
  }

  @Test
  void shouldDisposeProducerIfNoLeaderComesBackInTime() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    // a producer is still open while it recovers
    when(producer.isOpen()).thenReturn(true);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).assign(anyByte(), eq(client));
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1))
        .closeAfterStreamDeletion(Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();

    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void producerShouldBeClosedWithStreamDoesNotExistCodeIfStreamIsDeletedDuringRecovery()
      throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata("stream", null, null, Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
    when(clientFactory.client(any())).thenReturn(client);
    when(producer.isOpen()).thenReturn(true);

    coordinator.registerProducer(producer, null, "stream");
    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    verify(producer, timeout(5000).times(1))
        .closeAfterStreamDeletion(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    verify(locator, times(2)).metadata("stream");
    verify(producer, never()).running();
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @Test
  void shouldDisposeProducerAndNotTrackingConsumerIfMetadataUpdateTimesOut() throws Exception {
    scheduledExecutorService = createScheduledExecutorService(2);
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    // a producer is still open while it recovers
    when(producer.isOpen()).thenReturn(true);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);

    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).assign(anyByte(), eq(client));
    verify(producer, never()).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(trackingConsumer, never()).running();
    verify(trackingConsumer, never()).closeAfterStreamDeletion();
    waitAtMost(() -> coordinator.nodesConnected() == 0);
    waitAtMost(() -> coordinator.clientCount() == 0);
  }

  @ParameterizedTest
  @ValueSource(ints = {50, ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT})
  void growShrinkResourcesBasedOnProducersAndTrackingConsumersCount(int maxProducersByClient)
      throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    int extraProducerCount = maxProducersByClient / 5;
    int producerCount = maxProducersByClient + extraProducerCount;

    coordinator.close();
    coordinator =
        new ProducersCoordinator(
            environment,
            maxProducersByClient,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            clientFactory,
            true,
            null);

    class ProducerInfo {
      StreamProducer producer;
      byte publishingId;
      Runnable cleaningCallback;
    }
    List<ProducerInfo> producerInfos = new ArrayList<>(producerCount);
    IntStream.range(0, producerCount)
        .forEach(
            i -> {
              StreamProducer p = mock(StreamProducer.class);
              ProducerInfo info = new ProducerInfo();
              info.producer = p;
              doAnswer(answer(invocation -> info.publishingId = invocation.getArgument(0)))
                  .when(p)
                  .assign(anyByte(), any());
              Runnable cleaningCallback = coordinator.registerProducer(p, null, "stream");
              info.cleaningCallback = cleaningCallback;
              producerInfos.add(info);
            });

    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(2);

    // let's add some tracking consumers
    int extraTrackingConsumerCount = ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT / 5;
    int trackingConsumerCount =
        ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT * 2 + extraTrackingConsumerCount;

    class TrackingConsumerInfo {
      StreamConsumer consumer;
      Runnable cleaningCallback;
    }
    List<TrackingConsumerInfo> trackingConsumerInfos = new ArrayList<>(trackingConsumerCount);
    IntStream.range(0, trackingConsumerCount)
        .forEach(
            i -> {
              StreamConsumer c = mock(StreamConsumer.class);
              when(c.stream()).thenReturn("stream");
              TrackingConsumerInfo info = new TrackingConsumerInfo();
              info.consumer = c;
              Runnable cleaningCallback = coordinator.registerTrackingConsumer(c);
              info.cleaningCallback = cleaningCallback;
              trackingConsumerInfos.add(info);
            });

    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount())
        .as("new tracking consumers needs yet another client")
        .isEqualTo(3);

    Collections.reverse(trackingConsumerInfos);
    // let's remove some tracking consumers to free 1 client
    IntStream.range(0, extraTrackingConsumerCount)
        .forEach(
            i -> {
              trackingConsumerInfos.get(0).cleaningCallback.run();
              trackingConsumerInfos.remove(0);
            });

    // the emptied connection is closed after the idle linger
    waitAtMost(() -> coordinator.clientCount() == 2);

    // let's free the rest of tracking consumers
    trackingConsumerInfos.forEach(info -> info.cleaningCallback.run());

    assertThat(coordinator.clientCount()).isEqualTo(2);

    // we are closing one of the producers to check the next allocated publisher ID
    ProducerInfo info = producerInfos.get(10);
    info.cleaningCallback.run();

    StreamProducer p = mock(StreamProducer.class);
    AtomicReference<Byte> publishingIdForNewProducer = new AtomicReference<>();
    doAnswer(answer(invoc -> publishingIdForNewProducer.set(invoc.getArgument(0))))
        .when(p)
        .assign(anyByte(), any());
    coordinator.registerProducer(p, null, "stream");

    verify(p, times(1)).assign(anyByte(), eq(client));
    // if the soft limit is less than the hard limit, publisher IDs keep going up
    // if the soft limit is equal to the hard limit, we re-use the ID that has just been left
    // available
    int expectedPublishingId =
        maxProducersByClient < MAX_PRODUCERS_PER_CLIENT ? maxProducersByClient : info.publishingId;
    assertThat(publishingIdForNewProducer).hasValue((byte) expectedPublishingId);

    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(2);

    // close some of the last producers, this should free a whole producer manager and a bit of the
    // next one
    for (int i = producerInfos.size() - 1; i > (producerCount - (extraProducerCount + 20)); i--) {
      ProducerInfo producerInfo = producerInfos.get(i);
      producerInfo.cleaningCallback.run();
    }

    waitAtMost(() -> coordinator.clientCount() == 1);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
  }

  @Test
  void producerShouldBeCreatedProperlyIfManagerClientIsRetried() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any()))
        .thenAnswer(
            (Answer<Client>)
                invocationOnMock -> {
                  shutdownListener.handle(
                      new Client.ShutdownContext(
                          Client.ShutdownContext.ShutdownReason.CLIENT_CLOSE));

                  return client;
                })
        .thenReturn(client);

    when(producer.isOpen()).thenReturn(true);
    when(trackingConsumer.isOpen()).thenReturn(true);

    CountDownLatch assignLatch = new CountDownLatch(1);
    doAnswer(answer(() -> assignLatch.countDown())).when(producer).assign(anyByte(), eq(client));

    coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).assign(anyByte(), eq(client));
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    assertThat(assignLatch).is(completed());
  }

  @Test
  void findCandidateNodesShouldReturnOnlyLeaderWhenForceLeaderIsTrue() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    assertThat(coordinator.findCandidateNodes("stream", true)).containsOnly(leaderWrapper());
  }

  @Test
  void findCandidateNodesShouldReturnLeaderAndReplicasWhenForceLeaderIsFalse() {
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    assertThat(coordinator.findCandidateNodes("stream", false))
        .hasSize(3)
        .contains(leaderWrapper())
        .containsAll(replicaWrappers());
  }

  @Test
  void findCandidateNodesShouldThrowIfThereIsNoLeaderAndForceLeaderIsTrue() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));
    assertThatThrownBy(() -> coordinator.findCandidateNodes("stream", true))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void findCandidateNodesShouldThrowIfNoMembersAndForceLeaderIsFalse() {
    when(locator.metadata("stream")).thenReturn(metadata(null, List.of()));
    assertThatThrownBy(() -> coordinator.findCandidateNodes("stream", false))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void findCandidateNodesShouldReturnOnlyReplicasIfNoLeaderAndForceLeaderIsFalse() {
    when(locator.metadata("stream")).thenReturn(metadata(null, replicas()));
    assertThat(coordinator.findCandidateNodes("stream", false))
        .hasSize(2)
        .containsAll(replicaWrappers());
  }

  @Test
  void failureClassificationShouldRetryOnlyTransientFailures() {
    assertThat(recoverable(new ConnectionStreamException("closed"))).isTrue();
    assertThat(recoverable(new ClientClosedException())).isTrue();
    assertThat(recoverable(new StreamNotAvailableException("stream"))).isTrue();
    assertThat(
            recoverable(
                new StreamException("declared", Constants.RESPONSE_CODE_PRECONDITION_FAILED)))
        .isTrue();
    assertThat(
            recoverable(
                new StreamException("deleted", Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST)))
        .isTrue();
    assertThat(recoverable(new StreamException("refused", Constants.RESPONSE_CODE_ACCESS_REFUSED)))
        .isFalse();
    assertThat(recoverable(new StreamDoesNotExistException("stream"))).isFalse();
    assertThat(recoverable(new IllegalStateException("boom"))).isFalse();
    assertThat(recoverable(null)).isFalse();
  }

  // the transition that parked an attempt has been applied once its retry is scheduled
  private void verifyRetryScheduled(Duration delay, int times) {
    verify(scheduledExecutorService, timeout(10_000).times(times))
        .schedule(any(Runnable.class), eq(delay.toMillis()), eq(TimeUnit.MILLISECONDS));
  }

  // a connection of its own, open as long as the flag says so
  private static Client mockClient(AtomicBoolean open) {
    Client c = mock(Client.class);
    when(c.isOpen()).then(invocation -> open.get());
    when(c.serverAdvertisedHost()).thenReturn(leader().getHost());
    when(c.serverAdvertisedPort()).thenReturn(leader().getPort());
    when(c.declarePublisher(anyByte(), isNull(), anyString()))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK));
    when(c.deletePublisher(anyByte())).thenReturn(new Response(Constants.RESPONSE_CODE_OK));
    return c;
  }

  private static ScheduledExecutorService createScheduledExecutorService() {
    return createScheduledExecutorService(1);
  }

  private static ScheduledExecutorService createScheduledExecutorService(int nbThreads) {
    return new ScheduledExecutorServiceWrapper(
        nbThreads == 1
            ? Executors.newSingleThreadScheduledExecutor()
            : Executors.newScheduledThreadPool(nbThreads));
  }
}
