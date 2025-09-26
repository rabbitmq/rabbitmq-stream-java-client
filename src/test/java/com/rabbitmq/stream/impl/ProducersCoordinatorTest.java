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

import static com.rabbitmq.stream.impl.ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT;
import static com.rabbitmq.stream.impl.ProducersCoordinator.pickSlot;
import static com.rabbitmq.stream.impl.TestUtils.CountDownLatchConditions.completed;
import static com.rabbitmq.stream.impl.TestUtils.answer;
import static com.rabbitmq.stream.impl.TestUtils.metadata;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import io.netty.channel.ConnectTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    coordinator =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            clientFactory,
            true);
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

    verify(producer, times(1)).setClient(client);

    cleanTask.run();
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
            true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    when(client.serverAdvertisedHost()).thenReturn("foo").thenReturn(leader().getHost());
    when(client.serverAdvertisedPort()).thenReturn(42).thenReturn(leader().getPort());

    Runnable cleanTask = c.registerProducer(producer, null, "stream");

    verify(clientFactory, times(2)).client(any());
    verify(producer, times(1)).setClient(client);

    cleanTask.run();
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
            true);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));
    when(clientFactory.client(any())).thenReturn(client);

    when(client.serverAdvertisedHost()).thenReturn(leader().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader().getPort());

    Runnable cleanTask = c.registerProducer(producer, null, "stream");

    verify(clientFactory, times(1)).client(any());
    verify(producer, times(1)).setClient(client);

    cleanTask.run();
  }

  @Test
  void shouldRedistributeProducerAndTrackingConsumerIfConnectionIsLost() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
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

    CountDownLatch setClientLatch = new CountDownLatch(2 + 2 + 1);
    doAnswer(answer(() -> setClientLatch.countDown())).when(producer).setClient(client);
    doAnswer(answer(() -> setClientLatch.countDown()))
        .when(trackingConsumer)
        .setTrackingClient(client);
    doAnswer(answer(() -> setClientLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .setClient(client);

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(producer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(trackingConsumer).running();
    doAnswer(answer(() -> runningLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .running();

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);
    coordinator.registerProducer(producerClosedAfterDisconnection, null, "stream");

    verify(producer, times(1)).setClient(client);
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(producerClosedAfterDisconnection, times(1)).setClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(2)).setClient(client);
    verify(producer, times(1)).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(2)).setTrackingClient(client);
    verify(trackingConsumer, times(1)).running();
    verify(producerClosedAfterDisconnection, times(1)).unavailable();
    verify(producerClosedAfterDisconnection, times(1)).setClient(client);
    verify(producerClosedAfterDisconnection, never()).running();
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);
  }

  @Test
  void shouldRecoverOnConnectionTimeout() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
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

    verify(this.producer, times(1)).setClient(client);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(this.producer, times(1)).unavailable();
    verify(this.producer, times(2)).setClient(client);
  }

  @Test
  void shouldDisposeProducerAndNotTrackingConsumerIfRecoveryTimesOut() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);

    verify(producer, times(1)).setClient(client);
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(trackingConsumer, never()).running();
    verify(trackingConsumer, never()).closeAfterStreamDeletion();
    assertThat(coordinator.nodesConnected()).isEqualTo(0);
    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldRedistributeProducersAndTrackingConsumersOnMetadataUpdate() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
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

    CountDownLatch setClientLatch = new CountDownLatch(2 + 2 + 1);

    when(fixedProducer.isOpen()).thenReturn(true);
    when(movingProducer.isOpen()).thenReturn(true);
    when(movingTrackingConsumer.isOpen()).thenReturn(true);
    when(fixedTrackingConsumer.isOpen()).thenReturn(true);

    doAnswer(answer(() -> setClientLatch.countDown())).when(movingProducer).setClient(client);

    doAnswer(answer(() -> setClientLatch.countDown()))
        .when(movingTrackingConsumer)
        .setTrackingClient(client);

    doAnswer(answer(() -> setClientLatch.countDown()))
        .when(producerClosedAfterDisconnection)
        .setClient(client);

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(movingProducer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(movingTrackingConsumer).running();

    coordinator.registerProducer(movingProducer, null, movingStream);
    coordinator.registerProducer(fixedProducer, null, fixedStream);
    coordinator.registerProducer(producerClosedAfterDisconnection, null, movingStream);
    coordinator.registerTrackingConsumer(movingTrackingConsumer);
    coordinator.registerTrackingConsumer(fixedTrackingConsumer);

    verify(movingProducer, times(1)).setClient(client);
    verify(fixedProducer, times(1)).setClient(client);
    verify(producerClosedAfterDisconnection, times(1)).setClient(client);
    verify(movingTrackingConsumer, times(1)).setTrackingClient(client);
    verify(fixedTrackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    // the created client is on leader2
    when(client.serverAdvertisedHost()).thenReturn(leader2().getHost());
    when(client.serverAdvertisedPort()).thenReturn(leader2().getPort());

    metadataListener.handle(movingStream, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(movingProducer, times(1)).unavailable();
    verify(movingProducer, times(2)).setClient(client);
    verify(movingProducer, times(1)).running();
    verify(movingTrackingConsumer, times(1)).unavailable();
    verify(movingTrackingConsumer, times(2)).setTrackingClient(client);
    verify(movingTrackingConsumer, times(1)).running();

    verify(producerClosedAfterDisconnection, times(1)).unavailable();
    verify(producerClosedAfterDisconnection, times(1)).setClient(client);
    verify(producerClosedAfterDisconnection, never()).running();

    verify(fixedProducer, never()).unavailable();
    verify(fixedProducer, times(1)).setClient(client);
    verify(fixedProducer, never()).running();
    verify(fixedTrackingConsumer, never()).unavailable();
    verify(fixedTrackingConsumer, times(1)).setTrackingClient(client);
    verify(fixedTrackingConsumer, never()).running();
    assertThat(coordinator.nodesConnected()).isEqualTo(2);
    assertThat(coordinator.clientCount()).isEqualTo(2);
  }

  @Test
  void shouldDisposeProducerIfStreamIsDeleted() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();

    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldDisposeProducerAndNotTrackingConsumerIfMetadataUpdateTimesOut() throws Exception {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown()))
        .when(producer)
        .closeAfterStreamDeletion(any(Short.class));

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerTrackingConsumer(trackingConsumer);

    verify(producer, times(1)).setClient(client);
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    verify(trackingConsumer, times(1)).unavailable();
    verify(trackingConsumer, times(1)).setTrackingClient(client);
    verify(trackingConsumer, never()).running();
    verify(trackingConsumer, never()).closeAfterStreamDeletion();
    assertThat(coordinator.nodesConnected()).isEqualTo(0);
    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @ParameterizedTest
  @ValueSource(ints = {50, ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT})
  void growShrinkResourcesBasedOnProducersAndTrackingConsumersCount(int maxProducersByClient) {
    scheduledExecutorService = createScheduledExecutorService();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    int extraProducerCount = maxProducersByClient / 5;
    int producerCount = maxProducersByClient + extraProducerCount;

    coordinator =
        new ProducersCoordinator(
            environment,
            maxProducersByClient,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            type -> "producer-connection",
            clientFactory,
            true);

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
                  .setPublisherId(anyByte());
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

    assertThat(coordinator.clientCount()).isEqualTo(2);

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
        .setPublisherId(anyByte());
    coordinator.registerProducer(p, null, "stream");

    verify(p, times(1)).setClient(client);
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

    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);
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

    CountDownLatch setClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> setClientLatch.countDown())).when(producer).setClient(client);

    coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(coordinator.nodesConnected()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    assertThat(setClientLatch).is(completed());
  }

  @Test
  void pickSlotTest() {
    ConcurrentMap<Byte, String> map = new ConcurrentHashMap<>();
    AtomicInteger sequence = new AtomicInteger(0);
    assertThat(pickSlot(map, "0", sequence)).isZero();
    assertThat(sequence).hasValue(1);
    assertThat(map).hasSize(1);
    assertThat(pickSlot(map, "1", sequence)).isEqualTo(1);
    assertThat(pickSlot(map, "2", sequence)).isEqualTo(2);
    assertThat(pickSlot(map, "3", sequence)).isEqualTo(3);
    map.remove((byte) 1);
    assertThat(pickSlot(map, "4", sequence)).isEqualTo(4);
    assertThat(map).hasSize(4);

    sequence.set(ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT - 2);
    assertThat(pickSlot(map, "254", sequence)).isEqualTo(254);
    assertThat(pickSlot(map, "255", sequence)).isEqualTo(255);
    // 0 is already taken, so we should get index 1 when we overflow
    assertThat(pickSlot(map, "256", sequence)).isEqualTo(1);
    assertThat(pickSlot(map, "257", sequence)).isEqualTo(5);
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

  private static ScheduledExecutorService createScheduledExecutorService() {
    return new ScheduledExecutorServiceWrapper(Executors.newSingleThreadScheduledExecutor());
  }
}
