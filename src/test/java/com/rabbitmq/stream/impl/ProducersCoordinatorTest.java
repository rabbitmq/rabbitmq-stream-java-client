// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.answer;
import static com.rabbitmq.stream.impl.TestUtils.metadata;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.anyByte;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ProducersCoordinatorTest {

  @Mock StreamEnvironment environment;
  @Mock Client locator;
  @Mock StreamProducer producer;
  @Mock StreamConsumer committingConsumer;
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
    return new Client.Broker("leader", 5551);
  }

  static Client.Broker leader1() {
    return new Client.Broker("leader-1", 5551);
  }

  static Client.Broker leader2() {
    return new Client.Broker("leader-2", 5551);
  }

  static List<Client.Broker> replicas() {
    return Arrays.asList(new Client.Broker("replica1", 5551), new Client.Broker("replica2", 5551));
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
    when(environment.locator()).thenReturn(locator);
    when(environment.clientParametersCopy()).thenReturn(clientParameters);
    when(environment.addressResolver()).thenReturn(address -> address);
    when(committingConsumer.stream()).thenReturn("stream");
    when(client.declarePublisher(anyByte(), isNull(), anyString()))
        .thenReturn(new Response(Constants.RESPONSE_CODE_OK));
    coordinator =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_COMMITTING_CONSUMERS_PER_CLIENT,
            clientFactory);
  }

  @AfterEach
  void tearDown() throws Exception {
    // just taking the opportunity to check toString() generates valid JSON
    MonitoringTestUtils.extract(coordinator);
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    mocks.close();
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
            Utils.connectToAdvertisedNodeClientFactory(
                    context.key(), clientFactory, Duration.ofMillis(1))
                .client(context);
    ProducersCoordinator c =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_COMMITTING_CONSUMERS_PER_CLIENT,
            cf);
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
            Utils.connectToAdvertisedNodeClientFactory(
                    context.key(), clientFactory, Duration.ofMillis(1))
                .client(context);
    ProducersCoordinator c =
        new ProducersCoordinator(
            environment,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_COMMITTING_CONSUMERS_PER_CLIENT,
            cf);
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
  void shouldRedistributeProducerAndCommittingConsumerIfConnectionIsLost() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(null, replicas()))
        .thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch setClientLatch = new CountDownLatch(2 + 2);
    doAnswer(answer(() -> setClientLatch.countDown())).when(producer).setClient(client);
    doAnswer(answer(() -> setClientLatch.countDown())).when(committingConsumer).setClient(client);

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(producer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(committingConsumer).running();

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerCommittingConsumer(committingConsumer);

    verify(producer, times(1)).setClient(client);
    verify(committingConsumer, times(1)).setClient(client);
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(2)).setClient(client);
    verify(producer, times(1)).running();
    verify(committingConsumer, times(1)).unavailable();
    verify(committingConsumer, times(2)).setClient(client);
    verify(committingConsumer, times(1)).running();
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);
  }

  @Test
  void shouldDisposeProducerAndNotCommittingConsumerIfRecoveryTimesOut() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown())).when(producer).closeAfterStreamDeletion();

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerCommittingConsumer(committingConsumer);

    verify(producer, times(1)).setClient(client);
    verify(committingConsumer, times(1)).setClient(client);
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    verify(committingConsumer, times(1)).unavailable();
    verify(committingConsumer, times(1)).setClient(client);
    verify(committingConsumer, never()).running();
    verify(committingConsumer, never()).closeAfterStreamDeletion();
    assertThat(coordinator.poolSize()).isEqualTo(0);
    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldRedistributeProducersAndCommittingConsumersOnMetadataUpdate() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    String movingStream = "moving-stream";
    when(locator.metadata(movingStream))
        .thenReturn(metadata(movingStream, leader1(), replicas()))
        .thenReturn(metadata(movingStream, leader1(), replicas())) // for the first 2 registrations
        .thenReturn(metadata(movingStream, null, replicas()))
        .thenReturn(metadata(movingStream, leader2(), replicas()));

    String fixedStream = "fixed-stream";
    when(locator.metadata(fixedStream)).thenReturn(metadata(fixedStream, leader1(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    StreamProducer movingProducer = mock(StreamProducer.class);
    StreamProducer fixedProducer = mock(StreamProducer.class);
    StreamConsumer movingCommittingConsumer = mock(StreamConsumer.class);
    StreamConsumer fixedCommittingConsumer = mock(StreamConsumer.class);
    when(movingCommittingConsumer.stream()).thenReturn(movingStream);
    when(fixedCommittingConsumer.stream()).thenReturn(fixedStream);

    CountDownLatch setClientLatch = new CountDownLatch(2 + 2);

    doAnswer(answer(() -> setClientLatch.countDown())).when(movingProducer).setClient(client);

    doAnswer(answer(() -> setClientLatch.countDown()))
        .when(movingCommittingConsumer)
        .setClient(client);

    CountDownLatch runningLatch = new CountDownLatch(1 + 1);
    doAnswer(answer(() -> runningLatch.countDown())).when(movingProducer).running();
    doAnswer(answer(() -> runningLatch.countDown())).when(movingCommittingConsumer).running();

    coordinator.registerProducer(movingProducer, null, movingStream);
    coordinator.registerProducer(fixedProducer, null, fixedStream);
    coordinator.registerCommittingConsumer(movingCommittingConsumer);
    coordinator.registerCommittingConsumer(fixedCommittingConsumer);

    verify(movingProducer, times(1)).setClient(client);
    verify(fixedProducer, times(1)).setClient(client);
    verify(movingCommittingConsumer, times(1)).setClient(client);
    verify(fixedCommittingConsumer, times(1)).setClient(client);
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle(movingStream, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(runningLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(movingProducer, times(1)).unavailable();
    verify(movingProducer, times(2)).setClient(client);
    verify(movingProducer, times(1)).running();
    verify(movingCommittingConsumer, times(1)).unavailable();
    verify(movingCommittingConsumer, times(2)).setClient(client);
    verify(movingCommittingConsumer, times(1)).running();

    verify(fixedProducer, never()).unavailable();
    verify(fixedProducer, times(1)).setClient(client);
    verify(fixedProducer, never()).running();
    verify(fixedCommittingConsumer, never()).unavailable();
    verify(fixedCommittingConsumer, times(1)).setClient(client);
    verify(fixedCommittingConsumer, never()).running();
    assertThat(coordinator.poolSize()).isEqualTo(2);
    assertThat(coordinator.clientCount()).isEqualTo(2);
  }

  @Test
  void shouldDisposeProducerIfStreamIsDeleted() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown())).when(producer).closeAfterStreamDeletion();

    coordinator.registerProducer(producer, null, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();

    assertThat(coordinator.poolSize()).isEqualTo(0);
    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldDisposeProducerAndNotCommittingConsumerIfMetadataUpdateTimesOut() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(metadata(leader(), replicas()))
        .thenReturn(metadata(leader(), replicas())) // for the 2 registrations
        .thenReturn(metadata(null, replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(answer(() -> closeClientLatch.countDown())).when(producer).closeAfterStreamDeletion();

    coordinator.registerProducer(producer, null, "stream");
    coordinator.registerCommittingConsumer(committingConsumer);

    verify(producer, times(1)).setClient(client);
    verify(committingConsumer, times(1)).setClient(client);
    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    verify(committingConsumer, times(1)).unavailable();
    verify(committingConsumer, times(1)).setClient(client);
    verify(committingConsumer, never()).running();
    verify(committingConsumer, never()).closeAfterStreamDeletion();
    assertThat(coordinator.poolSize()).isEqualTo(0);
    assertThat(coordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void growShrinkResourcesBasedOnProducersAndCommittingConsumersCount() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(locator.metadata("stream")).thenReturn(metadata(leader(), replicas()));

    when(clientFactory.client(any())).thenReturn(client);

    int extraProducerCount = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT / 5;
    int producerCount = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT + extraProducerCount;

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

    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(2);

    // let's add some committing consumers
    int extraCommittingConsumerCount = ProducersCoordinator.MAX_COMMITTING_CONSUMERS_PER_CLIENT / 5;
    int committingConsumerCount =
        ProducersCoordinator.MAX_COMMITTING_CONSUMERS_PER_CLIENT * 2 + extraCommittingConsumerCount;

    class CommittingConsumerInfo {
      StreamConsumer consumer;
      Runnable cleaningCallback;
    }
    List<CommittingConsumerInfo> committingConsumerInfos = new ArrayList<>(committingConsumerCount);
    IntStream.range(0, committingConsumerCount)
        .forEach(
            i -> {
              StreamConsumer c = mock(StreamConsumer.class);
              when(c.stream()).thenReturn("stream");
              CommittingConsumerInfo info = new CommittingConsumerInfo();
              info.consumer = c;
              Runnable cleaningCallback = coordinator.registerCommittingConsumer(c);
              info.cleaningCallback = cleaningCallback;
              committingConsumerInfos.add(info);
            });

    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount())
        .as("new committing consumers needs yet another client")
        .isEqualTo(3);

    Collections.reverse(committingConsumerInfos);
    // let's remove some committing consumers to free 1 client
    IntStream.range(0, extraCommittingConsumerCount)
        .forEach(
            i -> {
              committingConsumerInfos.get(0).cleaningCallback.run();
              committingConsumerInfos.remove(0);
            });

    assertThat(coordinator.clientCount()).isEqualTo(2);

    // let's free the rest of committing consumers
    committingConsumerInfos.forEach(info -> info.cleaningCallback.run());

    assertThat(coordinator.clientCount()).isEqualTo(2);

    ProducerInfo info = producerInfos.get(10);
    info.cleaningCallback.run();

    StreamProducer p = mock(StreamProducer.class);
    AtomicReference<Byte> publishingIdForNewProducer = new AtomicReference<>();
    doAnswer(answer(invoc -> publishingIdForNewProducer.set(invoc.getArgument(0))))
        .when(p)
        .setPublisherId(anyByte());
    coordinator.registerProducer(p, null, "stream");

    verify(p, times(1)).setClient(client);
    assertThat(publishingIdForNewProducer.get()).isEqualTo(info.publishingId);

    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(2);

    // close some of the last producers, this should free a whole producer manager and a bit of the
    // next one
    for (int i = producerInfos.size() - 1; i > (producerCount - (extraProducerCount + 20)); i--) {
      ProducerInfo producerInfo = producerInfos.get(i);
      producerInfo.cleaningCallback.run();
    }

    assertThat(coordinator.poolSize()).isEqualTo(1);
    assertThat(coordinator.clientCount()).isEqualTo(1);
  }
}
