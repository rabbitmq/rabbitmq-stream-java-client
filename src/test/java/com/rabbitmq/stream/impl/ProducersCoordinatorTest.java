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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
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
import java.util.function.Function;
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
  @Mock Function<Client.ClientParameters, Client> clientFactory;
  @Mock Client client;
  AutoCloseable mocks;
  ProducersCoordinator producersCoordinator;
  ScheduledExecutorService scheduledExecutorService;

  volatile Client.ShutdownListener shutdownListener;
  volatile Client.MetadataListener metadataListener;

  static Duration ms(long ms) {
    return Duration.ofMillis(ms);
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
    producersCoordinator = new ProducersCoordinator(environment, clientFactory);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
    mocks.close();
  }

  @Test
  void registerShouldThrowExceptionWhenNoMetadataForTheStream() {
    assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenStreamDoesNotExist() {
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, null, null)));
    assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
        .isInstanceOf(StreamDoesNotExistException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenMetadataResponseIsNotOk() {
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_ACCESS_REFUSED, null, null)));
    assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registerShouldThrowExceptionWhenNoLeader() {
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));
    assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void registerShouldAllowPublishing() {
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())));
    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    Runnable cleanTask = producersCoordinator.registerProducer(producer, "stream");

    verify(producer, times(1)).setClient(client);

    cleanTask.run();
  }

  @Test
  void shouldRedistributeProducerIfConnectionIsLost() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    CountDownLatch setClientLatch = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              setClientLatch.countDown();
              return null;
            })
        .when(producer)
        .setClient(client);

    producersCoordinator.registerProducer(producer, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(2)).setClient(client);
    verify(producer, times(1)).running();
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);
  }

  @Test
  void shouldDisposeProducerIfRecoveryTimesOut() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.recoveryBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closeClientLatch.countDown();
              return null;
            })
        .when(producer)
        .closeAfterStreamDeletion();

    producersCoordinator.registerProducer(producer, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);

    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.UNKNOWN));

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    assertThat(producersCoordinator.poolSize()).isEqualTo(0);
    assertThat(producersCoordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldRedistributeProducersOnMetadataUpdate() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    Duration retryDelay = Duration.ofMillis(50);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    String movingStream = "moving-stream";
    when(locator.metadata(movingStream))
        .thenReturn(
            Collections.singletonMap(
                movingStream,
                new Client.StreamMetadata(
                    movingStream, Constants.RESPONSE_CODE_OK, leader1(), replicas())))
        .thenReturn(
            Collections.singletonMap(
                movingStream,
                new Client.StreamMetadata(
                    movingStream, Constants.RESPONSE_CODE_OK, null, replicas())))
        .thenReturn(
            Collections.singletonMap(
                movingStream,
                new Client.StreamMetadata(
                    movingStream, Constants.RESPONSE_CODE_OK, leader2(), replicas())));

    String fixedStream = "fixed-stream";
    when(locator.metadata(fixedStream))
        .thenReturn(
            Collections.singletonMap(
                fixedStream,
                new Client.StreamMetadata(
                    fixedStream, Constants.RESPONSE_CODE_OK, leader1(), replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    StreamProducer movingProducer = mock(StreamProducer.class);
    StreamProducer fixedProducer = mock(StreamProducer.class);

    CountDownLatch setClientLatch = new CountDownLatch(2);
    doAnswer(
            invocation -> {
              setClientLatch.countDown();
              return null;
            })
        .when(movingProducer)
        .setClient(client);

    producersCoordinator.registerProducer(movingProducer, movingStream);
    producersCoordinator.registerProducer(fixedProducer, fixedStream);

    verify(movingProducer, times(1)).setClient(client);
    verify(fixedProducer, times(1)).setClient(client);
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);

    metadataListener.handle(movingStream, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(setClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(movingProducer, times(1)).unavailable();
    verify(movingProducer, times(2)).setClient(client);
    verify(movingProducer, times(1)).running();

    verify(fixedProducer, never()).unavailable();
    verify(fixedProducer, times(1)).setClient(client);
    verify(fixedProducer, never()).running();
    assertThat(producersCoordinator.poolSize()).isEqualTo(2);
    assertThat(producersCoordinator.clientCount()).isEqualTo(2);
  }

  @Test
  void shouldDisposeProducerIfStreamIsDeleted() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, null, replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closeClientLatch.countDown();
              return null;
            })
        .when(producer)
        .closeAfterStreamDeletion();

    producersCoordinator.registerProducer(producer, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();

    assertThat(producersCoordinator.poolSize()).isEqualTo(0);
    assertThat(producersCoordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void shouldDisposeProducerIfMetadataUpdateTimesOut() throws Exception {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(environment.topologyUpdateBackOffDelayPolicy())
        .thenReturn(BackOffDelayPolicy.fixedWithInitialDelay(ms(10), ms(10), ms(100)));
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

    CountDownLatch closeClientLatch = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              closeClientLatch.countDown();
              return null;
            })
        .when(producer)
        .closeAfterStreamDeletion();

    producersCoordinator.registerProducer(producer, "stream");

    verify(producer, times(1)).setClient(client);
    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);

    metadataListener.handle("stream", Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE);

    assertThat(closeClientLatch.await(5, TimeUnit.SECONDS)).isTrue();
    verify(producer, times(1)).unavailable();
    verify(producer, times(1)).setClient(client);
    verify(producer, never()).running();
    assertThat(producersCoordinator.poolSize()).isEqualTo(0);
    assertThat(producersCoordinator.clientCount()).isEqualTo(0);
  }

  @Test
  void growShrinkResourcesBasedOnProducersCount() {
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
    when(locator.metadata("stream"))
        .thenReturn(
            Collections.singletonMap(
                "stream",
                new Client.StreamMetadata(
                    "stream", Constants.RESPONSE_CODE_OK, leader(), replicas())));

    when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);

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
              doAnswer(
                      invocation -> {
                        info.publishingId = invocation.getArgument(0);
                        return null;
                      })
                  .when(p)
                  .setPublisherId(anyByte());
              Runnable cleaningCallback = producersCoordinator.registerProducer(p, "stream");
              info.cleaningCallback = cleaningCallback;
              producerInfos.add(info);
            });

    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(2);

    ProducerInfo info = producerInfos.get(10);
    info.cleaningCallback.run();

    StreamProducer p = mock(StreamProducer.class);
    AtomicReference<Byte> publishingIdForNewProducer = new AtomicReference<>();
    doAnswer(
            invocation -> {
              publishingIdForNewProducer.set(invocation.getArgument(0));
              return null;
            })
        .when(p)
        .setPublisherId(anyByte());
    producersCoordinator.registerProducer(p, "stream");

    verify(p, times(1)).setClient(client);
    assertThat(publishingIdForNewProducer.get()).isEqualTo(info.publishingId);

    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(2);

    // close some of the last producers, this should free a whole producer manager and a bit of the
    // next one
    for (int i = producerInfos.size() - 1; i > (producerCount - (extraProducerCount + 20)); i--) {
      ProducerInfo producerInfo = producerInfos.get(i);
      producerInfo.cleaningCallback.run();
    }

    assertThat(producersCoordinator.poolSize()).isEqualTo(1);
    assertThat(producersCoordinator.clientCount()).isEqualTo(1);
  }

  Client.Broker leader() {
    return new Client.Broker("leader", -1);
  }

  Client.Broker leader1() {
    return new Client.Broker("leader-1", -1);
  }

  Client.Broker leader2() {
    return new Client.Broker("leader-2", -1);
  }

  List<Client.Broker> replicas() {
    return Arrays.asList(new Client.Broker("replica1", -1), new Client.Broker("replica2", -1));
  }
}
