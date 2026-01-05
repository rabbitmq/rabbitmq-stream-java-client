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

import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ObservationCollector;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.StreamEnvironment.LocatorNotAvailableException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StreamEnvironmentUnitTest {

  @Mock BackOffDelayPolicy recoveryBackOffDelayPolicy;
  @Mock BackOffDelayPolicy topologyUpdateBackOffDelayPolicy;
  @Mock Function<Client.ClientParameters, Client> cf;
  @Mock Client client;
  AutoCloseable mocks;

  StreamEnvironment environment;
  ScheduledExecutorService scheduledExecutorService;
  volatile Client.ShutdownListener shutdownListener;

  @BeforeEach
  void init() {
    AtomicReference<Client.ClientParameters> cpReference = new AtomicReference<>();
    Client.ClientParameters clientParameters =
        new Client.ClientParameters() {
          @Override
          public Client.ClientParameters shutdownListener(
              Client.ShutdownListener shutdownListener) {
            StreamEnvironmentUnitTest.this.shutdownListener = shutdownListener;
            return super.shutdownListener(shutdownListener);
          }

          @Override
          public Client.ClientParameters duplicate() {
            return cpReference.get();
          }
        };
    cpReference.set(clientParameters);
    mocks = MockitoAnnotations.openMocks(this);
    when(cf.apply(any(Client.ClientParameters.class))).thenReturn(client);
    when(client.getHost()).thenReturn("localhost");
    when(client.getPort()).thenReturn(5552);

    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    environment =
        new StreamEnvironment(
            scheduledExecutorService,
            clientParameters,
            Collections.emptyList(),
            recoveryBackOffDelayPolicy,
            topologyUpdateBackOffDelayPolicy,
            host -> host,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            null,
            null,
            Utils.byteBufAllocator(),
            false,
            type -> "locator-connection",
            cf,
            ObservationCollector.NO_OP,
            false,
            true,
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            -1);
  }

  @AfterEach
  void tearDown() throws Exception {
    // just taking the opportunity to check toString() generates valid JSON
    MonitoringTestUtils.extract(environment);
    environment.close();
    scheduledExecutorService.shutdownNow();
    mocks.close();
  }

  @Test
  void locatorRecoversAfterDisconnection() throws Exception {
    verify(cf, times(1)).apply(any(Client.ClientParameters.class));
    when(recoveryBackOffDelayPolicy.delay(anyInt())).thenReturn(Duration.ofMillis(50));
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.HEARTBEAT_FAILURE));
    Thread.sleep(50 * 3);
    verify(cf, times(1 + 1)).apply(any(Client.ClientParameters.class));
  }

  @Test
  void retryLocatorRecovery() throws Exception {
    verify(cf, times(1)).apply(any(Client.ClientParameters.class));
    when(cf.apply(any(Client.ClientParameters.class)))
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenReturn(client);
    when(recoveryBackOffDelayPolicy.delay(anyInt())).thenReturn(Duration.ofMillis(50));
    shutdownListener.handle(
        new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.HEARTBEAT_FAILURE));
    Thread.sleep(50 * 5);
    verify(cf, times(1 + 3)).apply(any(Client.ClientParameters.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldTryUrisOnInitializationFailure() throws Exception {
    reset(cf);
    // we don't want the scheduled retry to kick in
    when(recoveryBackOffDelayPolicy.delay(anyInt())).thenReturn(Duration.ofMinutes(60));
    when(cf.apply(any(Client.ClientParameters.class)))
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenReturn(client);

    URI uri = new URI("rabbitmq-stream://localhost:5552");
    environment =
        new StreamEnvironment(
            scheduledExecutorService,
            new Client.ClientParameters(),
            Arrays.asList(uri, uri, uri),
            recoveryBackOffDelayPolicy,
            topologyUpdateBackOffDelayPolicy,
            host -> host,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            null,
            null,
            Utils.byteBufAllocator(),
            false,
            type -> "locator-connection",
            cf,
            ObservationCollector.NO_OP,
            false,
            true,
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            -1);
    verify(cf, times(3)).apply(any(Client.ClientParameters.class));
  }

  @ParameterizedTest
  @CsvSource({"false,1", "true,0"})
  @SuppressWarnings("unchecked")
  void shouldNotOpenConnectionWhenLazyInitIsEnabled(
      boolean lazyInit, int expectedConnectionCreation) {
    reset(cf);
    when(cf.apply(any(Client.ClientParameters.class))).thenReturn(client);
    environment =
        new StreamEnvironment(
            scheduledExecutorService,
            new ClientParameters(),
            Collections.emptyList(),
            recoveryBackOffDelayPolicy,
            topologyUpdateBackOffDelayPolicy,
            host -> host,
            ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT,
            ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT,
            ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT,
            null,
            null,
            Utils.byteBufAllocator(),
            lazyInit,
            type -> "locator-connection",
            cf,
            ObservationCollector.NO_OP,
            false,
            true,
            Duration.ofMillis(100),
            Duration.ofMillis(100),
            -1);
    verify(cf, times(expectedConnectionCreation)).apply(any(Client.ClientParameters.class));
  }

  @Test
  void locatorOperationShouldReturnOperationResultIfNoProblem() {
    AtomicInteger counter = new AtomicInteger();
    int result =
        StreamEnvironment.locatorOperation(
            c -> counter.incrementAndGet(),
            CLIENT_SUPPLIER,
            BackOffDelayPolicy.fixed(Duration.ZERO));
    assertThat(result).isEqualTo(1);
  }

  @Test
  void locatorOperationShouldRetryAndReturnResultIfLocatorException() {
    AtomicInteger counter = new AtomicInteger();
    int result =
        StreamEnvironment.locatorOperation(
            c -> {
              if (counter.incrementAndGet() < 2) {
                throw new LocatorNotAvailableException();
              } else {
                return counter.get();
              }
            },
            CLIENT_SUPPLIER,
            BackOffDelayPolicy.fixed(Duration.ofMillis(10)));
    assertThat(result).isEqualTo(2);
  }

  @Test
  void locatorOperationShouldThrowLocatorExceptionWhenRetryExhausts() {
    AtomicInteger counter = new AtomicInteger();
    assertThatThrownBy(
            () ->
                StreamEnvironment.locatorOperation(
                    c -> {
                      counter.incrementAndGet();
                      throw new LocatorNotAvailableException();
                    },
                    CLIENT_SUPPLIER,
                    BackOffDelayPolicy.fixed(Duration.ofMillis(10))))
        .isInstanceOf(LocatorNotAvailableException.class);
    assertThat(counter).hasValue(3);
  }

  @Test
  void locatorOperationShouldThrowInterruptedExceptionAsCauseIfInterrupted()
      throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Exception> exception = new AtomicReference<>();
    Thread thread =
        new Thread(
            () -> {
              try {
                StreamEnvironment.locatorOperation(
                    c -> {
                      latch.countDown();
                      throw new LocatorNotAvailableException();
                    },
                    CLIENT_SUPPLIER,
                    BackOffDelayPolicy.fixed(Duration.ofMinutes(10)));
              } catch (StreamException e) {
                exception.set(e);
              }
            });
    thread.start();
    latchAssert(latch).completes();
    Thread.sleep(100);
    thread.interrupt();
    Thread.sleep(100);
    assertThat(exception.get())
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(InterruptedException.class);
  }

  @Test
  void locatorOperationShouldNotRetryAndReThrowUnexpectedException() {
    AtomicInteger counter = new AtomicInteger();
    assertThatThrownBy(
            () ->
                StreamEnvironment.locatorOperation(
                    c -> {
                      counter.incrementAndGet();
                      throw new RuntimeException();
                    },
                    CLIENT_SUPPLIER,
                    BackOffDelayPolicy.fixed(Duration.ofMillis(10))))
        .isInstanceOf(RuntimeException.class);
    assertThat(counter).hasValue(1);
  }

  private static final Supplier<StreamEnvironment.Locator> CLIENT_SUPPLIER =
      () -> {
        StreamEnvironment.Locator locator =
            new StreamEnvironment.Locator(-1, new Address("localhost", 5555));
        locator.client(mock(Client.class));
        return locator;
      };
}
