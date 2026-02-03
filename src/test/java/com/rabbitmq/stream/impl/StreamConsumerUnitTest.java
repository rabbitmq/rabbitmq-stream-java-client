// Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Resource.State.CLOSED;
import static com.rabbitmq.stream.Resource.State.CLOSING;
import static com.rabbitmq.stream.Resource.State.OPEN;
import static com.rabbitmq.stream.Resource.State.OPENING;
import static com.rabbitmq.stream.Resource.State.RECOVERING;
import static com.rabbitmq.stream.impl.StreamConsumer.getStoredOffsetSafely;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.ConsumerFlowStrategy;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.NoOffsetException;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.SubscriptionListener;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StreamConsumerUnitTest {

  private static final Duration LARGE_RPC_TIMEOUT = Duration.ofSeconds(10);

  @Mock StreamConsumer consumer;
  @Mock StreamEnvironment environment;
  @Mock Client client;
  @Mock StreamEnvironment.Locator locator;

  AutoCloseable closeable;

  ScheduledExecutorService scheduledExecutorService;

  @BeforeEach
  public void init() {
    closeable = MockitoAnnotations.openMocks(this);
    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    when(environment.scheduledExecutorService()).thenReturn(scheduledExecutorService);
  }

  @AfterEach
  public void tearDown() throws Exception {
    closeable.close();
    scheduledExecutorService.shutdownNow();
  }

  @Test
  void getStoredOffsetSafely_ShouldReturnOffsetWhenLeaderConnectionIsAvailable() {
    when(consumer.storedOffset()).thenReturn(42L);
    assertThat(getStoredOffsetSafely(consumer, environment)).isEqualTo(42);
    verify(consumer, times(1)).storedOffset();
    verify(environment, never()).scheduledExecutorService();
  }

  @Test
  void getStoredOffsetSafely_ShouldRetryWhenLeaderConnectionIsNotAvailable() {
    when(consumer.storedOffset()).thenThrow(new IllegalStateException());
    when(consumer.storedOffset(any())).thenThrow(new IllegalStateException()).thenReturn(42L);
    Duration retryDelay = Duration.ofMillis(10);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    when(environment.rpcTimeout()).thenReturn(LARGE_RPC_TIMEOUT);
    assertThat(getStoredOffsetSafely(consumer, environment)).isEqualTo(42);
    verify(consumer, times(1)).storedOffset();
    verify(environment, times(1)).scheduledExecutorService();
    verify(consumer, times(2)).storedOffset(any());
  }

  @Test
  void getStoredOffsetSafely_ShouldThrowNoOffsetException() {
    when(consumer.storedOffset()).thenThrow(new NoOffsetException(""));
    assertThatThrownBy(() -> getStoredOffsetSafely(consumer, environment))
        .isInstanceOf(NoOffsetException.class);
    verify(consumer, times(1)).storedOffset();
    verify(environment, never()).scheduledExecutorService();
    verify(consumer, never()).storedOffset(any());
  }

  @Test
  void getStoredOffsetSafely_ShouldReThrowNoOffsetExceptionFromFallback() {
    when(consumer.storedOffset()).thenThrow(new IllegalStateException());
    when(consumer.storedOffset(any())).thenThrow(new NoOffsetException("no offset"));
    Duration retryDelay = Duration.ofMillis(10);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    assertThatThrownBy(() -> getStoredOffsetSafely(consumer, environment))
        .isInstanceOf(NoOffsetException.class);
    verify(consumer, times(1)).storedOffset();
    verify(environment, times(1)).scheduledExecutorService();
    verify(consumer, times(1)).storedOffset(any());
  }

  @Test
  void getStoredOffsetSafely_ShouldThrowTimeoutExceptionIfFallbackTimesOut() {
    when(consumer.storedOffset()).thenThrow(new IllegalStateException());
    when(consumer.storedOffset(any())).thenThrow(new IllegalStateException());
    Duration retryDelay = Duration.ofMillis(50);
    Duration rpcTimeout = retryDelay.multipliedBy(2);
    when(environment.rpcTimeout()).thenReturn(rpcTimeout);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    assertThatThrownBy(() -> getStoredOffsetSafely(consumer, environment))
        .isInstanceOf(TimeoutStreamException.class);
    verify(consumer, times(1)).storedOffset();
    verify(environment, times(1)).scheduledExecutorService();
    verify(consumer, atLeastOnce()).storedOffset(any());
  }

  @Test
  void getStoredOffsetSafely_ShouldUseLocatorConnectionWhenLeaderConnectionIsNotAvailable() {
    when(consumer.canTrack()).thenReturn(true);
    when(consumer.storedOffset()).thenThrow(new IllegalStateException());
    when(consumer.storedOffset(any())).thenCallRealMethod();
    when(environment.locator()).thenReturn(locator);
    when(locator.client()).thenReturn(client);
    when(client.queryOffset(isNull(), isNull()))
        .thenReturn(new Client.QueryOffsetResponse(Constants.RESPONSE_CODE_OK, 42L));
    Duration retryDelay = Duration.ofMillis(10);
    when(environment.recoveryBackOffDelayPolicy()).thenReturn(BackOffDelayPolicy.fixed(retryDelay));
    assertThat(getStoredOffsetSafely(consumer, environment)).isEqualTo(42L);
    verify(consumer, times(1)).storedOffset();
    verify(environment, times(1)).scheduledExecutorService();
    verify(environment, times(1)).locator();
    verify(consumer, times(1)).storedOffset(any());
  }

  @Test
  void stateShouldRecoverWhenSubAndTrackingAreBack() {
    List<Resource.State> states = new ArrayList<>();
    Resource.StateListener lst = context -> states.add(context.currentState());
    StreamConsumer c = csrLst(lst);

    assertThat(states).containsExactly(OPENING, OPEN);
    // sub connection goes down
    c.markRecovering();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING);
    // tracking connection goes down
    c.unavailable();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING);
    // sub connection comes back
    c.markOpen();
    // still in "recovering" state, as the tracking connection is still down
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING);
    // tracking connection comes back
    c.running();
    // now back to open
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN);

    c.close();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED);
  }

  @Test
  void stateShouldRecoverWhenConnectionsAreBack() {
    List<Resource.State> states = new ArrayList<>();
    Resource.StateListener lst = context -> states.add(context.currentState());
    StreamConsumer c = csrLst(lst);

    assertThat(states).containsExactly(OPENING, OPEN);
    // sub connection goes down
    c.markRecovering();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING);
    // sub connection comes back
    c.markOpen();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN);

    // tracking connection goes down
    c.unavailable();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, RECOVERING);
    // tracking connection comes back
    c.running();
    assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, RECOVERING, OPEN);

    c.close();
    assertThat(states)
        .containsExactly(OPENING, OPEN, RECOVERING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED);
  }

  private StreamConsumer csrLst(Resource.StateListener lst) {
    when(environment.registerConsumer(
            any(StreamConsumer.class),
            anyString(),
            any(OffsetSpecification.class),
            anyString(),
            any(SubscriptionListener.class),
            any(Runnable.class),
            any(MessageHandler.class),
            anyMap(),
            any(ConsumerFlowStrategy.class)))
        .thenReturn(() -> {});
    return new StreamConsumer(
        "s",
        OffsetSpecification.first(),
        (ctx, msg) -> {},
        "app",
        environment,
        new StreamConsumerBuilder.TrackingConfiguration(
            false, false, -1, Duration.ZERO, Duration.ZERO),
        false,
        ctx -> {},
        Collections.emptyMap(),
        null,
        ConsumerFlowStrategy.creditOnChunkArrival(10),
        List.of(lst));
  }
}
