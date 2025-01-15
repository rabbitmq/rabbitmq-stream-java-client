// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.StreamConsumer.getStoredOffsetSafely;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.NoOffsetException;
import java.time.Duration;
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
}
