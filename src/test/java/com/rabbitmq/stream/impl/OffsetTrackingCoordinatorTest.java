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
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OffsetTrackingCoordinatorTest {

  @Mock StreamEnvironment env;
  @Mock StreamConsumer consumer;

  ScheduledExecutorService executorService;

  AutoCloseable mocks;

  OffsetTrackingCoordinator coordinator;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    executorService = Executors.newScheduledThreadPool(2);
    when(env.scheduledExecutorService()).thenReturn(executorService);
    when(consumer.isOpen()).thenReturn(true);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (coordinator != null) {
      coordinator.close();
    }
    if (executorService != null) {
      executorService.shutdownNow();
    }
    mocks.close();
  }

  @Test
  void needStoreRegistration() {
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env);
    assertThat(
            coordinator.needTrackingRegistration(
                new TrackingConfiguration(false, false, -1, Duration.ZERO, Duration.ZERO)))
        .as("tracking is disabled, no registration needed")
        .isFalse();
    assertThat(
            coordinator.needTrackingRegistration(
                new TrackingConfiguration(true, true, 100, Duration.ofSeconds(5), Duration.ZERO)))
        .as("auto tracking enabled, registration needed")
        .isTrue();
    assertThat(
            coordinator.needTrackingRegistration(
                new TrackingConfiguration(true, false, -1, Duration.ZERO, Duration.ofSeconds(5))))
        .as("manual tracking with check interval, registration needed")
        .isTrue();
    assertThat(
            coordinator.needTrackingRegistration(
                new TrackingConfiguration(true, false, -1, Duration.ZERO, Duration.ZERO)))
        .as("manual tracking without check interval, no registration needed")
        .isFalse();
  }

  @Test
  void autoShouldNotStoreIfNoMessagesArrive() throws Exception {
    Duration checkInterval = Duration.ofMillis(10);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    coordinator.registerTrackingConsumer(
        consumer,
        new TrackingConfiguration(true, true, 100, Duration.ofMillis(200), Duration.ZERO));

    Thread.sleep(3 * checkInterval.toMillis());
    verify(consumer, never()).store(anyLong());
  }

  @Test
  void autoShouldStoreAfterSomeInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).store(anyLong());

    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(true, true, 1, Duration.ofMillis(200), Duration.ZERO))
            .postMessageProcessingCallback();

    postProcessedMessageCallback.accept(context(1, () -> {}));

    assertThat(latchAssert(flushLatch)).completes(5);
  }

  @Test
  void autoShouldStoreFixedMessageCountAndAutoTrackingAfterInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).store(anyLong());

    int messageInterval = 100;
    int messageCount = 5 * messageInterval + messageInterval / 5;

    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(
                    true, true, messageInterval, Duration.ofMillis(200), Duration.ZERO))
            .postMessageProcessingCallback();

    AtomicInteger storedCountAfterProcessing = new AtomicInteger(0);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              postProcessedMessageCallback.accept(
                  context(i, () -> storedCountAfterProcessing.incrementAndGet()));
            });

    assertThat(latchAssert(flushLatch)).completes(5);
    assertThat(storedCountAfterProcessing.get()).isEqualTo(messageCount / messageInterval);
  }

  @Test
  void autoShouldNotStoreIfOffsetAlreadyStored() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    long storedOffset = 10;

    when(consumer.lastStoredOffset()).thenReturn(storedOffset);

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(true, true, 1, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    postProcessedMessageCallback.accept(context(10, () -> {}));

    Thread.sleep(autoFlushInterval.multipliedBy(4).toMillis());
    verify(consumer, never()).store(anyLong());
  }

  @Test
  void autoShouldNotFlushAfterInactivityIfLastStoreIsOnModulo() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    int storeEvery = 10;

    when(consumer.lastStoredOffset()).thenReturn((long) storeEvery - 1);

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(true, true, storeEvery, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    IntStream.range(0, storeEvery)
        .forEach(
            offset -> {
              postProcessedMessageCallback.accept(context(offset, () -> {}));
            });

    Thread.sleep(autoFlushInterval.multipliedBy(4).toMillis());
    verify(consumer, never()).store(anyLong());
  }

  @Test
  void autoShouldStoreLastProcessedAfterInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    int storeEvery = 10;
    int extraMessages = 3;

    long expectedLastStoredOffset = storeEvery + extraMessages - 1;
    when(consumer.lastStoredOffset()).thenReturn((long) (storeEvery - 1));

    ArgumentCaptor<Long> lastStoredOffsetCaptor = ArgumentCaptor.forClass(Long.class);
    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown()))
        .when(consumer)
        .store(lastStoredOffsetCaptor.capture());

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(true, true, storeEvery, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    IntStream.range(0, storeEvery + extraMessages)
        .forEach(
            offset -> {
              postProcessedMessageCallback.accept(context(offset, () -> {}));
            });

    assertThat(latchAssert(flushLatch)).completes(5);
    verify(consumer, times(1)).store(anyLong());
    assertThat(lastStoredOffsetCaptor.getValue()).isEqualTo(expectedLastStoredOffset);
  }

  @Test
  void manualShouldNotStoreIfAlreadyUpToDate() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    long lastStoredOffset = 50;
    when(consumer.lastStoredOffset()).thenReturn(lastStoredOffset);

    LongConsumer storeCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(
                    true, false, -1, Duration.ZERO, checkInterval.multipliedBy(2)))
            .trackingCallback();

    storeCallback.accept(lastStoredOffset);

    Thread.sleep(3 * checkInterval.toMillis());

    verify(consumer, never()).store(anyLong());
  }

  @Test
  void manualShouldStoreIfRequestedStoredOffsetIsBehind() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetTrackingCoordinator coordinator = new OffsetTrackingCoordinator(env, checkInterval);

    long lastRequestedOffset = 50;
    long lastStoredOffset = 40;
    when(consumer.lastStoredOffset()).thenReturn(lastStoredOffset);

    ArgumentCaptor<Long> lastStoredOffsetCaptor = ArgumentCaptor.forClass(Long.class);
    CountDownLatch storeLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> storeLatch.countDown()))
        .when(consumer)
        .store(lastStoredOffsetCaptor.capture());

    LongConsumer storeCallback =
        coordinator
            .registerTrackingConsumer(
                consumer,
                new TrackingConfiguration(
                    true, false, -1, Duration.ZERO, checkInterval.multipliedBy(2)))
            .trackingCallback();

    storeCallback.accept(lastRequestedOffset);

    assertThat(latchAssert(storeLatch)).completes(5);

    verify(consumer, times(1)).store(anyLong());
  }

  Context context(long offset, Runnable action) {
    return new Context() {
      @Override
      public long offset() {
        return offset;
      }

      @Override
      public void storeOffset() {
        action.run();
      }

      @Override
      public com.rabbitmq.stream.Consumer consumer() {
        return consumer;
      }
    };
  }
}
