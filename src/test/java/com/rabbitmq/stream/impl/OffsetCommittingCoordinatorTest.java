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

import static com.rabbitmq.stream.impl.TestUtils.answer;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.CommitConfiguration;
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

public class OffsetCommittingCoordinatorTest {

  @Mock StreamEnvironment env;
  @Mock StreamConsumer consumer;
  @Mock Client client;

  ScheduledExecutorService executorService;

  AutoCloseable mocks;

  OffsetCommittingCoordinator coordinator;

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
  void needCommitRegistration() {
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env);
    assertThat(
            coordinator.needCommitRegistration(
                new CommitConfiguration(false, false, -1, Duration.ZERO, Duration.ZERO)))
        .as("commit is disabled, no registration needed")
        .isFalse();
    assertThat(
            coordinator.needCommitRegistration(
                new CommitConfiguration(true, true, 100, Duration.ofSeconds(5), Duration.ZERO)))
        .as("auto commit enabled, registration needed")
        .isTrue();
    assertThat(
            coordinator.needCommitRegistration(
                new CommitConfiguration(true, false, -1, Duration.ZERO, Duration.ofSeconds(5))))
        .as("manual commit with check interval, registration needed")
        .isTrue();
    assertThat(
            coordinator.needCommitRegistration(
                new CommitConfiguration(true, false, -1, Duration.ZERO, Duration.ZERO)))
        .as("manual commit without check interval, no registration needed")
        .isFalse();
  }

  @Test
  void autoShouldNotCommitIfNoMessagesArrive() throws Exception {
    Duration checkInterval = Duration.ofMillis(10);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    coordinator.registerCommittingConsumer(
        consumer, new CommitConfiguration(true, true, 100, Duration.ofMillis(200), Duration.ZERO));

    Thread.sleep(3 * checkInterval.toMillis());
    verify(consumer, never()).commit(anyLong());
  }

  @Test
  void autoShouldCommitAfterSomeInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).commit(anyLong());

    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(true, true, 1, Duration.ofMillis(200), Duration.ZERO))
            .postMessageProcessingCallback();

    postProcessedMessageCallback.accept(context(1, () -> {}));

    assertThat(latchAssert(flushLatch)).completes(5);
  }

  @Test
  void autoShouldCommitFixedMessageCountAndAutoCommitAfterInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).commit(anyLong());

    int messageInterval = 100;
    int messageCount = 5 * messageInterval + messageInterval / 5;

    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(
                    true, true, messageInterval, Duration.ofMillis(200), Duration.ZERO))
            .postMessageProcessingCallback();

    AtomicInteger committedCountAfterProcessing = new AtomicInteger(0);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              postProcessedMessageCallback.accept(
                  context(i, () -> committedCountAfterProcessing.incrementAndGet()));
            });

    assertThat(latchAssert(flushLatch)).completes(5);
    assertThat(committedCountAfterProcessing.get()).isEqualTo(messageCount / messageInterval);
  }

  @Test
  void autoShouldNotCommitIfOffsetAlreadyCommitted() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    long committedOffset = 10;

    when(consumer.lastCommittedOffset()).thenReturn(committedOffset);

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerCommittingConsumer(
                consumer, new CommitConfiguration(true, true, 1, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    postProcessedMessageCallback.accept(context(10, () -> {}));

    Thread.sleep(autoFlushInterval.multipliedBy(4).toMillis());
    verify(consumer, never()).commit(anyLong());
  }

  @Test
  void autoShouldNotFlushAfterInactivityIfLastCommitIsOnModulo() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    int commitEvery = 10;

    when(consumer.lastCommittedOffset()).thenReturn((long) commitEvery - 1);

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(true, true, commitEvery, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    IntStream.range(0, commitEvery)
        .forEach(
            offset -> {
              postProcessedMessageCallback.accept(context(offset, () -> {}));
            });

    Thread.sleep(autoFlushInterval.multipliedBy(4).toMillis());
    verify(consumer, never()).commit(anyLong());
  }

  @Test
  void autoShouldCommitLastProcessedAfterInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    int commitEvery = 10;
    int extraMessages = 3;

    long expectedLastCommittedOffset = commitEvery + extraMessages - 1;
    when(consumer.lastCommittedOffset()).thenReturn((long) (commitEvery - 1));

    ArgumentCaptor<Long> lastCommittedOffsetCaptor = ArgumentCaptor.forClass(Long.class);
    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown()))
        .when(consumer)
        .commit(lastCommittedOffsetCaptor.capture());

    Duration autoFlushInterval = Duration.ofMillis(checkInterval.toMillis() * 2);
    Consumer<Context> postProcessedMessageCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(true, true, commitEvery, autoFlushInterval, Duration.ZERO))
            .postMessageProcessingCallback();

    IntStream.range(0, commitEvery + extraMessages)
        .forEach(
            offset -> {
              postProcessedMessageCallback.accept(context(offset, () -> {}));
            });

    assertThat(latchAssert(flushLatch)).completes(5);
    verify(consumer, times(1)).commit(anyLong());
    assertThat(lastCommittedOffsetCaptor.getValue()).isEqualTo(expectedLastCommittedOffset);
  }

  @Test
  void manualShouldNotCommitIfAlreadyUpToDate() throws Exception {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    long lastCommittedOffset = 50;
    when(consumer.lastCommittedOffset()).thenReturn(lastCommittedOffset);

    LongConsumer commitCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(
                    true, false, -1, Duration.ZERO, checkInterval.multipliedBy(2)))
            .commitCallback();

    commitCallback.accept(lastCommittedOffset);

    Thread.sleep(3 * checkInterval.toMillis());

    verify(consumer, never()).commit(anyLong());
  }

  @Test
  void manualShouldCommitIfRequestedCommittedOffsetIsBehind() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    long lastRequestedOffset = 50;
    long lastCommittedOffset = 40;
    when(consumer.lastCommittedOffset()).thenReturn(lastCommittedOffset);

    ArgumentCaptor<Long> lastCommittedOffsetCaptor = ArgumentCaptor.forClass(Long.class);
    CountDownLatch commitLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> commitLatch.countDown()))
        .when(consumer)
        .commit(lastCommittedOffsetCaptor.capture());

    LongConsumer commitCallback =
        coordinator
            .registerCommittingConsumer(
                consumer,
                new CommitConfiguration(
                    true, false, -1, Duration.ZERO, checkInterval.multipliedBy(2)))
            .commitCallback();

    commitCallback.accept(lastRequestedOffset);

    assertThat(latchAssert(commitLatch)).completes(5);

    verify(consumer, times(1)).commit(anyLong());
  }

  Context context(long offset, Runnable action) {
    return new Context() {
      @Override
      public long offset() {
        return offset;
      }

      @Override
      public void commit() {
        action.run();
      }

      @Override
      public com.rabbitmq.stream.Consumer consumer() {
        return consumer;
      }
    };
  }
}
