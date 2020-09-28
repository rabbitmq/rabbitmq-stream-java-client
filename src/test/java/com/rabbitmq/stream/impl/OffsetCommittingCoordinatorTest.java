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
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OffsetCommittingCoordinatorTest {

  @Mock StreamEnvironment env;
  @Mock StreamConsumer consumer;

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
  void autoShouldNotCommitIfNoMessagesArrive() throws Exception {
    Duration checkInterval = Duration.ofMillis(10);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    coordinator.registerCommittingConsumer(
        consumer, new CommitConfiguration(true, true, 100, Duration.ofMillis(200)));

    Thread.sleep(3 * checkInterval.toMillis());
    verify(consumer, never()).commit(anyLong());
  }

  @Test
  void autoCommitAfterSomeInactivity() {
    Duration checkInterval = Duration.ofMillis(100);
    OffsetCommittingCoordinator coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).commit(anyLong());

    Consumer<Context> postProcessedMessageCallback =
        coordinator.registerCommittingConsumer(
            consumer, new CommitConfiguration(true, true, 100, Duration.ofMillis(200)));

    postProcessedMessageCallback.accept(context(1, () -> {}));

    assertThat(latchAssert(flushLatch)).completes(5);
  }

  @Test
  void autoShouldCommitFixedMessageCountAndAutoCommitAfterInactivity() {
    Duration checkInterval = Duration.ofMillis(500);
    coordinator = new OffsetCommittingCoordinator(env, checkInterval);

    CountDownLatch flushLatch = new CountDownLatch(1);
    doAnswer(answer(inv -> flushLatch.countDown())).when(consumer).commit(anyLong());

    int messageInterval = 100;
    int messageCount = 5 * messageInterval + messageInterval / 5;

    Consumer<Context> postProcessedMessageCallback =
        coordinator.registerCommittingConsumer(
            consumer,
            new CommitConfiguration(true, true, messageInterval, Duration.ofMillis(1000)));

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
