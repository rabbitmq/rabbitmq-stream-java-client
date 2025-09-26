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

import static com.rabbitmq.stream.BackOffDelayPolicy.fixedWithInitialDelay;
import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.ThreadUtils.internalThreadFactory;
import static java.time.Duration.ZERO;
import static java.time.Duration.ofMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.impl.TestUtils.Sync;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AsyncRetryTest {

  @Mock Callable<Integer> task;
  AutoCloseable mocks;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void callbackCalledIfCompletedImmediately(ScheduledExecutorService scheduler) throws Exception {
    when(task.call()).thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .delayPolicy(fixedWithInitialDelay(ZERO, ofMillis(10)))
            .scheduler(scheduler)
            .build();
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(result::set);
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(1)).call();
    scheduler.shutdownNow();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void shouldRetryWhenExecutionFails(ScheduledExecutorService scheduler) throws Exception {
    when(task.call())
        .thenThrow(new RuntimeException())
        .thenThrow(new RuntimeException())
        .thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task).scheduler(scheduler).delay(ofMillis(50)).build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(
        value -> {
          result.set(value);
          latch.countDown();
        });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(3)).call();
    scheduler.shutdownNow();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void shouldTimeoutWhenExecutionFailsForTooLong(ScheduledExecutorService scheduler)
      throws Exception {
    when(task.call()).thenThrow(new RuntimeException());
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(scheduler)
            .delayPolicy(fixedWithInitialDelay(ofMillis(50), ofMillis(50), ofMillis(500)))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean acceptCalled = new AtomicBoolean(false);
    AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
    completableFuture
        .thenAccept(
            value -> {
              acceptCalled.set(true);
            })
        .exceptionally(
            e -> {
              exceptionallyCalled.set(true);
              latch.countDown();
              return null;
            });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(acceptCalled.get()).isFalse();
    assertThat(exceptionallyCalled.get()).isTrue();
    verify(task, atLeast(5)).call();
    scheduler.shutdownNow();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void shouldRetryWhenPredicateAllowsIt(ScheduledExecutorService scheduler) throws Exception {
    when(task.call())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalStateException())
        .thenReturn(42);
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(scheduler)
            .retry(e -> e instanceof IllegalStateException)
            .delay(ofMillis(50))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger result = new AtomicInteger(0);
    completableFuture.thenAccept(
        value -> {
          result.set(value);
          latch.countDown();
        });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(result.get()).isEqualTo(42);
    verify(task, times(3)).call();
    scheduler.shutdownNow();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void shouldFailWhenPredicateDoesNotAllowRetry(ScheduledExecutorService scheduler)
      throws Exception {
    when(task.call())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalStateException())
        .thenThrow(new IllegalArgumentException());
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .scheduler(scheduler)
            .retry(e -> !(e instanceof IllegalArgumentException))
            .delay(ofMillis(50))
            .build();
    CountDownLatch latch = new CountDownLatch(1);
    AtomicBoolean acceptCalled = new AtomicBoolean(false);
    AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
    completableFuture
        .thenAccept(
            value -> {
              acceptCalled.set(true);
            })
        .exceptionally(
            e -> {
              exceptionallyCalled.set(true);
              latch.countDown();
              return null;
            });
    assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
    assertThat(acceptCalled.get()).isFalse();
    assertThat(exceptionallyCalled.get()).isTrue();
    verify(task, times(3)).call();
    scheduler.shutdownNow();
  }

  @ParameterizedTest
  @MethodSource("schedulers")
  void completeExceptionally(ScheduledExecutorService scheduler) throws Exception {
    when(task.call()).thenThrow(new UnsupportedOperationException());
    CompletableFuture<Integer> completableFuture =
        AsyncRetry.asyncRetry(task)
            .delayPolicy(fixedWithInitialDelay(ZERO, ofMillis(10)))
            .retry(ex -> ex instanceof IllegalStateException)
            .scheduler(scheduler)
            .build();
    Sync sync = sync();
    completableFuture.handleAsync(
        (v, ex) -> {
          if (ex != null) {
            sync.down();
          }
          return null;
        },
        scheduler);
    assertThat(sync).completes();
    scheduler.shutdownNow();
  }

  static List<ScheduledExecutorService> schedulers() {
    return List.of(
        Executors.newSingleThreadScheduledExecutor(),
        Executors.newScheduledThreadPool(0, internalThreadFactory("async-retry-test-")));
  }
}
