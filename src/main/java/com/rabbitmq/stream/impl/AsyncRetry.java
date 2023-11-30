// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
// Inc. and/or its subsidiaries.
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

import static com.rabbitmq.stream.impl.Utils.namedRunnable;

import com.rabbitmq.stream.BackOffDelayPolicy;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncRetry<V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRetry.class);

  private final CompletableFuture<V> completableFuture;

  private AsyncRetry(
      Callable<V> task,
      String description,
      ScheduledExecutorService scheduler,
      BackOffDelayPolicy delayPolicy,
      Predicate<Exception> retry) {
    this.completableFuture = new CompletableFuture<>();
    AtomicReference<Runnable> retryableTaskReference = new AtomicReference<>();
    AtomicInteger attempts = new AtomicInteger(0);
    Runnable retryableTask =
        namedRunnable(
            () -> {
              if (Thread.currentThread().isInterrupted()) {
                LOGGER.debug("Task '{}' interrupted, failing future", description);
                this.completableFuture.completeExceptionally(new CancellationException());
                return;
              }
              try {
                V result = task.call();
                LOGGER.debug("Task '{}' succeeded, completing future", description);
                completableFuture.complete(result);
              } catch (Exception e) {
                int attemptCount = attempts.getAndIncrement();
                if (retry.test(e)) {
                  if (delayPolicy.delay(attemptCount).equals(BackOffDelayPolicy.TIMEOUT)) {
                    LOGGER.debug(
                        "Retryable attempts for task '{}' timed out, failing future", description);
                    this.completableFuture.completeExceptionally(new RetryTimeoutException());
                  } else {
                    LOGGER.debug(
                        "Retryable exception ({}) for task '{}', scheduling another attempt",
                        e.getClass().getSimpleName(),
                        description);
                    schedule(
                        scheduler, retryableTaskReference.get(), delayPolicy.delay(attemptCount));
                  }
                } else {
                  LOGGER.debug(
                      "Non-retryable exception for task '{}', failing future", description);
                  this.completableFuture.completeExceptionally(e);
                }
              }
            },
            description);
    retryableTaskReference.set(retryableTask);
    Duration initialDelay = delayPolicy.delay(attempts.getAndIncrement());
    LOGGER.debug("Scheduling task '{}' with policy {}", description, delayPolicy);
    if (initialDelay.isZero()) {
      retryableTask.run();
    } else {
      schedule(scheduler, retryableTaskReference.get(), initialDelay);
    }
  }

  private static void schedule(
      ScheduledExecutorService scheduler, Runnable command, Duration delay) {
    try {
      scheduler.schedule(command, delay.toMillis(), TimeUnit.MILLISECONDS);
    } catch (RuntimeException e) {
      LOGGER.debug("Error while scheduling command", e);
    }
  }

  static <V> AsyncRetryBuilder<V> asyncRetry(Callable<V> task) {
    return new AsyncRetryBuilder<>(task);
  }

  static class AsyncRetryBuilder<V> {

    private final Callable<V> task;
    private String description = "";
    private ScheduledExecutorService scheduler;
    private BackOffDelayPolicy delayPolicy = BackOffDelayPolicy.fixed(Duration.ofSeconds(1));
    private Predicate<Exception> retry = e -> true;

    AsyncRetryBuilder(Callable<V> task) {
      this.task = task;
    }

    AsyncRetryBuilder<V> scheduler(ScheduledExecutorService scheduler) {
      this.scheduler = scheduler;
      return this;
    }

    AsyncRetryBuilder<V> delay(Duration delay) {
      this.delayPolicy = BackOffDelayPolicy.fixed(delay);
      return this;
    }

    AsyncRetryBuilder<V> delayPolicy(BackOffDelayPolicy delayPolicy) {
      this.delayPolicy = delayPolicy;
      return this;
    }

    AsyncRetryBuilder<V> retry(Predicate<Exception> predicate) {
      this.retry = predicate;
      return this;
    }

    AsyncRetryBuilder<V> description(String description, Object... args) {
      this.description = String.format(description, args);
      return this;
    }

    CompletableFuture<V> build() {
      return new AsyncRetry<>(task, description, scheduler, delayPolicy, retry).completableFuture;
    }
  }

  static class RetryTimeoutException extends RuntimeException {}
}
