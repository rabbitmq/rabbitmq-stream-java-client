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

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

class AsyncRetry<V> {

    private final Callable<V> task;
    private final ScheduledExecutorService scheduler;
    private final Duration delay;
    private final long timeoutInNanos;
    private final Predicate<Exception> retry;
    private final CompletableFuture<V> completableFuture;

    AsyncRetry(Callable<V> task, ScheduledExecutorService scheduler, Duration delay, Duration timeout,
               Predicate<Exception> retry) {
        this.task = task;
        this.scheduler = scheduler;
        this.delay = delay;
        this.timeoutInNanos = timeout.toNanos();
        this.retry = retry;

        this.completableFuture = new CompletableFuture<>();
        AtomicReference<Runnable> retryableTaskReference = new AtomicReference<>();
        long started = System.nanoTime();
        Runnable retryableTask = () -> {
            try {
                V result = task.call();
                completableFuture.complete(result);
            } catch (Exception e) {
                if (retry.test(e)) {
                    if (System.nanoTime() - started > timeoutInNanos) {
                        this.completableFuture.completeExceptionally(new RetryTimeoutException());
                    } else {
                        scheduler.schedule(retryableTaskReference.get(), delay.toMillis(), TimeUnit.MILLISECONDS);
                    }
                } else {
                    this.completableFuture.completeExceptionally(e);
                }
            }
        };
        retryableTaskReference.set(retryableTask);
        retryableTask.run();
    }

    static <V> AsyncRetryBuilder<V> asyncRetry(Callable<V> task) {
        return new AsyncRetryBuilder<>(task);
    }

    static class AsyncRetryBuilder<V> {

        private final Callable<V> task;
        private ScheduledExecutorService scheduler;
        private Duration delay = Duration.ofSeconds(1);
        private Duration timeout = Duration.ofSeconds(10);
        private Predicate<Exception> retry = e -> true;

        AsyncRetryBuilder(Callable<V> task) {
            this.task = task;
        }

        AsyncRetryBuilder<V> scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        AsyncRetryBuilder<V> delay(Duration delay) {
            this.delay = delay;
            return this;
        }

        AsyncRetryBuilder<V> timeout(Duration timeout) {
            this.timeout = timeout;
            return this;
        }

        AsyncRetryBuilder<V> retry(Predicate<Exception> predicate) {
            this.retry = predicate;
            return this;
        }

        CompletableFuture<V> build() {
            return new AsyncRetry<>(task, scheduler, delay, timeout, retry).completableFuture;
        }

    }

    static class RetryTimeoutException extends RuntimeException {

    }

}
