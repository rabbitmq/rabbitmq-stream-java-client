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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;

class AsyncRetry<V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRetry.class);

    private final Callable<V> task;
    private final String description;
    private final ScheduledExecutorService scheduler;
    private final Function<Integer, Duration> delayPolicy;
    private final BooleanSupplier hasTimedOut;
    private final Predicate<Exception> retry;
    private final CompletableFuture<V> completableFuture;

    // FIXME consider supporting a timeout with the delayPolicy/BackOffDelayPolicy
    // the policy could return a Duration.of(Long.MAX_VALUE) and that would be the condition to stop
    private AsyncRetry(Callable<V> task, String description,
                       ScheduledExecutorService scheduler,
                       Function<Integer, Duration> delayPolicy, Duration timeout,
                       Predicate<Exception> retry) {
        this.task = task;
        this.description = description;
        this.scheduler = scheduler;
        this.delayPolicy = delayPolicy;
        if (timeout.isZero()) {
            this.hasTimedOut = () -> false;
        } else {
            long started = System.nanoTime();
            long timeoutInNanos = timeout.toNanos();
            this.hasTimedOut = () -> System.nanoTime() - started > timeoutInNanos;
        }
        this.retry = retry;

        this.completableFuture = new CompletableFuture<>();
        AtomicReference<Runnable> retryableTaskReference = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger(0);
        Runnable retryableTask = () -> {
            try {
                V result = task.call();
                LOGGER.debug("Task '{}' succeeded, completing future", description);
                completableFuture.complete(result);
            } catch (Exception e) {
                if (retry.test(e)) {
                    if (hasTimedOut.getAsBoolean()) {
                        LOGGER.debug("Retryable attempts for task '{}' timed out, failing future", description);
                        this.completableFuture.completeExceptionally(new RetryTimeoutException());
                    } else {
                        LOGGER.debug("Retryable exception for task '{}', scheduling another attempt", description);
                        scheduler.schedule(retryableTaskReference.get(),
                                delayPolicy.apply(attempts.getAndIncrement()).toMillis(), TimeUnit.MILLISECONDS);
                    }
                } else {
                    LOGGER.debug("Non-retryable exception for task '{}', failing future", description);
                    this.completableFuture.completeExceptionally(e);
                }
            }
        };
        retryableTaskReference.set(retryableTask);
        Duration initialDelay = delayPolicy.apply(attempts.getAndIncrement());
        if (initialDelay.isZero()) {
            retryableTask.run();
        } else {
            scheduler.schedule(retryableTaskReference.get(), initialDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    static <V> AsyncRetryBuilder<V> asyncRetry(Callable<V> task) {
        return new AsyncRetryBuilder<>(task);
    }

    static Function<Integer, Duration> fixed(Duration delay) {
        return new FixedWithInitialDelayPolicy(Duration.ZERO, delay);
    }

    static Function<Integer, Duration> fixedWithInitialyDelay(Duration initialDelay, Duration delay) {
        return new FixedWithInitialDelayPolicy(initialDelay, delay);
    }

    static class AsyncRetryBuilder<V> {

        private final Callable<V> task;
        private String description = "";
        private ScheduledExecutorService scheduler;
        private Function<Integer, Duration> delayPolicy = fixed(Duration.ofSeconds(1));
        private Duration timeout = Duration.ZERO;
        private Predicate<Exception> retry = e -> true;

        AsyncRetryBuilder(Callable<V> task) {
            this.task = task;
        }

        AsyncRetryBuilder<V> scheduler(ScheduledExecutorService scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        AsyncRetryBuilder<V> delay(Duration delay) {
            this.delayPolicy = fixed(delay);
            return this;
        }

        AsyncRetryBuilder<V> delayPolicy(Function<Integer, Duration> delayPolicy) {
            this.delayPolicy = delayPolicy;
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

        AsyncRetryBuilder<V> description(String description) {
            this.description = description;
            return this;
        }

        CompletableFuture<V> build() {
            return new AsyncRetry<>(task, description, scheduler, delayPolicy, timeout, retry).completableFuture;
        }

    }

    static class RetryTimeoutException extends RuntimeException {

    }

    private static class FixedWithInitialDelayPolicy implements Function<Integer, Duration> {

        private final Duration initialDelay;
        private final Duration delay;
        private final AtomicBoolean first = new AtomicBoolean(true);

        private FixedWithInitialDelayPolicy(Duration initialDelay, Duration delay) {
            this.initialDelay = initialDelay;
            this.delay = delay;
        }

        @Override
        public Duration apply(Integer integer) {
            if (first.compareAndSet(true, false)) {
                return initialDelay;
            } else {
                return delay;
            }
        }
    }

}
