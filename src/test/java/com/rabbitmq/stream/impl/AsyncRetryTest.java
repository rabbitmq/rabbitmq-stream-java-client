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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class AsyncRetryTest {

    ScheduledExecutorService scheduler;
    @Mock
    Callable<Integer> task;

    @BeforeEach
    void init() {
        MockitoAnnotations.initMocks(this);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
    }

    @AfterEach
    void tearDown() {
        this.scheduler.shutdownNow();
    }

    @Test
    void callbackCalledIfCompletedImmediately() throws Exception {
        when(task.call()).thenReturn(42);
        CompletableFuture<Integer> completableFuture = AsyncRetry.asyncRetry(task).scheduler(scheduler).build();
        AtomicInteger result = new AtomicInteger(0);
        completableFuture.thenAccept(value -> result.set(value));
        assertThat(result.get()).isEqualTo(42);
        verify(task, times(1)).call();
    }

    @Test
    void shouldRetryWhenExecutionFails() throws Exception {
        when(task.call())
                .thenThrow(new RuntimeException())
                .thenThrow(new RuntimeException())
                .thenReturn(42);
        CompletableFuture<Integer> completableFuture = AsyncRetry.asyncRetry(task)
                .scheduler(scheduler)
                .delay(Duration.ofMillis(50))
                .timeout(Duration.ofSeconds(1))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(0);
        completableFuture.thenAccept(value -> {
            result.set(value);
            latch.countDown();
        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).isEqualTo(42);
        verify(task, times(3)).call();
    }

    @Test
    void shouldTimeoutWhenExecutionFailsForTooLong() throws Exception {
        when(task.call())
                .thenThrow(new RuntimeException());
        CompletableFuture<Integer> completableFuture = AsyncRetry.asyncRetry(task)
                .scheduler(scheduler)
                .delay(Duration.ofMillis(50))
                .timeout(Duration.ofMillis(500))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean acceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        completableFuture.thenAccept(value -> {
            acceptCalled.set(true);
        }).exceptionally(e -> {
            exceptionallyCalled.set(true);
            latch.countDown();
            return null;
        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(acceptCalled.get()).isFalse();
        assertThat(exceptionallyCalled.get()).isTrue();
        verify(task, atLeast(5)).call();
    }

    @Test
    void shouldRetryWhenPredicateAllowsIt() throws Exception {
        when(task.call())
                .thenThrow(new IllegalStateException())
                .thenThrow(new IllegalStateException())
                .thenReturn(42);
        CompletableFuture<Integer> completableFuture = AsyncRetry.asyncRetry(task)
                .scheduler(scheduler)
                .retry(e -> e instanceof IllegalStateException)
                .delay(Duration.ofMillis(50))
                .timeout(Duration.ofSeconds(1))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger(0);
        completableFuture.thenAccept(value -> {
            result.set(value);
            latch.countDown();
        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).isEqualTo(42);
        verify(task, times(3)).call();
    }

    @Test
    void shouldFailWhenPredicateDoesNotAllowRetry() throws Exception {
        when(task.call())
                .thenThrow(new IllegalStateException())
                .thenThrow(new IllegalStateException())
                .thenThrow(new IllegalArgumentException());
        CompletableFuture<Integer> completableFuture = AsyncRetry.asyncRetry(task)
                .scheduler(scheduler)
                .retry(e -> !(e instanceof IllegalArgumentException))
                .delay(Duration.ofMillis(50))
                .timeout(Duration.ofSeconds(50))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean acceptCalled = new AtomicBoolean(false);
        AtomicBoolean exceptionallyCalled = new AtomicBoolean(false);
        completableFuture.thenAccept(value -> {
            acceptCalled.set(true);
        }).exceptionally(e -> {
            exceptionallyCalled.set(true);
            latch.countDown();
            return null;
        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(acceptCalled.get()).isFalse();
        assertThat(exceptionallyCalled.get()).isTrue();
        verify(task, times(3)).call();
    }
}
