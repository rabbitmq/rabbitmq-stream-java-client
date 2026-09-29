// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.Resource.State;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StateEventSupportTest {

  private static final State[] STATES = {OPENING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED};

  Resource resource = new Resource() {};
  ExecutorService executor;

  @BeforeEach
  void init() {
    executor = Executors.newCachedThreadPool();
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  @Test
  void eventsShouldBeDeliveredInOrder() throws Exception {
    Queue<State> states = new ConcurrentLinkedQueue<>();
    StateEventSupport support = support(List.of(ctx -> states.add(ctx.currentState())));
    List<State> expected = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      State state = STATES[i % STATES.length];
      expected.add(state);
      support.dispatch(resource, null, state);
    }
    waitAtMost(() -> states.size() == expected.size());
    assertThat(states).containsExactlyElementsOf(expected);
  }

  @Test
  void listenerExceptionShouldNotStopOtherListenersNorNextEvents() throws Exception {
    Queue<State> states = new ConcurrentLinkedQueue<>();
    StateEventSupport support =
        support(
            List.of(
                ctx -> {
                  throw new RuntimeException();
                },
                ctx -> states.add(ctx.currentState())));
    support.dispatch(resource, null, OPENING);
    support.dispatch(resource, OPENING, OPEN);
    waitAtMost(() -> states.size() == 2);
    assertThat(states).containsExactly(OPENING, OPEN);
  }

  @Test
  void blockingListenerShouldNotBlockDispatching() throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    Queue<State> states = new ConcurrentLinkedQueue<>();
    StateEventSupport support =
        support(
            List.of(
                ctx -> {
                  try {
                    release.await(10, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                  states.add(ctx.currentState());
                }));
    long start = System.nanoTime();
    support.dispatch(resource, null, OPENING);
    support.dispatch(resource, OPENING, OPEN);
    assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(5));
    assertThat(states).isEmpty();
    release.countDown();
    waitAtMost(() -> states.size() == 2);
    assertThat(states).containsExactly(OPENING, OPEN);
  }

  @Test
  void eventsDispatchedConcurrentlyShouldAllBeDeliveredOneAtATime() throws Exception {
    int threads = 8;
    int eventsPerThread = 10_000;
    AtomicInteger inFlight = new AtomicInteger();
    AtomicBoolean concurrentCall = new AtomicBoolean(false);
    AtomicInteger received = new AtomicInteger();
    StateEventSupport support =
        support(
            List.of(
                ctx -> {
                  if (inFlight.incrementAndGet() > 1) {
                    concurrentCall.set(true);
                  }
                  received.incrementAndGet();
                  inFlight.decrementAndGet();
                }));
    ExecutorService dispatchers = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      IntStream.range(0, threads)
          .forEach(
              ignored ->
                  dispatchers.execute(
                      () -> {
                        try {
                          start.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                        }
                        for (int i = 0; i < eventsPerThread; i++) {
                          support.dispatch(resource, OPEN, RECOVERING);
                        }
                      }));
      start.countDown();
      waitAtMost(() -> received.get() == threads * eventsPerThread);
    } finally {
      dispatchers.shutdownNow();
    }
    assertThat(concurrentCall).isFalse();
  }

  @Test
  void eventsShouldBeDeliveredInLineWhenExecutorRejectsTask() {
    AtomicReference<Thread> listenerThread = new AtomicReference<>();
    Queue<State> states = new ConcurrentLinkedQueue<>();
    StateEventSupport support =
        new StateEventSupport(
            List.of(
                ctx -> {
                  listenerThread.set(Thread.currentThread());
                  states.add(ctx.currentState());
                }),
            task -> {
              throw new RejectedExecutionException();
            });
    support.dispatch(resource, CLOSING, CLOSED);
    assertThat(states).containsExactly(CLOSED);
    assertThat(listenerThread).hasValue(Thread.currentThread());
    support.dispatch(resource, CLOSED, OPEN);
    assertThat(states).containsExactly(CLOSED, OPEN);
  }

  @Test
  void executorShouldNotBeUsedWithoutListeners() {
    AtomicInteger executions = new AtomicInteger();
    StateEventSupport support =
        new StateEventSupport(
            List.of(),
            task -> {
              executions.incrementAndGet();
              task.run();
            });
    support.dispatch(resource, null, OPENING);
    assertThat(executions).hasValue(0);
  }

  private StateEventSupport support(List<Resource.StateListener> listeners) {
    return new StateEventSupport(listeners, executor);
  }
}
