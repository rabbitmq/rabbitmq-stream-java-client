// Copyright (c) 2025-2026 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.StreamException;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-threaded, single-writer event loop used to serialize access to control-plane state.
 *
 * <p>Unlike a request/reply RPC dispatcher, {@link Client#submit(Consumer)} never blocks the
 * caller: connection and stream events are posted from netty I/O threads, and those threads must
 * never wait on this loop. {@link Client#query(Function)} is the only blocking entry point, kept
 * for the handful of callers that genuinely need a result back, and is bounded by the timeout
 * passed to the constructor.
 */
final class EventLoop implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

  private final Duration timeout;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final EventExecutor eventExecutor;
  private final Map<Long, Object> activeClients = new HashMap<>();

  EventLoop(EventExecutorGroup eventExecutorGroup, Duration timeout) {
    this.eventExecutor = eventExecutorGroup.next();
    this.timeout = timeout;
  }

  <S> Client<S> register(Supplier<S> stateSupplier) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    Client<S> client = new Client<>(this);
    CountDownLatch latch = new CountDownLatch(1);

    eventExecutor.execute(
        () -> {
          try {
            S state = stateSupplier.get();
            activeClients.put(client.id, state);
          } catch (Exception e) {
            LOGGER.warn("Error during event loop client registration", e);
          } finally {
            latch.countDown();
          }
        });

    awaitLatch(latch, "Event loop registration");
    return client;
  }

  private <S> void submit(Client<S> client, Function<S, TaskResult> task) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    if (eventExecutor.inEventLoop()) {
      executeTask(client, task);
    } else {
      eventExecutor.execute(() -> executeTask(client, task));
    }
  }

  private <S, R> R query(Client<S> client, Function<S, R> queryFunction) {
    if (this.closed.get()) {
      throw new IllegalStateException("Event loop is closed");
    }
    AtomicReference<R> result = new AtomicReference<>();
    Function<S, TaskResult> task =
        s -> {
          result.set(queryFunction.apply(s));
          return TaskResult.CONTINUE;
        };
    if (eventExecutor.inEventLoop()) {
      // already on the loop thread, e.g. a query triggered by another task: run in line,
      // waiting on the latch below would deadlock
      executeTask(client, task);
      return result.get();
    }
    CountDownLatch latch = new CountDownLatch(1);
    eventExecutor.execute(
        () -> {
          try {
            executeTask(client, task);
          } finally {
            latch.countDown();
          }
        });
    awaitLatch(latch, "Event loop query");
    return result.get();
  }

  private void awaitLatch(CountDownLatch latch, String label) {
    try {
      boolean completed = latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      if (!completed) {
        throw new StreamException(
            "%s did not complete in %d second(s)", label, timeout.toSeconds());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StreamException(label + " processing has been interrupted", e);
    }
  }

  private <S> void executeTask(Client<S> client, Function<S, TaskResult> task) {
    if (!client.closed.get()) {
      try {
        @SuppressWarnings("unchecked")
        S clientState = (S) activeClients.get(client.id);
        if (clientState != null) {
          TaskResult result = task.apply(clientState);
          if (result == TaskResult.STOP) {
            activeClients.remove(client.id);
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Error during event loop task", e);
      }
    }
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      eventExecutor.execute(activeClients::clear);
    }
  }

  private enum TaskResult {
    CONTINUE,
    STOP
  }

  private static final AtomicLong CLIENT_ID_SEQUENCE = new AtomicLong();

  static class Client<S> implements AutoCloseable {

    private final long id;
    private final EventLoop loop;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private Client(EventLoop loop) {
      this.id = CLIENT_ID_SEQUENCE.getAndIncrement();
      this.loop = loop;
    }

    void submit(Consumer<S> task) {
      if (this.closed.get()) {
        throw new IllegalStateException("Client is closed");
      }
      this.loop.submit(
          this,
          s -> {
            task.accept(s);
            return TaskResult.CONTINUE;
          });
    }

    <R> R query(Function<S, R> queryFunction) {
      return this.loop.query(this, queryFunction);
    }

    @Override
    public void close() {
      if (this.closed.compareAndSet(false, true)) {
        try {
          this.loop.submit(this, s -> TaskResult.STOP);
        } catch (IllegalStateException e) {
          // event loop already closed
        }
      }
    }

    boolean isClosed() {
      return this.closed.get();
    }
  }
}
