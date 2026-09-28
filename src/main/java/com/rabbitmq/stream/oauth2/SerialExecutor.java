// Copyright (c) 2024-2026 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.oauth2;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs submitted tasks one at a time, in submission order, on top of a delegate {@link Executor}.
 *
 * <p>Has no thread of its own. State confined to tasks run through the same instance can use plain
 * fields: the queue and the scheduling flag provide the happens-before relationship between
 * consecutive tasks.
 */
final class SerialExecutor implements Executor {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerialExecutor.class);
  private static final int MAX_BATCH = 64;

  private final Executor delegate;
  private final Queue<Runnable> tasks = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean scheduled = new AtomicBoolean(false);
  private volatile Thread runner;

  SerialExecutor(Executor delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public void execute(Runnable task) {
    tasks.add(Objects.requireNonNull(task));
    schedule();
  }

  boolean inExecutor() {
    return runner == Thread.currentThread();
  }

  private void schedule() {
    if (scheduled.compareAndSet(false, true)) {
      try {
        delegate.execute(this::drain);
      } catch (RejectedExecutionException e) {
        scheduled.set(false);
        throw e;
      }
    }
  }

  private void drain() {
    runner = Thread.currentThread();
    try {
      Runnable task;
      int count = 0;
      while (count++ < MAX_BATCH && (task = tasks.poll()) != null) {
        try {
          task.run();
        } catch (Throwable t) {
          LOGGER.warn("Error in serial executor task", t);
        }
      }
    } finally {
      runner = null;
      scheduled.set(false);
      if (!tasks.isEmpty()) {
        try {
          schedule();
        } catch (RejectedExecutionException e) {
          LOGGER.debug("Could not reschedule serial executor, delegate rejected it", e);
        }
      }
    }
  }
}
