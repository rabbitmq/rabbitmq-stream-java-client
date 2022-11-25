// Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import io.micrometer.core.instrument.util.NamedThreadFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ScheduledExecutorServiceWrapper implements ScheduledExecutorService {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ScheduledExecutorServiceWrapper.class);
  private final ScheduledExecutorService delegate;
  private final Set<Task> tasks = ConcurrentHashMap.newKeySet();
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          new NamedThreadFactory("rabbitmq-stream-scheduled-executor-service-wrapper-"));

  ScheduledExecutorServiceWrapper(ScheduledExecutorService delegate) {
    this.delegate = delegate;
    Duration period = Duration.ofSeconds(10);
    this.scheduler.scheduleAtFixedRate(
        () -> {
          LOGGER.debug("Background scheduled task check, {} task(s) submitted", this.tasks.size());
          try {
            long now = System.nanoTime();
            Duration warningTimeout = Duration.ofSeconds(60);
            int cleanedCount = 0;
            Iterator<Task> iterator = this.tasks.iterator();
            while (iterator.hasNext()) {
              Task task = iterator.next();
              if (task.isCompleted()) {
                iterator.remove();
                cleanedCount++;
              } else {
                Duration elapsed = task.elapsedTime(now);
                if (elapsed.compareTo(warningTimeout) > 0) {
                  LOGGER.debug(
                      "Warning: task {} has been running for {} second(s)",
                      task.description,
                      elapsed.getSeconds());
                }
              }
            }
            LOGGER.debug("{} completed task(s) cleaned", cleanedCount);
          } catch (Exception e) {
            LOGGER.debug("Error during background scheduled task check", e.getMessage());
          }
        },
        period.toMillis(),
        period.toMillis(),
        TimeUnit.MILLISECONDS);
  }

  private void task(Runnable command, Future<?> future) {
    task(command.toString(), future);
  }

  private void task(Callable<?> callable, Future<?> future) {
    task(callable.toString(), future);
  }

  private void task(String description, Future<?> future) {
    this.tasks.add(new Task(description, future));
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    ScheduledFuture<?> future = this.delegate.schedule(command, delay, unit);
    task(command, future);
    return future;
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    ScheduledFuture<V> future = this.delegate.schedule(callable, delay, unit);
    task(callable, future);
    return future;
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    // we don't track these, because they are expected to run for a long time
    LOGGER.debug("Registering scheduled at fixed rate task '%s'", command.toString());
    return this.delegate.scheduleAtFixedRate(command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    // we don't track these, because they are expected to run for a long time
    LOGGER.debug("Registering scheduled with fixed delay task '%s'", command.toString());
    return this.delegate.scheduleWithFixedDelay(command, initialDelay, delay, unit);
  }

  @Override
  public void shutdown() {
    this.delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return this.delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return this.delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return this.delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    Future<T> future = this.delegate.submit(task);
    task(task, future);
    return future;
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    Future<T> future = this.delegate.submit(task, result);
    task(task, future);
    return future;
  }

  @Override
  public Future<?> submit(Runnable task) {
    Future<?> future = this.delegate.submit(task);
    task(task, future);
    return future;
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    List<? extends Callable<T>> taskList = new ArrayList<>(tasks);
    List<Future<T>> futures = this.delegate.invokeAll(taskList);
    for (int i = 0; i < taskList.size(); i++) {
      task(taskList.get(i), futures.get(i));
    }
    return futures;
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    List<? extends Callable<T>> taskList = new ArrayList<>(tasks);
    List<Future<T>> futures = this.delegate.invokeAll(taskList, timeout, unit);
    for (int i = 0; i < taskList.size(); i++) {
      task(taskList.get(i), futures.get(i));
    }
    return futures;
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void execute(Runnable command) {
    Callable<Void> callable =
        () -> {
          command.run();
          return null;
        };
    Future<Void> future = this.delegate.submit(callable);
    task(command, future);
  }

  static class Task {

    private final Future<?> future;
    private final String description;
    private final long start;

    Task(String description, Future<?> future) {
      this.description = description;
      this.future = future;
      this.start = System.nanoTime();
    }

    boolean isCompleted() {
      return this.future.isDone();
    }

    Duration elapsedTime(long now) {
      if (now - start < 0) {
        return Duration.ZERO;
      } else {
        return Duration.ofNanos(now - start);
      }
    }
  }
}
