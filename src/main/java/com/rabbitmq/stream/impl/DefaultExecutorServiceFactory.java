// Copyright (c) 2023-2024 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultExecutorServiceFactory implements ExecutorServiceFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultExecutorServiceFactory.class);
  private static final Comparator<Executor> EXECUTOR_COMPARATOR =
      Comparator.comparingInt(Executor::usage);

  private final List<Executor> executors;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ThreadFactory threadFactory;
  private final int minSize;
  private final int clientPerExecutor;
  private final Supplier<Executor> executorFactory;

  DefaultExecutorServiceFactory(int minSize, int clientPerExecutor, String prefix) {
    this.minSize = minSize;
    this.clientPerExecutor = clientPerExecutor;
    this.threadFactory = threadFactory(prefix);
    this.executorFactory = () -> newExecutor();
    List<Executor> l = new ArrayList<>(this.minSize);
    IntStream.range(0, this.minSize).forEach(ignored -> l.add(this.executorFactory.get()));
    executors = new CopyOnWriteArrayList<>(l);
  }

  static void maybeResize(
      List<Executor> current, int min, int clientsPerResource, Supplier<Executor> factory) {
    LOGGER.debug(
        "Resizing {}, with min = {}, clients per resource = {}", current, min, clientsPerResource);
    int clientCount = 0;
    for (Executor executor : current) {
      clientCount += executor.usage();
    }
    LOGGER.debug("Total usage is {}", clientCount);

    int target = Math.max((clientCount / clientsPerResource) + 1, min);
    LOGGER.debug("Target size is {}, current size is {}", target, current.size());
    if (target > current.size()) {
      LOGGER.debug("Upsizing...");
      List<Executor> l = new ArrayList<>();
      for (int i = 0; i < target; i++) {
        if (i < current.size()) {
          l.add(current.get(i));
        } else {
          l.add(factory.get());
        }
      }
      current.clear();
      current.addAll(l);
      LOGGER.debug("New list is {}", current);
    } else if (target < current.size()) {
      LOGGER.debug("Downsizing...");
      boolean hasUnusedExecutors = current.stream().filter(ex -> ex.usage() == 0).count() > 0;
      if (!hasUnusedExecutors) {
        LOGGER.debug("No downsizing, there is no unused executor");
      }
      if (hasUnusedExecutors) {
        List<Executor> l = new ArrayList<>(target);
        for (int i = 0; i < current.size(); i++) {
          Executor executor = current.get(i);
          if (executor.usage() == 0) {
            executor.close();
          } else {
            l.add(executor);
          }
        }
        if (l.size() < target) {
          for (int i = l.size(); i < target; i++) {
            l.add(factory.get());
          }
        }
        current.clear();
        current.addAll(l);
        LOGGER.debug("New list is {}", current);
      }
    }
  }

  private Executor newExecutor() {
    return new Executor(Executors.newSingleThreadExecutor(threadFactory));
  }

  @Override
  public synchronized ExecutorService get() {
    if (closed.get()) {
      throw new IllegalStateException("Executor service factory is closed");
    } else {
      maybeResize(this.executors, this.minSize, this.clientPerExecutor, this.executorFactory);
      LOGGER.debug("Looking least used executor in {}", this.executors);
      Executor executor = this.executors.stream().min(EXECUTOR_COMPARATOR).get();
      LOGGER.debug("Least used executor is {}", executor);
      executor.incrementUsage();
      return executor.executorService;
    }
  }

  @Override
  public synchronized void clientClosed(ExecutorService executorService) {
    if (!closed.get()) {
      Executor executor = find(executorService);
      if (executor == null) {
        LOGGER.info("Could not find executor service wrapper");
      } else {
        executor.decrementUsage();
        maybeResize(this.executors, this.minSize, this.clientPerExecutor, this.executorFactory);
      }
    }
  }

  private Executor find(ExecutorService executorService) {
    for (Executor executor : this.executors) {
      if (executor.executorService.equals(executorService)) {
        return executor;
      }
    }
    return null;
  }

  @Override
  public synchronized void close() {
    if (closed.compareAndSet(false, true)) {
      this.executors.forEach(executor -> executor.executorService.shutdownNow());
    }
  }

  static class Executor {

    private final ExecutorService executorService;
    private AtomicInteger usage = new AtomicInteger(0);

    Executor(ExecutorService executorService) {
      this.executorService = executorService;
    }

    Executor incrementUsage() {
      this.usage.incrementAndGet();
      return this;
    }

    Executor decrementUsage() {
      this.usage.decrementAndGet();
      return this;
    }

    Executor addUsage(int delta) {
      this.usage.addAndGet(delta);
      return this;
    }

    Executor substractUsage(int delta) {
      this.usage.addAndGet(-delta);
      return this;
    }

    private int usage() {
      return this.usage.get();
    }

    private void close() {
      this.executorService.shutdownNow();
    }

    @Override
    public String toString() {
      return "Executor{" + "usage=" + usage.get() + '}';
    }
  }
}
