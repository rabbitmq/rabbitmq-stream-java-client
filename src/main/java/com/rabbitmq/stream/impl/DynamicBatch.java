// Copyright (c) 2024 Broadcom. All Rights Reserved.
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

import static java.lang.Math.max;
import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DynamicBatch<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicBatch.class);
  private static final int MIN_BATCH_SIZE = 32;
  private static final int MAX_BATCH_SIZE = 8192;

  private final BlockingQueue<T> requests = new LinkedBlockingQueue<>();
  private final BiPredicate<List<T>, Boolean> consumer;
  private final int configuredBatchSize;
  private final Thread thread;

  DynamicBatch(BiPredicate<List<T>, Boolean> consumer, int batchSize) {
    this.consumer = consumer;
    this.configuredBatchSize = min(max(batchSize, MIN_BATCH_SIZE), MAX_BATCH_SIZE);
    this.thread = ConcurrencyUtils.defaultThreadFactory().newThread(this::loop);
    this.thread.start();
  }

  void add(T item) {
    try {
      requests.put(item);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void loop() {
    State<T> state = new State<>();
    state.batchSize = this.configuredBatchSize;
    state.items = new ArrayList<>(state.batchSize);
    state.retry = false;
    Thread currentThread = Thread.currentThread();
    T item;
    while (!currentThread.isInterrupted()) {
      try {
        item = this.requests.poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        currentThread.interrupt();
        return;
      }
      if (item != null) {
        state.items.add(item);
        if (state.items.size() >= state.batchSize) {
          this.maybeCompleteBatch(state, true);
        } else {
          item = this.requests.poll();
          if (item == null) {
            this.maybeCompleteBatch(state, false);
          } else {
            state.items.add(item);
            if (state.items.size() >= state.batchSize) {
              this.maybeCompleteBatch(state, true);
            }
          }
        }
      } else {
        this.maybeCompleteBatch(state, false);
      }
    }
  }

  private static final class State<T> {

    int batchSize;
    List<T> items;
    boolean retry;
  }

  private void maybeCompleteBatch(State<T> state, boolean increaseIfCompleted) {
    try {
      boolean completed = this.consumer.test(state.items, state.retry);
      if (completed) {
        if (increaseIfCompleted) {
          state.batchSize = min(state.batchSize * 2, MAX_BATCH_SIZE);
        } else {
          state.batchSize = max(state.batchSize / 2, MIN_BATCH_SIZE);
        }
        state.items = new ArrayList<>(state.batchSize);
        state.retry = false;
      } else {
        state.retry = true;
      }
    } catch (Exception e) {
      LOGGER.warn("Error during dynamic batch completion: {}", e.getMessage());
      state.retry = true;
    }
  }

  @Override
  public void close() {
    this.thread.interrupt();
  }
}
