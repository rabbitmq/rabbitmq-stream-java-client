// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DynamicBatch<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicBatch.class);
  private static final int MIN_BATCH_SIZE = 16;

  private final BlockingQueue<T> requests = new LinkedBlockingQueue<>();
  private final BatchConsumer<T> consumer;
  private final int configuredBatchSize, minBatchSize, maxBatchSize;
  private final Thread thread;

  DynamicBatch(BatchConsumer<T> consumer, int batchSize, int maxUnconfirmed) {
    this.consumer = consumer;
    if (batchSize < maxUnconfirmed) {
      this.minBatchSize = min(MIN_BATCH_SIZE, batchSize / 2);
    } else {
      this.minBatchSize = min(1, maxUnconfirmed / 2);
    }
    this.configuredBatchSize = batchSize;
    this.maxBatchSize = batchSize * 2;
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
          pump(state, 2);
        }
      } else {
        this.maybeCompleteBatch(state, false);
      }
    }
  }

  private void pump(State<T> state, int pumpCount) {
    if (pumpCount <= 0) {
      return;
    }
    T item = this.requests.poll();
    if (item == null) {
      this.maybeCompleteBatch(state, false);
    } else {
      state.items.add(item);
      if (state.items.size() >= state.batchSize) {
        this.maybeCompleteBatch(state, true);
      }
      this.pump(state, pumpCount - 1);
    }
  }

  private static final class State<T> {

    int batchSize;
    List<T> items;
  }

  private void maybeCompleteBatch(State<T> state, boolean increaseIfCompleted) {
    try {
      boolean completed = this.consumer.process(state.items);
      if (completed) {
        if (increaseIfCompleted) {
          state.batchSize = min(state.batchSize * 2, this.maxBatchSize);
        } else {
          state.batchSize = max(state.batchSize / 2, this.minBatchSize);
        }
        state.items = new ArrayList<>(state.batchSize);
      }
    } catch (Exception e) {
      //      e.printStackTrace();
      LOGGER.warn("Error during dynamic batch completion: {}", e.getMessage());
    }
  }

  @Override
  public void close() {
    this.thread.interrupt();
  }

  @FunctionalInterface
  interface BatchConsumer<T> {

    boolean process(List<T> items);
  }
}
