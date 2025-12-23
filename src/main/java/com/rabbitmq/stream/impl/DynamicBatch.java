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

  // Additive Increase, Multiplicative Decrease (AIMD) parameters
  private static final double MULTIPLICATIVE_DECREASE_FACTOR = 0.5;
  private static final int ADDITIVE_INCREASE_STEP_DIVISOR = 10; // configuredBatchSize / 10

  private static final long RETRY_DELAY_MS = 50;

  private final BlockingQueue<T> requests;
  private final BatchConsumer<T> consumer;
  private final int configuredBatchSize, minBatchSize, maxBatchSize;
  private final int additiveIncreaseStep;
  private final Thread thread;
  private volatile boolean running = true;

  DynamicBatch(BatchConsumer<T> consumer, int batchSize, int maxUnconfirmed, String id) {
    this.consumer = consumer;
    this.requests = new LinkedBlockingQueue<>(max(1, maxUnconfirmed));
    if (batchSize < maxUnconfirmed) {
      this.minBatchSize = min(MIN_BATCH_SIZE, batchSize / 2);
    } else {
      this.minBatchSize = max(1, maxUnconfirmed / 2);
    }
    this.configuredBatchSize = batchSize;
    this.maxBatchSize = batchSize * 2;

    // Calculate additive increase step: 10% of configured size, minimum 1
    this.additiveIncreaseStep = max(1, batchSize / ADDITIVE_INCREASE_STEP_DIVISOR);

    this.thread = ThreadUtils.newInternalThread(id, this::loop);
    this.thread.setDaemon(true);
    this.thread.start();
  }

  void add(T item) {
    try {
      requests.put(item);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private void loop() {
    // Initial allocation based on maxBatchSize to avoid resizing if it grows
    State<T> state = new State<>(new ArrayList<>(this.maxBatchSize));
    state.batchSize = this.configuredBatchSize;
    Thread currentThread = Thread.currentThread();
    T item;
    while (running && !currentThread.isInterrupted()) {
      try {
        item = this.requests.poll(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        currentThread.interrupt();
        return;
      }
      if (item != null) {
        state.items.add(item);
        int remaining = state.batchSize - state.items.size();
        if (remaining > 0) {
          this.requests.drainTo(state.items, remaining);
        }
        this.maybeCompleteBatch(state, state.items.size() >= state.batchSize);
      } else {
        this.maybeCompleteBatch(state, false);
      }
    }
  }

  private static final class State<T> {

    int batchSize;
    final ArrayList<T> items;

    private State(ArrayList<T> items) {
      this.items = items;
    }
  }

  private void maybeCompleteBatch(State<T> state, boolean increaseIfCompleted) {
    if (state.items.isEmpty()) {
      return;
    }

    try {
      boolean completed = this.consumer.process(state.items);
      if (completed) {
        if (increaseIfCompleted) {
          // AIMD: Additive Increase
          // Grow slowly and linearly when batch is full
          state.batchSize = min(state.batchSize + this.additiveIncreaseStep, this.maxBatchSize);
        } else {
          // AIMD: Multiplicative Decrease
          // React quickly to low utilization by cutting back
          state.batchSize =
              max((int) (state.batchSize * MULTIPLICATIVE_DECREASE_FACTOR), this.minBatchSize);
        }
        state.items.clear();
        return;
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Error during dynamic batch completion, batch size: {}, items: {}",
          state.batchSize,
          state.items.size(),
          e);
    }
    try {
      Thread.sleep(RETRY_DELAY_MS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void close() {
    this.running = false;
    this.thread.interrupt();
    try {
      this.thread.join(TimeUnit.SECONDS.toMillis(5));
      if (this.thread.isAlive()) {
        LOGGER.warn("Dynamic batch thread did not terminate within timeout");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while waiting for dynamic batch thread to terminate");
    }
    // Process any remaining items in the queue
    if (!this.requests.isEmpty()) {
      List<T> remaining = new ArrayList<>();
      this.requests.drainTo(remaining);
      if (!remaining.isEmpty()) {
        try {
          this.consumer.process(remaining);
        } catch (Exception e) {
          LOGGER.warn("Error processing remaining {} items during shutdown", remaining.size(), e);
        }
      }
    }
  }

  @FunctionalInterface
  interface BatchConsumer<T> {

    boolean process(List<T> items);
  }
}
