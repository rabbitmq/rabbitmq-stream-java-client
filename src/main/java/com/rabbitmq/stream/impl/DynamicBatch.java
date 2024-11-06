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
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class DynamicBatch<T> implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicBatch.class);
  private static final int MIN_BATCH_SIZE = 32;
  private static final int MAX_BATCH_SIZE = 8192;

  private final BlockingQueue<T> requests = new LinkedBlockingQueue<>();
  private final Predicate<List<T>> consumer;
  private final int configuredBatchSize;
  private final Thread thread;

  DynamicBatch(Predicate<List<T>> consumer, int batchSize) {
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
    int batchSize = this.configuredBatchSize;
    List<T> batch = new ArrayList<>(batchSize);
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
        batch.add(item);
        if (batch.size() >= batchSize) {
          if (this.completeBatch(batch)) {
            batchSize = min(batchSize * 2, MAX_BATCH_SIZE);
            batch = new ArrayList<>(batchSize);
          }
        } else {
          item = this.requests.poll();
          if (item == null) {
            if (this.completeBatch(batch)) {
              batchSize = max(batchSize / 2, MIN_BATCH_SIZE);
              batch = new ArrayList<>(batchSize);
            }
          } else {
            batch.add(item);
            if (batch.size() >= batchSize) {
              if (this.completeBatch(batch)) {
                batchSize = min(batchSize * 2, MAX_BATCH_SIZE);
                batch = new ArrayList<>(batchSize);
              }
            }
          }
        }
      } else {
        if (this.completeBatch(batch)) {
          batchSize = min(batchSize * 2, MAX_BATCH_SIZE);
          batch = new ArrayList<>(batchSize);
        }
      }
    }
  }

  private boolean completeBatch(List<T> items) {
    try {
      return this.consumer.test(items);
    } catch (Exception e) {
      LOGGER.warn("Error during dynamic batch completion: {}", e.getMessage());
      return false;
    }
  }

  @Override
  public void close() {
    this.thread.interrupt();
  }
}
