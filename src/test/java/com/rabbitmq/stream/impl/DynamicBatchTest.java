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

import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

public class DynamicBatchTest {

  private static void simulateActivity(long duration) {
    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static void printHistogram(Histogram histogram) {
    Locale locale = Locale.getDefault();
    System.out.printf(locale, "             count = %d%n", histogram.getCount());
    Snapshot snapshot = histogram.getSnapshot();
    System.out.printf(locale, "               min = %d%n", snapshot.getMin());
    System.out.printf(locale, "               max = %d%n", snapshot.getMax());
    System.out.printf(locale, "              mean = %2.2f%n", snapshot.getMean());
    System.out.printf(locale, "            stddev = %2.2f%n", snapshot.getStdDev());
    System.out.printf(locale, "            median = %2.2f%n", snapshot.getMedian());
    System.out.printf(locale, "              75%% <= %2.2f%n", snapshot.get75thPercentile());
    System.out.printf(locale, "              95%% <= %2.2f%n", snapshot.get95thPercentile());
    System.out.printf(locale, "              98%% <= %2.2f%n", snapshot.get98thPercentile());
    System.out.printf(locale, "              99%% <= %2.2f%n", snapshot.get99thPercentile());
    System.out.printf(locale, "            99.9%% <= %2.2f%n", snapshot.get999thPercentile());
  }

  @Test
  void itemAreProcessed(TestInfo info) {
    MetricRegistry metrics = new MetricRegistry();
    Histogram batchSizeMetrics = metrics.histogram("batch-size");
    int itemCount = 3000;
    Sync sync = sync(itemCount);
    Random random = new Random();
    DynamicBatch.BatchConsumer<String> action =
        items -> {
          batchSizeMetrics.update(items.size());
          simulateActivity(random.nextInt(10) + 1);
          sync.down(items.size());
          return true;
        };
    try (DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info))) {
      RateLimiter rateLimiter = RateLimiter.create(10000);
      IntStream.range(0, itemCount)
          .forEach(
              i -> {
                rateLimiter.acquire();
                batch.add(String.valueOf(i));
              });
      assertThat(sync).completes();
      //      printHistogram(batchSizeMetrics);
    }
  }

  @Test
  void failedProcessingIsReplayed(TestInfo info) throws Exception {
    int itemCount = 10000;
    AtomicInteger collected = new AtomicInteger(0);
    AtomicInteger processed = new AtomicInteger(0);
    AtomicBoolean canProcess = new AtomicBoolean(true);
    DynamicBatch.BatchConsumer<String> action =
        items -> {
          boolean result;
          if (canProcess.get()) {
            collected.addAndGet(items.size());
            processed.addAndGet(items.size());
            result = true;
          } else {
            result = false;
          }
          return result;
        };
    try (DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info))) {
      int firstRoundCount = itemCount / 5;
      IntStream.range(0, firstRoundCount)
          .forEach(
              i -> {
                batch.add(String.valueOf(i));
              });
      waitAtMost(() -> processed.get() == firstRoundCount);
      canProcess.set(false);
      IntStream.range(firstRoundCount, itemCount)
          .forEach(
              i -> {
                batch.add(String.valueOf(i));
              });
      canProcess.set(true);
      waitAtMost(() -> processed.get() == itemCount);
      waitAtMost(() -> collected.get() == itemCount);
    }
  }

  @Test
  void lowThrottlingValueShouldStillHighPublishingRate(TestInfo info) throws Exception {
    int batchSize = 10;
    Semaphore semaphore = new Semaphore(batchSize);
    DynamicBatch.BatchConsumer<Long> action =
        items -> {
          semaphore.release(items.size());
          return true;
        };

    try (DynamicBatch<Long> batch = new DynamicBatch<>(action, batchSize, 10_000, batchId(info))) {
      MetricRegistry metrics = new MetricRegistry();
      Meter rate = metrics.meter("publishing-rate");
      AtomicBoolean keepGoing = new AtomicBoolean(true);
      AtomicLong sequence = new AtomicLong();
      new Thread(
              () -> {
                while (keepGoing.get() && !Thread.interrupted()) {
                  long id = sequence.getAndIncrement();
                  if (semaphore.tryAcquire()) {
                    batch.add(id);
                    rate.mark();
                  }
                }
              })
          .start();
      long start = System.nanoTime();
      waitAtMost(
          () ->
              System.nanoTime() - start > TimeUnit.SECONDS.toNanos(1) && rate.getMeanRate() > 1000);
    }
  }

  private static String batchId(TestInfo info) {
    return info.getTestMethod().get().getName();
  }
}
