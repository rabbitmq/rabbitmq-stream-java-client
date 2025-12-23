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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @Test
  void addShouldThrowWhenInterrupted(TestInfo info) throws Exception {
    DynamicBatch.BatchConsumer<String> action = items -> true;
    try (DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info))) {
      Thread testThread =
          new Thread(
              () -> {
                Thread.currentThread().interrupt();
                assertThatThrownBy(() -> batch.add("item"))
                    .isInstanceOf(RuntimeException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
              });
      testThread.start();
      testThread.join();
    }
  }

  @Test
  void batchSizeIncreasesWhenBatchIsFull(TestInfo info) throws Exception {
    int batchSize = 100;
    AtomicInteger maxObservedBatchSize = new AtomicInteger(0);
    AtomicInteger totalProcessed = new AtomicInteger(0);
    List<Integer> sizes = Collections.synchronizedList(new ArrayList<>());

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          maxObservedBatchSize.updateAndGet(v -> Math.max(v, items.size()));
          totalProcessed.addAndGet(items.size());
          sizes.add(items.size());
          simulateActivity(5);
          return true;
        };

    try (DynamicBatch<String> batch =
        new DynamicBatch<>(action, batchSize, 10_000, batchId(info))) {
      int itemCount = 5000;
      IntStream.range(0, itemCount).parallel().forEach(i -> batch.add("item-" + i));
      waitAtMost(() -> totalProcessed.get() == itemCount);

      assertThat(maxObservedBatchSize.get()).isGreaterThan(batchSize);
    }
  }

  @Test
  void batchSizeDecreasesWhenBatchIsNotFull(TestInfo info) throws Exception {
    int batchSize = 100;
    List<Integer> observedBatchSizes = Collections.synchronizedList(new ArrayList<>());
    AtomicInteger totalProcessed = new AtomicInteger(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          observedBatchSizes.add(items.size());
          totalProcessed.addAndGet(items.size());
          simulateActivity(20);
          return true;
        };

    try (DynamicBatch<String> batch =
        new DynamicBatch<>(action, batchSize, 10_000, batchId(info))) {
      int itemCount = 50;
      IntStream.range(0, itemCount)
          .forEach(
              i -> {
                batch.add("item-" + i);
                simulateActivity(10);
              });
      waitAtMost(() -> totalProcessed.get() == itemCount);

      assertThat(observedBatchSizes).anyMatch(s -> s < batchSize / 2);
    }
  }

  @Test
  void exceptionInProcessingIsHandledAndRetried(TestInfo info) throws Exception {
    AtomicInteger attemptCount = new AtomicInteger(0);
    AtomicInteger processedCount = new AtomicInteger(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt <= 2) {
            throw new RuntimeException("Simulated failure");
          }
          processedCount.addAndGet(items.size());
          return true;
        };

    try (DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info))) {
      batch.add("item");
      waitAtMost(() -> processedCount.get() == 1);
      assertThat(attemptCount.get()).isGreaterThan(2);
    }
  }

  @Test
  void closeProcessesRemainingItems(TestInfo info) {
    AtomicInteger processedCount = new AtomicInteger(0);
    Semaphore blockProcessing = new Semaphore(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          try {
            blockProcessing.acquire(); // Block until released
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          processedCount.addAndGet(items.size());
          return true;
        };

    DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info));

    // Add items that will queue up
    IntStream.range(0, 50).forEach(i -> batch.add("item-" + i));

    // Release processing and close
    blockProcessing.release(100);
    batch.close();

    // All items should be processed (including remaining in queue)
    assertThat(processedCount.get()).isEqualTo(50);
  }

  @Test
  void minBatchSizeCalculationWhenBatchSizeLessThanMaxUnconfirmed(TestInfo info) throws Exception {
    int batchSize = 50;
    int maxUnconfirmed = 10_000;
    List<Integer> observedSizes = Collections.synchronizedList(new ArrayList<>());

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          observedSizes.add(items.size());
          simulateActivity(10);
          return true;
        };

    try (DynamicBatch<String> batch =
        new DynamicBatch<>(action, batchSize, maxUnconfirmed, batchId(info))) {
      IntStream.range(0, 20)
          .forEach(
              i -> {
                batch.add("item-" + i);
                simulateActivity(20);
              });
      waitAtMost(() -> observedSizes.stream().mapToInt(i -> i).sum() >= 20);

      assertThat(observedSizes).anyMatch(s -> s <= 16);
    }
  }

  @Test
  void minBatchSizeCalculationWhenBatchSizeGreaterOrEqualMaxUnconfirmed(TestInfo info)
      throws Exception {
    int batchSize = 100;
    int maxUnconfirmed = 50;
    AtomicInteger processedCount = new AtomicInteger(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          processedCount.addAndGet(items.size());
          return true;
        };

    try (DynamicBatch<String> batch =
        new DynamicBatch<>(action, batchSize, maxUnconfirmed, batchId(info))) {
      IntStream.range(0, 100).forEach(i -> batch.add("item-" + i));
      waitAtMost(() -> processedCount.get() == 100);
    }

    assertThat(processedCount.get()).isEqualTo(100);
  }

  @Test
  void batchSizeDoesNotExceedMaxBatchSize(TestInfo info) throws Exception {
    int batchSize = 100;
    int maxBatchSize = batchSize * 2;
    AtomicInteger maxObservedBatchSize = new AtomicInteger(0);
    AtomicInteger totalProcessed = new AtomicInteger(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          maxObservedBatchSize.updateAndGet(v -> Math.max(v, items.size()));
          totalProcessed.addAndGet(items.size());
          return true;
        };

    try (DynamicBatch<String> batch =
        new DynamicBatch<>(action, batchSize, 10_000, batchId(info))) {
      int itemCount = 50_000;
      IntStream.range(0, itemCount).parallel().forEach(i -> batch.add("item-" + i));
      waitAtMost(() -> totalProcessed.get() == itemCount);

      assertThat(maxObservedBatchSize.get()).isLessThanOrEqualTo(maxBatchSize);
    }
  }

  @Test
  void emptyBatchIsNotProcessed(TestInfo info) throws Exception {
    AtomicInteger processCallCount = new AtomicInteger(0);

    DynamicBatch.BatchConsumer<String> action =
        items -> {
          processCallCount.incrementAndGet();
          return true;
        };

    try (DynamicBatch<String> batch = new DynamicBatch<>(action, 100, 10_000, batchId(info))) {
      Thread.sleep(300);
    }

    assertThat(processCallCount.get()).isZero();
  }

  private static String batchId(TestInfo info) {
    return info.getTestMethod().get().getName();
  }
}
