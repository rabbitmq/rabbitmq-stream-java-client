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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class DynamicBatchTest {

  @Test
  void test() {
    MetricRegistry metrics = new MetricRegistry();
    Histogram batchSizeMetrics = metrics.histogram("batch-size");
    ConsoleReporter reporter =
        ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();

    int itemCount = 3000;
    TestUtils.Sync sync = TestUtils.sync(itemCount);
    Random random = new Random();
    DynamicBatch<String> batch =
        new DynamicBatch<>(
            items -> {
              batchSizeMetrics.update(items.size());
              sync.down(items.size());
              try {
                Thread.sleep(random.nextInt(10) + 1);
              } catch (InterruptedException e) {
                throw new RuntimeException(e);
              }
              return true;
            },
            100);
    RateLimiter rateLimiter = RateLimiter.create(3000);
    long start = System.nanoTime();
    IntStream.range(0, itemCount)
        .forEach(
            i -> {
              rateLimiter.acquire();
              batch.add(String.valueOf(i));
            });
    Assertions.assertThat(sync).completes();
    long end = System.nanoTime();
    //    System.out.println("Done in " + Duration.ofNanos(end - start));
    //    reporter.report();
  }
}
