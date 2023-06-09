// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.benchmark;

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.metrics.DropwizardMetricsCollector;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class FilteringBenchmark {

  static final String stream = "filtering";

  public static void main(String[] args) throws Exception {
    int filterValueCount = 100;
    int filterValueSubsetCount = 40;
    int rate = 100_000;
    int filterSize = 255;
    int batchSize = 1;
    int maxUnconfirmedMessages = 1;

    Duration publishingDuration = Duration.ofSeconds(10);
    Duration publishingCycle = Duration.ofSeconds(1);

    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();
    try (Environment env = Environment.builder().build()) {
      try {
        env.deleteStream(stream);
      } catch (StreamException e) {
        // OK
      }
      env.streamCreator().stream(stream).filterSize(filterSize).create();

      List<String> filterValues = new ArrayList<>(filterValueCount);
      IntStream.range(0, filterValueCount)
          .forEach(i -> filterValues.add(UUID.randomUUID().toString()));

      AtomicLong publishedCount = new AtomicLong(0);
      AtomicLong confirmedCount = new AtomicLong(0);

      Producer producer =
          env.producerBuilder().stream(stream)
              .batchSize(batchSize)
              .maxUnconfirmedMessages(maxUnconfirmedMessages)
              .filterValue(msg -> msg.getProperties().getTo())
              .build();

      AtomicBoolean keepPublishing = new AtomicBoolean(true);
      scheduledExecutorService.schedule(
          () -> keepPublishing.set(false), publishingDuration.toMillis(), TimeUnit.MILLISECONDS);

      RateLimiter rateLimiter = RateLimiter.create(rate);

      Random random = new Random();
      ConfirmationHandler confirmationHandler = status -> confirmedCount.getAndIncrement();
      System.out.printf(
          "Starting test, filter values %s, subset %s, filter size %d%n",
          filterValueCount, filterValueSubsetCount, filterSize);
      System.out.printf(
          "Starting publishing for %d second(s) at rate %d, batch size %d, max unconfirmed messages %d...%n",
          publishingDuration.getSeconds(), rate, batchSize, maxUnconfirmedMessages);
      while (keepPublishing.get()) {
        AtomicBoolean keepPublishingInCycle = new AtomicBoolean(true);
        scheduledExecutorService.schedule(
            () -> keepPublishingInCycle.set(false),
            publishingCycle.toMillis(),
            TimeUnit.MILLISECONDS);
        Collections.shuffle(filterValues);
        List<String> filterValueSubset = filterValues.subList(0, filterValueSubsetCount);
        System.out.printf(
            "Starting publishing cycle for %d second(s)...%n", publishingCycle.getSeconds());
        while (keepPublishingInCycle.get()) {
          rateLimiter.acquire(1);
          String filterValue = filterValueSubset.get(random.nextInt(filterValueSubsetCount));
          producer.send(
              producer.messageBuilder().properties().to(filterValue).messageBuilder().build(),
              confirmationHandler);
          publishedCount.getAndIncrement();
        }
      }
      System.out.println("Done publishing, waiting for all confirmations...");
      waitAtMost(() -> publishedCount.get() == confirmedCount.get());

      System.out.println("Starting consuming...");

      List<String> values = filterValues.subList(0, 10);
      for (String filterValue : values) {
        Duration timeout = Duration.ofSeconds(30);
        long start = System.nanoTime();
        System.out.printf("For filter value %s%n", filterValue);
        MetricRegistry registry = new MetricRegistry();
        DropwizardMetricsCollector collector = new DropwizardMetricsCollector(registry);
        AtomicLong unfilteredTargetMessageCount = new AtomicLong(0);
        Duration unfilteredDuration;
        try (Environment e = Environment.builder().metricsCollector(collector).build()) {
          AtomicBoolean hasReceivedSomething = new AtomicBoolean(false);
          AtomicLong lastReceived = new AtomicLong(0);
          long s = System.nanoTime();
          e.consumerBuilder().stream(stream)
              .offset(OffsetSpecification.first())
              .messageHandler(
                  (ctx, msg) -> {
                    hasReceivedSomething.set(true);
                    lastReceived.set(System.nanoTime());
                    if (filterValue.equals(msg.getProperties().getTo())) {
                      unfilteredTargetMessageCount.getAndIncrement();
                    }
                  })
              .build();
          waitAtMost(
              timeout,
              () ->
                  hasReceivedSomething.get()
                      && System.nanoTime() - lastReceived.get() > Duration.ofSeconds(1).toNanos());
          unfilteredDuration = Duration.ofNanos(System.nanoTime() - s);
        }

        long unfilteredChunkCount = registry.getMeters().get("rabbitmq.stream.chunk").getCount();
        long unfilteredMessageCount =
            registry.getMeters().get("rabbitmq.stream.consumed").getCount();

        registry = new MetricRegistry();
        collector = new DropwizardMetricsCollector(registry);
        AtomicLong filteredTargetMessageCount = new AtomicLong(0);
        Duration filteredDuration;
        try (Environment e = Environment.builder().metricsCollector(collector).build()) {
          AtomicBoolean hasReceivedSomething = new AtomicBoolean(false);
          AtomicLong lastReceived = new AtomicLong(0);
          long s = System.nanoTime();
          e.consumerBuilder().stream(stream)
              .offset(OffsetSpecification.first())
              .filter()
              .values(filterValue)
              .postFilter(msg -> filterValue.equals(msg.getProperties().getTo()))
              .builder()
              .messageHandler(
                  (ctx, msg) -> {
                    hasReceivedSomething.set(true);
                    lastReceived.set(System.nanoTime());
                    filteredTargetMessageCount.getAndIncrement();
                  })
              .build();
          waitAtMost(
              timeout,
              () ->
                  hasReceivedSomething.get()
                      && System.nanoTime() - lastReceived.get() > Duration.ofSeconds(1).toNanos());
          filteredDuration = Duration.ofNanos(System.nanoTime() - s);
        }
        long filteredChunkCount = registry.getMeters().get("rabbitmq.stream.chunk").getCount();
        long filteredMessageCount = registry.getMeters().get("rabbitmq.stream.consumed").getCount();
        System.out.printf(
            "consumed in %d / %d ms, target messages %d / %d, chunk count %d / %d (%d %%), messages %d / %d (%d %%)%n",
            unfilteredDuration.toMillis(),
            filteredDuration.toMillis(),
            unfilteredTargetMessageCount.get(),
            filteredTargetMessageCount.get(),
            unfilteredChunkCount,
            filteredChunkCount,
            (unfilteredChunkCount - filteredChunkCount) * 100 / unfilteredChunkCount,
            unfilteredMessageCount,
            filteredMessageCount,
            (unfilteredMessageCount - filteredMessageCount) * 100 / unfilteredMessageCount);
      }

    } finally {
      scheduledExecutorService.shutdownNow();
    }
  }
}
