// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.perf;

import com.codahale.metrics.ConsoleReporter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.HistogramSupport;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.CharacterIterator;
import java.text.SimpleDateFormat;
import java.text.StringCharacterIterator;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultPerformanceMetrics implements PerformanceMetrics {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPerformanceMetrics.class);

  private final String metricsPrefix;
  private final CompositeMeterRegistry meterRegistry;
  private final Timer latency, confirmLatency;
  private final boolean summaryFile;
  private final PrintWriter out;
  private final boolean includeByteRates;
  private final Supplier<String> memoryReportSupplier;
  private volatile Closeable closingSequence = () -> {};
  private volatile long lastPublishedCount = 0;
  private volatile long lastConsumedCount = 0;
  private volatile long offset;

  DefaultPerformanceMetrics(
      CompositeMeterRegistry meterRegistry,
      String metricsPrefix,
      boolean summaryFile,
      boolean includeByteRates,
      boolean confirmLatency,
      Supplier<String> memoryReportSupplier,
      PrintWriter out) {
    this.summaryFile = summaryFile;
    this.includeByteRates = includeByteRates;
    this.memoryReportSupplier = memoryReportSupplier;
    this.out = out;
    this.metricsPrefix = metricsPrefix;
    this.meterRegistry = meterRegistry;

    this.latency =
        Timer.builder(metricsPrefix + ".latency")
            .description("message latency")
            .publishPercentiles(0.5, 0.75, 0.95, 0.99)
            .distributionStatisticExpiry(Duration.ofSeconds(1))
            .serviceLevelObjectives()
            .register(meterRegistry);
    if (confirmLatency) {
      this.confirmLatency =
          Timer.builder(metricsPrefix + ".confirm_latency")
              .description("publish confirm latency")
              .publishPercentiles(0.5, 0.75, 0.95, 0.99)
              .distributionStatisticExpiry(Duration.ofSeconds(1))
              .serviceLevelObjectives()
              .register(meterRegistry);
    } else {
      this.confirmLatency = null;
    }
  }

  private long getPublishedCount() {
    return (long) this.meterRegistry.get(metricsName("published")).counter().count();
  }

  private long getConsumedCount() {
    return (long) this.meterRegistry.get(metricsName("consumed")).counter().count();
  }

  @Override
  public void start(String description) throws Exception {
    long startTime = System.nanoTime();

    String metricPublished = metricsName("published");
    String metricProducerConfirmed = metricsName("confirmed");
    String metricConsumed = metricsName("consumed");
    String metricChunkSize = metricsName("chunk_size");
    String metricLatency = metricsName("latency");
    String metricConfirmLatency = metricsName("confirm_latency");
    String metricWrittenBytes = metricsName("written_bytes");
    String metricReadBytes = metricsName("read_bytes");

    Set<String> allMetrics =
        new HashSet<>(
            Arrays.asList(
                metricPublished,
                metricProducerConfirmed,
                metricConsumed,
                metricChunkSize,
                metricLatency));

    if (confirmLatency()) {
      allMetrics.add(metricConfirmLatency);
    }

    Map<String, String> countersNamesAndLabels = new LinkedHashMap<>();
    countersNamesAndLabels.put(metricPublished, "published");
    countersNamesAndLabels.put(metricProducerConfirmed, "confirmed");
    countersNamesAndLabels.put(metricConsumed, "consumed");

    if (this.includeByteRates) {
      allMetrics.add(metricWrittenBytes);
      allMetrics.add(metricReadBytes);
      countersNamesAndLabels.put(metricWrittenBytes, "written bytes");
      countersNamesAndLabels.put(metricReadBytes, "read bytes");
    }

    ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor();

    Closeable summaryFileClosingSequence =
        maybeSetSummaryFile(description, allMetrics, scheduledExecutorService);

    Map<String, Counter> counters = new LinkedHashMap<>(countersNamesAndLabels.size());
    countersNamesAndLabels
        .entrySet()
        .forEach(
            entry -> counters.put(entry.getValue(), meterRegistry.get(entry.getKey()).counter()));

    Map<String, FormatCallback> formatCounter = new HashMap<>();
    countersNamesAndLabels.entrySet().stream()
        .filter(entry -> !entry.getKey().contains("bytes"))
        .forEach(
            entry -> {
              formatCounter.put(
                  entry.getValue(),
                  (lastValue, currentValue, duration) -> {
                    long rate = 1000 * (currentValue - lastValue) / duration.toMillis();
                    return String.format("%s %d msg/s, ", entry.getValue(), rate);
                  });
            });

    countersNamesAndLabels.entrySet().stream()
        .filter(entry -> entry.getKey().contains("bytes"))
        .forEach(
            entry -> {
              formatCounter.put(
                  entry.getValue(),
                  (lastValue, currentValue, duration) -> {
                    long rate = 1000 * (currentValue - lastValue) / duration.toMillis();
                    return formatByteRate(entry.getValue(), rate) + ", ";
                  });
            });

    HistogramSupport chunkSize = meterRegistry.get(metricChunkSize).summary();
    Function<HistogramSupport, String> formatChunkSize =
        histogram -> String.format("chunk size %.0f", histogram.takeSnapshot().mean());

    Function<Number, Number> convertDuration =
        in -> in instanceof Long ? in.longValue() / 1_000_000 : in.doubleValue() / 1_000_000;
    BiFunction<String, Timer, String> formatLatency =
        (name, timer) -> {
          HistogramSnapshot snapshot = timer.takeSnapshot();

          return String.format(
              name + " median/75th/95th/99th %.0f/%.0f/%.0f/%.0f ms",
              convertDuration.apply(percentile(snapshot, 0.5).value()),
              convertDuration.apply(percentile(snapshot, 0.75).value()),
              convertDuration.apply(percentile(snapshot, 0.95).value()),
              convertDuration.apply(percentile(snapshot, 0.99).value()));
        };

    AtomicInteger reportCount = new AtomicInteger(1);

    AtomicLong lastTick = new AtomicLong(startTime);
    Map<String, Long> lastMetersValues = new ConcurrentHashMap<>(counters.size());
    counters.entrySet().forEach(e -> lastMetersValues.put(e.getKey(), (long) e.getValue().count()));

    ScheduledFuture<?> consoleReportingTask =
        scheduledExecutorService.scheduleAtFixedRate(
            () -> {
              try {
                if (checkActivity()) {
                  long currentTime = System.nanoTime();
                  Duration duration = Duration.ofNanos(currentTime - lastTick.get());
                  lastTick.set(currentTime);
                  StringBuilder builder = new StringBuilder();
                  builder.append(reportCount.get()).append(", ");
                  counters
                      .entrySet()
                      .forEach(
                          entry -> {
                            String meterName = entry.getKey();
                            Counter counter = entry.getValue();
                            long lastValue = lastMetersValues.get(meterName);
                            long currentValue = (long) counter.count();
                            builder.append(
                                formatCounter
                                    .get(meterName)
                                    .compute(lastValue, currentValue, duration));
                            lastMetersValues.put(meterName, currentValue);
                          });
                  if (confirmLatency()) {
                    builder
                        .append(formatLatency.apply("confirm latency", confirmLatency))
                        .append(", ");
                  }
                  builder.append(formatLatency.apply("latency", latency)).append(", ");
                  builder.append(formatChunkSize.apply(chunkSize));
                  this.out.println(builder);
                  String memoryReport = this.memoryReportSupplier.get();
                  if (!memoryReport.isEmpty()) {
                    this.out.println(memoryReport);
                  }
                }
                reportCount.incrementAndGet();
              } catch (Exception e) {
                LOGGER.warn("Error while computing metrics report: {}", e.getMessage());
              }
            },
            1,
            1,
            TimeUnit.SECONDS);

    this.closingSequence =
        () -> {
          consoleReportingTask.cancel(true);

          summaryFileClosingSequence.close();

          scheduledExecutorService.shutdownNow();

          Duration d = Duration.ofNanos(System.nanoTime() - startTime);
          Duration duration = d.getSeconds() <= 0 ? Duration.ofSeconds(1) : d;

          Function<Entry<String, Counter>, String> formatMeterSummary =
              entry -> {
                if (entry.getKey().contains("bytes")) {
                  return formatByteRate(
                          entry.getKey(),
                          1000 * (long) entry.getValue().count() / duration.toMillis())
                      + ", ";
                } else {
                  return String.format(
                      "%s %d msg/s, ",
                      entry.getKey(), 1000 * (long) entry.getValue().count() / duration.toMillis());
                }
              };

          BiFunction<String, HistogramSupport, String> formatLatencySummary =
              (name, histogram) ->
                  String.format(
                      name + " 95th %.0f ms",
                      convertDuration.apply(percentile(histogram.takeSnapshot(), 0.95).value()));

          StringBuilder builder = new StringBuilder("Summary: ");
          counters.entrySet().forEach(entry -> builder.append(formatMeterSummary.apply(entry)));
          if (confirmLatency()) {
            builder
                .append(formatLatencySummary.apply("confirm latency", confirmLatency))
                .append(", ");
          }
          builder.append(formatLatencySummary.apply("latency", latency)).append(", ");
          builder.append(formatChunkSize.apply(chunkSize));
          this.out.println();
          this.out.println(builder);
        };
  }

  static String formatByteRate(String label, double bytes) {
    // based on
    // https://stackoverflow.com/questions/3758606/how-can-i-convert-byte-size-into-a-human-readable-format-in-java
    if (-1000 < bytes && bytes < 1000) {
      return bytes + " B/s";
    }
    CharacterIterator ci = new StringCharacterIterator("kMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%s %.1f %cB/s", label, bytes / 1000.0, ci.current());
  }

  private Closeable maybeSetSummaryFile(
      String description, Set<String> allMetrics, ScheduledExecutorService scheduledExecutorService)
      throws IOException {
    Closeable summaryFileClosingSequence;
    if (this.summaryFile) {
      String currentFilename = "stream-perf-test-current.txt";
      String finalFilename =
          "stream-perf-test-"
              + new SimpleDateFormat("yyyy-MM-dd-HHmmss").format(new Date())
              + ".txt";
      Path currentFile = Paths.get(currentFilename);
      if (Files.exists(currentFile)) {
        if (!Files.deleteIfExists(Paths.get(currentFilename))) {
          LOGGER.warn("Could not delete file {}", currentFilename);
        }
      }
      OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(currentFilename));
      PrintStream printStream = new PrintStream(outputStream);
      if (description != null && !description.trim().isEmpty()) {
        printStream.println(description);
      }

      DropwizardMeterRegistry dropwizardMeterRegistry =
          this.meterRegistry.getRegistries().stream()
              .filter(r -> r instanceof DropwizardMeterRegistry)
              .map(r -> (DropwizardMeterRegistry) r)
              .findAny()
              .orElseGet(() -> Utils.dropwizardMeterRegistry());

      if (!this.meterRegistry.getRegistries().contains(dropwizardMeterRegistry)) {
        this.meterRegistry.add(dropwizardMeterRegistry);
      }

      ConsoleReporter fileReporter =
          ConsoleReporter.forRegistry(dropwizardMeterRegistry.getDropwizardRegistry())
              .filter((name, metric) -> allMetrics.contains(name))
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .outputTo(printStream)
              .scheduleOn(scheduledExecutorService)
              .shutdownExecutorOnStop(false)
              .build();
      fileReporter.start(1, TimeUnit.SECONDS);
      summaryFileClosingSequence =
          () -> {
            fileReporter.stop();
            printStream.close();
            Files.move(currentFile, currentFile.resolveSibling(finalFilename));
          };
    } else {
      summaryFileClosingSequence = () -> {};
    }
    return summaryFileClosingSequence;
  }

  boolean checkActivity() {
    long currentPublishedCount = getPublishedCount();
    long currentConsumedCount = getConsumedCount();
    boolean activity =
        this.lastPublishedCount != currentPublishedCount
            || this.lastConsumedCount != currentConsumedCount;
    LOGGER.debug(
        "Activity check: published {} vs {}, consumed {} vs {}, activity {}, offset {}",
        this.lastPublishedCount,
        currentPublishedCount,
        this.lastConsumedCount,
        currentConsumedCount,
        activity,
        this.offset);
    if (activity) {
      this.lastPublishedCount = currentPublishedCount;
      this.lastConsumedCount = currentConsumedCount;
    }
    return activity;
  }

  @Override
  public void latency(long latency, TimeUnit unit) {
    this.latency.record(latency, unit);
  }

  @Override
  public void confirmLatency(long latency, TimeUnit unit) {
    this.confirmLatency.record(latency, unit);
  }

  @Override
  public void offset(long offset) {
    this.offset = offset;
  }

  @Override
  public void close() throws Exception {
    this.closingSequence.close();
  }

  private boolean confirmLatency() {
    return this.confirmLatency != null;
  }

  private interface FormatCallback {

    String compute(long lastValue, long currentValue, Duration duration);
  }

  private String metricsName(String name) {
    return this.metricsPrefix + "." + name;
  }

  private static ValueAtPercentile percentile(HistogramSnapshot snapshot, double expected) {
    for (ValueAtPercentile percentile : snapshot.percentileValues()) {
      if (percentile.percentile() == expected) {
        return percentile;
      }
    }
    return null;
  }
}
