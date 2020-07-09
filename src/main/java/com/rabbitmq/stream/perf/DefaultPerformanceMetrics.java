// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.codahale.metrics.*;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

class DefaultPerformanceMetrics implements PerformanceMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultPerformanceMetrics.class);

    private final MetricRegistry metricRegistry;
    private final Timer latency;
    private final boolean summaryFile;
    private volatile Closeable closingSequence = () -> {
    };

    DefaultPerformanceMetrics(CompositeMeterRegistry meterRegistry, String metricsPrefix, boolean summaryFile) {
        this.summaryFile = summaryFile;
        DropwizardConfig dropwizardConfig = new DropwizardConfig() {
            @Override
            public String prefix() {
                return "";
            }

            @Override
            public String get(String key) {
                return null;
            }
        };
        this.metricRegistry = new MetricRegistry();
        DropwizardMeterRegistry dropwizardMeterRegistry = new DropwizardMeterRegistry(dropwizardConfig, this.metricRegistry,
                HierarchicalNameMapper.DEFAULT, io.micrometer.core.instrument.Clock.SYSTEM) {
            @Override
            protected Double nullGaugeValue() {
                return null;
            }
        };
        meterRegistry.add(dropwizardMeterRegistry);
        this.latency = Timer
                .builder(metricsPrefix + ".latency")
                .description("message latency")
                .publishPercentiles(0.5, 0.75, 0.95, 0.99)
                .distributionStatisticExpiry(Duration.ofSeconds(1))
                .serviceLevelObjectives()
                .register(meterRegistry);
    }

    @Override
    public void start(String description) throws Exception {
        String metricPublished = "rabbitmqStreamPublished";
        String metricProducerConfirmed = "rabbitmqStreamProducer_confirmed";
        String metricConfirmed = "rabbitmqStreamConfirmed";
        String metricConsumed = "rabbitmqStreamConsumed";
        String metricChunkSize = "rabbitmqStreamChunk_size";
        String metricLatency = "rabbitmqStreamLatency";

        Set<String> allMetrics = new HashSet<>(Arrays.asList(metricPublished, metricConfirmed, metricConsumed,
                metricChunkSize, metricLatency));

        Map<String, String> metersNamesAndLabels = new LinkedHashMap<>();
        metersNamesAndLabels.put(metricPublished, "published");
        metersNamesAndLabels.put(metricConfirmed, "confirmed");
        metersNamesAndLabels.put(metricProducerConfirmed, "p. confirmed");
        metersNamesAndLabels.put(metricConsumed, "consumed");

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        Closeable summaryFileClosingSequence = maybeSetSummaryFile(description, allMetrics, scheduledExecutorService);

        SortedMap<String, Meter> registryMeters = metricRegistry.getMeters();

        Map<String, Meter> meters = new LinkedHashMap<>(metersNamesAndLabels.size());
        metersNamesAndLabels.entrySet().forEach(entry -> meters.put(entry.getValue(), registryMeters.get(entry.getKey())));
        Function<Map.Entry<String, Meter>, String> formatMeter = entry ->
                String.format("%s %.0f msg/s, ", entry.getKey(), entry.getValue().getMeanRate()
                );

        Histogram chunkSize = metricRegistry.getHistograms().get(metricChunkSize);
        Function<Histogram, String> formatChunkSize = histogram -> String.format("chunk size %.0f", histogram.getSnapshot().getMean());

        com.codahale.metrics.Timer latency = metricRegistry.getTimers().get(metricLatency);

        Function<Number, Number> convertDuration = in -> in instanceof Long ? in.longValue() / 1000 : in.doubleValue() / 1000;
        Function<com.codahale.metrics.Timer, String> formatLatency = timer -> {
            Snapshot snapshot = timer.getSnapshot();
            return String.format(
                    "latency min/median/75th/95th/99th %.0f/%.0f/%.0f/%.0f/%.0f µs",
                    convertDuration.apply(snapshot.getMin()), convertDuration.apply(snapshot.getMedian()),
                    convertDuration.apply(snapshot.get75thPercentile()),
                    convertDuration.apply(snapshot.get95thPercentile()), convertDuration.apply(snapshot.get99thPercentile())
            );
        };

        AtomicInteger reportCount = new AtomicInteger(1);
        ScheduledFuture<?> consoleReportingTask = scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                StringBuilder builder = new StringBuilder();
                builder.append(reportCount.get()).append(", ");
                meters.entrySet().forEach(entry -> builder.append(formatMeter.apply(entry)));
                builder.append(formatLatency.apply(latency)).append(", ");
                builder.append(formatChunkSize.apply(chunkSize));
                System.out.println(builder);
                reportCount.incrementAndGet();
            } catch (Exception e) {
                LOGGER.warn("Error while metrics report: {}", e.getMessage());
            }
        }, 1, 1, TimeUnit.SECONDS);

        long start = System.currentTimeMillis();

        this.closingSequence = () -> {
            consoleReportingTask.cancel(true);

            summaryFileClosingSequence.close();

            scheduledExecutorService.shutdownNow();

            long duration = System.currentTimeMillis() - start;

            Function<Map.Entry<String, Meter>, String> formatMeterSummary = entry -> String.format("%s %d msg/s, ",
                    entry.getKey(),
                    entry.getValue().getCount() * 1000 / duration);

            Function<com.codahale.metrics.Timer, String> formatLatencySummary = histogram -> String.format(
                    "latency 95th %.0f µs",
                    convertDuration.apply(latency.getSnapshot().get95thPercentile())
            );

            StringBuilder builder = new StringBuilder("Summary: ");
            meters.entrySet().forEach(entry -> builder.append(formatMeterSummary.apply(entry)));
            builder.append(formatLatencySummary.apply(latency)).append(", ");
            builder.append(formatChunkSize.apply(chunkSize));
            System.out.println();
            System.out.println(builder);
        };
    }

    private Closeable maybeSetSummaryFile(String description, Set<String> allMetrics, ScheduledExecutorService scheduledExecutorService) throws IOException {
        Closeable summaryFileClosingSequence;
        if (this.summaryFile) {
            String currentFilename = "stream-perf-test-current.txt";
            String finalFilename = "stream-perf-test-" + new SimpleDateFormat("yyyy-MM-dd-HHmmss").format(new Date()) + ".txt";
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

            ConsoleReporter fileReporter = ConsoleReporter.forRegistry(metricRegistry)
                    .filter((name, metric) -> allMetrics.contains(name))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .outputTo(printStream)
                    .scheduleOn(scheduledExecutorService)
                    .shutdownExecutorOnStop(false)
                    .build();
            fileReporter.start(1, TimeUnit.SECONDS);
            summaryFileClosingSequence = () -> {
                fileReporter.stop();
                printStream.close();
                Files.move(currentFile, currentFile.resolveSibling(finalFilename));
            };
        } else {
            summaryFileClosingSequence = () -> {
            };
        }
        return summaryFileClosingSequence;
    }

    @Override
    public void latency(long latency, TimeUnit unit) {
        this.latency.record(latency, unit);
    }

    @Override
    public void close() throws Exception {
        this.closingSequence.close();
    }
}
