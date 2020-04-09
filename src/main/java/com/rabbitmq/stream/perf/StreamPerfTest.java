// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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
import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.Client;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.QpidProtonCodec;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@CommandLine.Command(name = "stream-perf-test", mixinStandardHelpOptions = true, showDefaultValues = true,
        version = "perftest 0.1",
        description = "Tests the performance of stream queues in RabbitMQ.")
public class StreamPerfTest implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamPerfTest.class);
    private final String[] arguments;
    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private final Codec codec = new QpidProtonCodec();
    int addressDispatching = 0;
    int targetDispatching = 0;
    @CommandLine.Option(names = {"--addresses", "-a"}, description = "servers to connect to, e.g. localhost:5555", defaultValue = "localhost:5555")
    private List<String> addrs;
    @CommandLine.Option(names = {"--producers", "-x"}, description = "number of producers", defaultValue = "1")
    private int producers;
    @CommandLine.Option(names = {"--consumers", "-y"}, description = "number of consumers", defaultValue = "1")
    private int consumers;
    @CommandLine.Option(names = {"--size", "-s"}, description = "size of messages in bytes", defaultValue = "10")
    private int messageSize;
    @CommandLine.Option(names = {"--initial-credit", "-icr"}, description = "initial credit when registering a consumer", defaultValue = "10")
    private int initialCredit;
    @CommandLine.Option(names = {"--credit", "-cr"}, description = "credit requested on acknowledgment", defaultValue = "1")
    private int credit;
    @CommandLine.Option(names = {"--ack", "-ac"}, description = "ack (request credit) every x chunk(s)", defaultValue = "1")
    private int ack;
    @CommandLine.Option(names = {"--confirms", "-c"}, description = "outstanding confirms", defaultValue = "-1")
    private int confirms;
    @CommandLine.Option(names = {"--targets", "-t"}, description = "target(s) to send to and consume from", defaultValue = "target1")
    private List<String> targets;
    @CommandLine.Option(names = {"--offset", "-o"}, description = "offset to start listening from", defaultValue = "0")
    private int offset;
    @CommandLine.Option(names = {"--pre-declared", "-p"}, description = "whether targets are already declared or not", defaultValue = "false")
    private boolean preDeclared;
    @CommandLine.Option(names = {"--rate", "-r"}, description = "maximum rate of published messages", defaultValue = "-1")
    private int rate;
    @CommandLine.Option(names = {"--batch-size", "-bs"}, description = "size of a batch of published messages", defaultValue = "1")
    private int batchSize;
    private List<Address> addresses;

    public StreamPerfTest(String[] arguments) {
        this.arguments = arguments;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new StreamPerfTest(args)).execute(args);
        System.exit(exitCode);
    }

    private static List<Address> addresses(List<String> addresses) {
        return addresses.stream().map(address -> {
            String[] hostPort = address.split(":");
            return new Address(hostPort[0], Integer.parseInt(hostPort[1]));
        }).collect(Collectors.toList());
    }

    private static Closeable startMetricsReporting(String header, MetricRegistry metrics) throws IOException {
        String filename = "stream-perf-test-" + new SimpleDateFormat("yyyy-MM-dd-HHmmss").format(new Date()) + ".txt";
        OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(filename));
        PrintStream printStream = new PrintStream(outputStream);
        if (header != null && !header.trim().isEmpty()) {
            printStream.println(header);
        }

        ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        ConsoleReporter fileReporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .outputTo(printStream)
                .scheduleOn(scheduledExecutorService)
                .shutdownExecutorOnStop(false)
                .build();


        SortedMap<String, Meter> registryMeters = metrics.getMeters();
        List<String> metersNames = Arrays.asList("published", "confirmed", "consumed");
        Map<String, Meter> meters = new LinkedHashMap<>(metersNames.size());
        metersNames.forEach(name -> meters.put(name, registryMeters.get(name)));
        Function<Map.Entry<String, Meter>, String> formatMeter = entry ->
                String.format("%s %.0f msg/s, ", entry.getKey(), entry.getValue().getMeanRate()
                );

        Histogram chunkSize = metrics.getHistograms().get("chunk.size");
        Function<Histogram, String> formatChunkSize = histogram -> String.format("chunk size %.0f", histogram.getSnapshot().getMean());

        Histogram latency = metrics.getHistograms().get("latency");
        Function<Histogram, String> formatLatency = histogram -> {
            Snapshot snapshot = histogram.getSnapshot();
            return String.format(
                    "latency min/median/75th/95th/99th %d/%.0f/%.0f/%.0f/%.0f µs",
                    snapshot.getMin(), snapshot.getMedian(), snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(), snapshot.get99thPercentile()
            );
        };

        fileReporter.start(1, TimeUnit.SECONDS);

        AtomicInteger reportCount = new AtomicInteger(1);
        ScheduledFuture<?> consoleReportingTask = scheduledExecutorService.scheduleAtFixedRate(() -> {
            StringBuilder builder = new StringBuilder();
            builder.append(reportCount.get()).append(", ");
            meters.entrySet().forEach(entry -> builder.append(formatMeter.apply(entry)));
            builder.append(formatLatency.apply(latency)).append(", ");
            builder.append(formatChunkSize.apply(chunkSize));
            System.out.println(builder);
            reportCount.incrementAndGet();
        }, 1, 1, TimeUnit.SECONDS);

        long start = System.currentTimeMillis();

        return () -> {
            consoleReportingTask.cancel(true);
            fileReporter.stop();
            printStream.close();
            scheduledExecutorService.shutdownNow();

            long duration = System.currentTimeMillis() - start;

            Function<Map.Entry<String, Meter>, String> formatMeterSummary = entry -> String.format("%s %d msg/s, ",
                    entry.getKey(),
                    entry.getValue().getCount() * 1000 / duration);

            Function<Histogram, String> formatLatencySummary = histogram -> String.format(
                    "latency 95th %.0f µs",
                    latency.getSnapshot().get95thPercentile()
            );

            StringBuilder builder = new StringBuilder("Summary: ");
            meters.entrySet().forEach(entry -> builder.append(formatMeterSummary.apply(entry)));
            builder.append(formatLatencySummary.apply(latency)).append(", ");
            builder.append(formatChunkSize.apply(chunkSize));
            System.out.println();
            System.out.println(builder);

        };

    }

    @Override
    public Integer call() throws Exception {
        this.addresses = addresses(this.addrs);

        ShutdownService shutdownService = new ShutdownService();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownService.close()));

        shutdownService.wrap(closeStep("Closing Netty", () -> {
            int shutdownTimeout = 10;
            try {
                eventLoopGroup.shutdownGracefully(0, 10, TimeUnit.SECONDS).get(shutdownTimeout, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.info("Could not shut down Netty in {} second(s)", shutdownTimeout);
            }

        }));

        MetricRegistry metrics = new MetricRegistry();
        Meter consumed = metrics.meter("consumed");
        Histogram chunkSize = metrics.histogram("chunk.size");
        Histogram latency = metrics.histogram("latency");

        if (!preDeclared) {
            Address address = addresses.get(0);
            Client client = client(new Client.ClientParameters()
                    .host(address.host).port(address.port)
            );
            for (String target : targets) {
                Client.Response response = client.create(target);
                if (response.isOk()) {
                    LOGGER.info("Created target {}", target);
                } else {
                    throw new IllegalStateException("Could not create " + target + ", response code is " + response.getResponseCode());
                }
            }
            client.close();
            shutdownService.wrap(closeStep("Deleting target(s)", () -> {
                if (!preDeclared) {
                    Address addr = addresses.get(0);
                    Client c = client(new Client.ClientParameters()
                            .host(address.host).port(addr.port)
                    );
                    for (String target : targets) {
                        LOGGER.info("Deleting {}", target);
                        Client.Response response = c.delete(target);
                        if (!response.isOk()) {
                            LOGGER.warn("Could not delete target {}, response code was {}", target, response.getResponseCode());
                        }
                        LOGGER.info("Deleted {}", target);
                    }
                    c.close();
                }
            }));
        }

        // FIXME handle metadata update for consumers and publishers
        // they should at least issue a warning that their target has been deleted and that they're now useless

        List<Client> consumers = Collections.synchronizedList(IntStream.range(0, this.consumers).mapToObj(i -> {
            BooleanSupplier creditRequestCondition;
            if (this.ack == 1) {
                creditRequestCondition = () -> true;
            } else {
                AtomicInteger chunkCount = new AtomicInteger(0);
                int ackEvery = this.ack;
                creditRequestCondition = () -> chunkCount.incrementAndGet() % ackEvery == 0;
            }
            int creditToAsk = this.credit;
            Client.ChunkListener chunkListener = (client, correlationId, offset, recordCount, dataSize) -> {
                chunkSize.update(recordCount);
                if (creditRequestCondition.getAsBoolean()) {
                    client.credit(correlationId, creditToAsk);
                }
            };
            Client.RecordListener recordListener = (correlationId, offset, data) -> {
                consumed.mark();
                latency.update((System.nanoTime() - data.getProperties().getCreationTime()) / 1000L);
            };

            Address address = address();
            Client client = client(new Client.ClientParameters()
                    .host(address.host).port(address.port)
                    .chunkListener(chunkListener)
                    .recordListener(recordListener)
            );

            return client;
        }).collect(Collectors.toList()));

        shutdownService.wrap(closeStep("Closing consumers", () -> {
            for (Client consumer : consumers) {
                consumer.close();
            }
        }));

        Meter published = metrics.meter("published");
        Meter confirmed = metrics.meter("confirmed");
        List<Client> producers = Collections.synchronizedList(new ArrayList<>(this.producers));
        List<Runnable> producerRunnables = IntStream.range(0, this.producers).mapToObj(i -> {
            Runnable beforePublishingCallback;
            Client.ConfirmListener confirmListener;
            if (this.confirms > 0) {
                Semaphore confirmsSemaphore = new Semaphore(this.confirms);
                beforePublishingCallback = () -> {
                    try {
                        if (!confirmsSemaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                            LOGGER.info("Could not acquire confirm semaphore in 30 seconds");
                        }
                    } catch (InterruptedException e) {
                        LOGGER.info("Interrupted while waiting confirms before publishing");
                        Thread.currentThread().interrupt();
                    }
                };
                confirmListener = correlationId -> {
                    confirmed.mark();
                    confirmsSemaphore.release();
                };
            } else {
                beforePublishingCallback = () -> {
                };
                confirmListener = correlationId -> confirmed.mark();
            }

            Runnable rateLimiterCallback;
            if (this.rate > 0) {
                RateLimiter rateLimiter = com.google.common.util.concurrent.RateLimiter.create(this.rate);
                rateLimiterCallback = () -> {
                    rateLimiter.acquire(this.batchSize);
                };
            } else {
                rateLimiterCallback = () -> {
                };
            }

            Address address = address();
            Client client = client(new Client.ClientParameters()
                    .host(address.host).port(address.port)
                    .confirmListener(confirmListener)
            );
            producers.add(client);

            shutdownService.wrap(closeStep("Closing producers", () -> {
                for (Client producer : producers) {
                    producer.close();
                }
            }));

            String target = target();
            LOGGER.info("Producer will target {}", target);
            return (Runnable) () -> {
                // FIXME fill the message with some data
                byte[] data = new byte[this.messageSize];
                List<Message> messages = new ArrayList<>(this.batchSize);
                for (int j = 0; j < this.batchSize; j++) {
                    messages.add(client.messageBuilder().addData(data).build());
                }
                while (true && !Thread.currentThread().isInterrupted()) {
                    beforePublishingCallback.run();
                    rateLimiterCallback.run();
                    long creationTime = System.nanoTime();
                    for (int j = 0; j < this.batchSize; j++) {
                        messages.set(j, client.messageBuilder()
                                .properties().creationTime(creationTime).messageBuilder()
                                .addData(data).build());
                    }
                    client.publish(target, messages);
                    published.mark(this.batchSize);
                }
            };

        }).collect(Collectors.toList());

        int consumerSequence = 0;
        for (Client consumer : consumers) {
            String target = target();
            LOGGER.info("Starting consuming on {}", target);
            consumer.subscribe(consumerSequence++, target, this.offset, this.initialCredit);
        }

        ExecutorService executorService;
        if (this.producers > 0) {
            executorService = Executors.newFixedThreadPool(this.producers);
            for (Runnable producer : producerRunnables) {
                LOGGER.info("Starting producer");
                executorService.submit(producer);
            }
        } else {
            executorService = null;
        }

        shutdownService.wrap(closeStep("Closing executor service", () -> {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }));

        String metricsHeader = "Arguments: " + String.join(" ", arguments);

        Closeable metricsReporting = startMetricsReporting(metricsHeader, metrics);
        shutdownService.wrap(closeStep("Closing metrics", () -> {
            metricsReporting.close();
        }));

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> latch.countDown()));
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        shutdownService.close();

        return 0;
    }

    private ShutdownService.CloseCallback closeStep(String message, ShutdownService.CloseCallback callback) {
        return () -> {
            LOGGER.debug(message);
            callback.run();
        };
    }

    private Client client(Client.ClientParameters parameters) {
        return new Client(parameters.eventLoopGroup(this.eventLoopGroup).codec(codec));
    }

    private String target() {
        return targets.get(targetDispatching++ % targets.size());
    }

    private Address address() {
        return addresses.get(addressDispatching++ % addresses.size());
    }

    private static class Address {

        final String host;
        final int port;

        private Address(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }


}
