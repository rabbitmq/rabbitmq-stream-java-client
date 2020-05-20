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
import com.rabbitmq.stream.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

@CommandLine.Command(name = "stream-perf-test", mixinStandardHelpOptions = false, showDefaultValues = true,
        version = "perftest 0.1",
        description = "Tests the performance of stream queues in RabbitMQ.")
public class StreamPerfTest implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamPerfTest.class);
    private final String[] arguments;
    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    private volatile Codec codec;
    @CommandLine.Mixin
    private final CommandLine.HelpCommand helpCommand = new CommandLine.HelpCommand();
    int addressDispatching = 0;
    int streamDispatching = 0;
    @CommandLine.Option(names = {"--addresses", "-a"},
            description = "servers to connect to, e.g. localhost:5555, separated by commas",
            defaultValue = "localhost:5555",
            split = ","
    )
    private List<String> addrs;
    @CommandLine.Option(names = {"--producers", "-x"}, description = "number of producers", defaultValue = "1")
    private int producers;
    @CommandLine.Option(names = {"--consumers", "-y"}, description = "number of consumers", defaultValue = "1")
    private int consumers;
    @CommandLine.Option(names = {"--size", "-s"}, description = "size of messages in bytes", defaultValue = "10")
    private volatile int messageSize;
    @CommandLine.Option(names = {"--initial-credit", "-icr"},
            description = "initial credit when registering a consumer", defaultValue = "10"
    )
    private int initialCredit;
    @CommandLine.Option(names = {"--credit", "-cr"}, description = "credit requested on acknowledgment", defaultValue = "1")
    private int credit;
    @CommandLine.Option(names = {"--ack", "-ac"}, description = "ack (request credit) every x chunk(s)", defaultValue = "1")
    private int ack;
    @CommandLine.Option(names = {"--confirms", "-c"}, description = "outstanding confirms", defaultValue = "-1")
    private int confirms;
    @CommandLine.Option(names = {"--streams", "-st"},
            description = "stream(s) to send to and consume from, separated by commas", defaultValue = "stream1",
            split = ","
    )
    private List<String> streams;
    @CommandLine.Option(names = {"--offset", "-o"}, description = "offset to start listening from", defaultValue = "0")
    private long offset;
    @CommandLine.Option(names = {"--pre-declared", "-p"}, description = "whether streams are already declared or not", defaultValue = "false")
    private boolean preDeclared;
    @CommandLine.Option(names = {"--rate", "-r"}, description = "maximum rate of published messages", defaultValue = "-1")
    private int rate;
    @CommandLine.Option(names = {"--batch-size", "-bs"}, description = "size of a batch of published messages", defaultValue = "1")
    private int batchSize;
    @CommandLine.Option(names = {"--codec", "-cc"},
            description = "class of codec to use. Aliases: qpid, simple.",
            defaultValue = "qpid"
    )
    private String codecClass;


    private List<Address> addresses;
    @CommandLine.Option(names = {"--version", "-v"}, description = "show version information", defaultValue = "false")
    private boolean version;

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

            Files.move(currentFile, currentFile.resolveSibling(finalFilename));

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

    private static void versionInformation() {
        String lineSeparator = System.getProperty("line.separator");
        String version = format(
                "RabbitMQ Stream Perf Test %s (%s; %s)",
                Version.VERSION, Version.BUILD, Version.BUILD_TIMESTAMP
        );
        String info = format(
                "Java version: %s, vendor: %s" + lineSeparator +
                        "Java home: %s" + lineSeparator +
                        "Default locale: %s, platform encoding: %s" + lineSeparator +
                        "OS name: %s, version: %s, arch: %s",
                System.getProperty("java.version"), System.getProperty("java.vendor"),
                System.getProperty("java.home"),
                Locale.getDefault().toString(), Charset.defaultCharset().toString(),
                System.getProperty("os.name"), System.getProperty("os.version"), System.getProperty("os.arch")
        );
        System.out.println("\u001B[1m" + version);
        System.out.println("\u001B[0m" + info);
    }

    private static final Map<String, String> CODEC_ALIASES = new HashMap<String, String>() {{
        put("qpid", QpidProtonCodec.class.getName());
        put("simple", SimpleCodec.class.getName());
    }};

    private static Codec createCodec(String className) {
        className = CODEC_ALIASES.getOrDefault(className, className);
        try {
            return (Codec) Class.forName(className).getConstructor().newInstance();
        } catch (Exception e) {
            throw new ClientException("Exception while creating codec " + className, e);
        }
    }

    public static void writeLong(byte[] array, long value) {
        // from Guava Longs
        for (int i = 7; i >= 0; i--) {
            array[i] = (byte) (value & 0xffL);
            value >>= 8;
        }
    }

    public static long readLong(byte[] array) {
        // from Guava Longs
        return (array[0] & 0xFFL) << 56
                | (array[1] & 0xFFL) << 48
                | (array[2] & 0xFFL) << 40
                | (array[3] & 0xFFL) << 32
                | (array[4] & 0xFFL) << 24
                | (array[5] & 0xFFL) << 16
                | (array[6] & 0xFFL) << 8
                | (array[7] & 0xFFL);
    }

    @Override
    public Integer call() throws Exception {
        if (this.version) {
            versionInformation();
            System.exit(0);
        }
        this.addresses = addresses(this.addrs);
        this.codec = createCodec(this.codecClass);

        PayloadCreator payloadCreator;
        MessageProducingCallback messageProducingCallback;
        CreationTimeExtractor creationTimeExtractor;
        if ("simple".equals(this.codecClass)) {
            this.messageSize = this.messageSize < 8 ? 8 : this.messageSize; // we need to store a long in it
            payloadCreator = (size, timestamp) -> {
                byte[] payload = new byte[size];
                writeLong(payload, timestamp);
                return payload;
            };
            messageProducingCallback = (payload, timestamp, messageBuilder) -> messageBuilder.addData(payload).build();
            creationTimeExtractor = message -> readLong(message.getBodyAsBinary());
        } else {
            payloadCreator = (size, timestamp) -> new byte[size];
            messageProducingCallback = (payload, timestamp, messageBuilder) -> messageBuilder.properties().creationTime(timestamp).messageBuilder()
                    .addData(payload).build();
            creationTimeExtractor = message -> message.getProperties().getCreationTime();
        }

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
            for (String stream : streams) {
                Client.Response response = client.create(stream);
                if (response.isOk()) {
                    LOGGER.info("Created stream {}", stream);
                } else {
                    throw new IllegalStateException("Could not create " + stream + ", response code is " + response.getResponseCode());
                }
            }
            client.close();
            shutdownService.wrap(closeStep("Deleting stream(s)", () -> {
                if (!preDeclared) {
                    Address addr = addresses.get(0);
                    Client c = client(new Client.ClientParameters()
                            .host(address.host).port(addr.port)
                    );
                    for (String stream : streams) {
                        LOGGER.debug("Deleting {}", stream);
                        Client.Response response = c.delete(stream);
                        if (!response.isOk()) {
                            LOGGER.warn("Could not delete stream {}, response code was {}", stream, response.getResponseCode());
                        }
                        LOGGER.debug("Deleted {}", stream);
                    }
                    c.close();
                }
            }));
        }

        // FIXME handle metadata update for consumers and publishers
        // they should at least issue a warning that their stream has been deleted and that they're now useless

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
            Client.ChunkListener chunkListener = (client, correlationId, offset, messageCount, dataSize) -> {
                chunkSize.update(messageCount);
                if (creditRequestCondition.getAsBoolean()) {
                    client.credit(correlationId, creditToAsk);
                }
            };
            Client.MessageListener messageListener = (correlationId, offset, data) -> {
                consumed.mark();
                latency.update((System.nanoTime() - creationTimeExtractor.extract(data)) / 1000L);
            };

            Address address = address();
            Client client = client(new Client.ClientParameters()
                    .host(address.host).port(address.port)
                    .chunkListener(chunkListener)
                    .messageListener(messageListener)
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

            String stream = stream();
            LOGGER.info("Producer will stream {}", stream);
            return (Runnable) () -> {
                // FIXME fill the message with some data
                byte[] data = new byte[this.messageSize];
                List<Message> messages = new ArrayList<>(this.batchSize);
                for (int j = 0; j < this.batchSize; j++) {
                    messages.add(client.messageBuilder().addData(data).build());
                }
                final int msgSize = this.messageSize;
                while (true && !Thread.currentThread().isInterrupted()) {
                    beforePublishingCallback.run();
                    rateLimiterCallback.run();
                    long creationTime = System.nanoTime();
                    data = payloadCreator.create(msgSize, creationTime);
                    for (int j = 0; j < this.batchSize; j++) {
                        messages.set(j, messageProducingCallback.create(data, creationTime, client.messageBuilder()));
                    }
                    client.publish(stream, messages);
                    published.mark(this.batchSize);
                }
            };

        }).collect(Collectors.toList());

        int consumerSequence = 0;
        for (Client consumer : consumers) {
            String stream = stream();
            LOGGER.info("Starting consuming on {}", stream);
            consumer.subscribe(consumerSequence++, stream, this.offset, this.initialCredit);
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

    private String stream() {
        return streams.get(streamDispatching++ % streams.size());
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

    private interface PayloadCreator {

        byte[] create(int size, long timestamp);

    }

    private interface MessageProducingCallback {

        Message create(byte[] payload, long timestamp, MessageBuilder messageBuilder);

    }

    private interface CreationTimeExtractor {

        long extract(Message message);
    }

}
