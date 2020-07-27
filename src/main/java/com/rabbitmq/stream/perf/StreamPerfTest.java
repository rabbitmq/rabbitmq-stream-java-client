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

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.MessageBatch;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.metrics.MicrometerMetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

@CommandLine.Command(name = "stream-perf-test", mixinStandardHelpOptions = false, showDefaultValues = true,
        version = "perftest 0.1",
        description = "Tests the performance of stream queues in RabbitMQ.")
public class StreamPerfTest implements Callable<Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamPerfTest.class);
    private static final Map<String, String> CODEC_ALIASES = new HashMap<String, String>() {{
        put("qpid", QpidProtonCodec.class.getName());
        put("simple", SimpleCodec.class.getName());
    }};
    private final String[] arguments;
    private final EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
    @CommandLine.Mixin
    private final CommandLine.HelpCommand helpCommand = new CommandLine.HelpCommand();
    int addressDispatching = 0;
    int streamDispatching = 0;
    private volatile Codec codec;
    @CommandLine.Option(names = {"--username", "-u"}, description = "username to use for connecting", defaultValue = "guest")
    private String username;
    @CommandLine.Option(names = {"--password", "-pw"}, description = "password to use for connecting", defaultValue = "guest")
    private String password;
    @CommandLine.Option(names = {"--addresses", "-a"},
            description = "servers to connect to, e.g. localhost:5555, separated by commas",
            defaultValue = "localhost:5555",
            split = ","
    )
    private List<String> addrs;
    @CommandLine.Option(names = {"--producers", "-x"}, description = "number of producers", defaultValue = "1",
            converter = Utils.NotNegativeIntegerTypeConverter.class)
    private int producers;
    @CommandLine.Option(names = {"--consumers", "-y"}, description = "number of consumers", defaultValue = "1",
            converter = Utils.NotNegativeIntegerTypeConverter.class)
    private int consumers;
    @CommandLine.Option(names = {"--size", "-s"}, description = "size of messages in bytes", defaultValue = "10",
            converter = Utils.NotNegativeIntegerTypeConverter.class)
    private volatile int messageSize;
    @CommandLine.Option(names = {"--initial-credit", "-icr"},
            description = "initial credit when registering a consumer", defaultValue = "10",
            converter = Utils.NotNegativeIntegerTypeConverter.class)
    private int initialCredit;
    @CommandLine.Option(names = {"--credit", "-cr"}, description = "credit requested on acknowledgment", defaultValue = "1",
            converter = Utils.PositiveIntegerTypeConverter.class)
    private int credit;
    @CommandLine.Option(names = {"--ack", "-ac"}, description = "ack (request credit) every x chunk(s)", defaultValue = "1",
            converter = Utils.PositiveIntegerTypeConverter.class)
    private int ack;
    @CommandLine.Option(names = {"--confirms", "-c"}, description = "outstanding confirms", defaultValue = "-1")
    private int confirms;
    @CommandLine.Option(names = {"--streams", "-st"},
            description = "stream(s) to send to and consume from, separated by commas", defaultValue = "stream1",
            split = ","
    )
    private List<String> streams;
    @CommandLine.Option(names = {"--offset", "-o"}, description = "offset to start listening from. " +
            "Valid values are 'first', 'last', 'next', an unsigned long, or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z).",
            defaultValue = "first", converter = Utils.OffsetSpecificationTypeConverter.class)
    private OffsetSpecification offset;
    @CommandLine.Option(names = {"--pre-declared", "-p"}, description = "whether streams are already declared or not", defaultValue = "false")
    private boolean preDeclared;
    @CommandLine.Option(names = {"--rate", "-r"}, description = "maximum rate of published messages", defaultValue = "-1")
    private int rate;
    @CommandLine.Option(names = {"--batch-size", "-bs"}, description = "size of a batch of published messages", defaultValue = "1",
            converter = Utils.PositiveIntegerTypeConverter.class)
    private int batchSize;
    @CommandLine.Option(names = {"--sub-entry-size", "-ses"}, description = "number of messages packed into a normal message entry", defaultValue = "1",
            converter = Utils.PositiveIntegerTypeConverter.class)
    private int subEntrySize;
    @CommandLine.Option(names = {"--codec", "-cc"},
            description = "class of codec to use. Aliases: qpid, simple.",
            defaultValue = "qpid"
    )
    private String codecClass;
    @CommandLine.Option(names = {"--max-length-bytes", "-mlb"}, description = "max size of created streams",
            defaultValue = "20gb", converter = Utils.ByteCapacityTypeConverter.class)
    private ByteCapacity maxLengthBytes;
    @CommandLine.Option(names = {"--max-segment-size", "-mss"}, description = "max size of segments",
            defaultValue = "500mb", converter = Utils.ByteCapacityTypeConverter.class)
    private ByteCapacity maxSegmentSize;
    private List<Address> addresses;
    @CommandLine.Option(names = {"--version", "-v"}, description = "show version information", defaultValue = "false")
    private boolean version;
    @CommandLine.Option(names = {"--summary-file", "-sf"}, description = "generate a summary file with metrics", defaultValue = "false")
    private boolean summaryFile;

    private MetricsCollector metricsCollector;
    private PerformanceMetrics performanceMetrics;

    // constructor for completion script generation
    public StreamPerfTest() {
        this(null);
    }

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

    private static Codec createCodec(String className) {
        className = CODEC_ALIASES.getOrDefault(className, className);
        try {
            return (Codec) Class.forName(className).getConstructor().newInstance();
        } catch (Exception e) {
            throw new StreamException("Exception while creating codec " + className, e);
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

        CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
        String metricsPrefix = "rabbitmq.stream";
        this.metricsCollector = new MicrometerMetricsCollector(meterRegistry, metricsPrefix);

        Counter producerConfirm = meterRegistry.counter(metricsPrefix + ".producer_confirmed");

        this.performanceMetrics = new DefaultPerformanceMetrics(meterRegistry, metricsPrefix, this.summaryFile);

        this.messageSize = this.messageSize < 8 ? 8 : this.messageSize; // we need to store a long in it

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

        // FIXME add confirm latency

        BrokerLocator configurationBrokerLocator = new RoundRobinAddressLocator(addresses);

        if (!preDeclared) {
            for (String stream : streams) {
                Address address = configurationBrokerLocator.get(null);
                try (Client client = client(new Client.ClientParameters().host(address.host).port(address.port))) {
                    Client.Response response = client.create(stream, new Client.StreamParametersBuilder()
                            .maxLengthBytes(maxLengthBytes).maxSegmentSizeBytes(maxSegmentSize).build());

                    if (response.isOk()) {
                        LOGGER.info("Created stream {}", stream);
                    } else {
                        throw new IllegalStateException("Could not create " + stream + ", response code is " + response.getResponseCode());
                    }
                }
            }
            shutdownService.wrap(closeStep("Deleting stream(s)", () -> {
                if (!preDeclared) {
                    Address address = configurationBrokerLocator.get(null);
                    try (Client c = client(new Client.ClientParameters().host(address.host).port(address.port))) {
                        for (String stream : streams) {
                            LOGGER.debug("Deleting {}", stream);
                            Client.Response response = c.delete(stream);
                            if (!response.isOk()) {
                                LOGGER.warn("Could not delete stream {}, response code was {}", stream, response.getResponseCode());
                            }
                            LOGGER.debug("Deleted {}", stream);
                        }
                    }
                }
            }));
        }

        Map<String, Client.StreamMetadata> metadataMap = new ConcurrentHashMap<>();
        Address address = configurationBrokerLocator.get(null);
        try (Client c = client(new Client.ClientParameters().host(address.host).port(address.port))) {
            Map<String, Client.StreamMetadata> metadata = c.metadata(streams.toArray(new String[0]));
            metadataMap.putAll(metadata);
        }

        Topology topology = new MapTopology(metadataMap);

        BrokerLocator publisherBrokerLocator = new LeaderOnlyBrokerLocator(topology);
        BrokerLocator consumerBrokerLocator = new RoundRobinReplicaBrokerLocator(topology);

        // FIXME handle metadata update for consumers and publishers
        // they should at least issue a warning that their stream has been deleted and that they're now useless

        Environment environment = Environment.builder()
                .host(address.host)
                .port(address.port)
                .username(username)
                .password(password)
                .metricsCollector(metricsCollector)
                .build();

        List<Client> producers = Collections.synchronizedList(new ArrayList<>(this.producers));
        List<Runnable> producerRunnables = IntStream.range(0, this.producers).mapToObj(i -> {
            Runnable beforePublishingCallback;
            Client.PublishConfirmListener publishConfirmListener;
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
                publishConfirmListener = correlationId -> {
                    confirmsSemaphore.release();
                };
            } else {
                beforePublishingCallback = () -> {
                };
                publishConfirmListener = correlationId -> {
                };
            }

            Runnable rateLimiterCallback;
            if (this.rate > 0) {
                RateLimiter rateLimiter = com.google.common.util.concurrent.RateLimiter.create(this.rate);
//                int messageCountInFrame = this.batchSize * this.subEntrySize;
                rateLimiterCallback = () -> rateLimiter.acquire(1);
            } else {
                rateLimiterCallback = () -> {
                };
            }

            String stream = stream();
            Address publisherAddress = publisherBrokerLocator.get(stream);
            Client client = client(new Client.ClientParameters()
                    .host(publisherAddress.host).port(publisherAddress.port)
                    .publishConfirmListener(publishConfirmListener));

            producers.add(client);

            ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(this.producers);

            shutdownService.wrap(closeStep("Closing scheduled executor service", () -> scheduledExecutorService.shutdownNow()));

            shutdownService.wrap(closeStep("Closing producers", () -> {
                for (Client producer : producers) {
                    producer.close();
                }
            }));
            LOGGER.info("Connecting to {} to publish to {}", publisherAddress, stream);
            return (Runnable) () -> {
                final int msgSize = this.messageSize;
                Runnable publishCallback;
                if (this.subEntrySize == 1) {
                    List<Message> messages = new ArrayList<>(this.batchSize);
                    IntStream.range(0, this.batchSize).forEach(unused -> messages.add(null));
                    publishCallback = () -> {
                        long creationTime = System.nanoTime();
                        byte[] payload = new byte[msgSize];
                        writeLong(payload, creationTime);
                        for (int j = 0; j < this.batchSize; j++) {
                            messages.set(j, codec.messageBuilder().addData(payload).build());
                        }
                        client.publish(stream, messages);
                    };
                } else {
                    List<MessageBatch> messageBatches = new ArrayList<>(this.batchSize);
                    IntStream.range(0, this.batchSize).forEach(unused -> messageBatches.add(null));
                    List<Message> messages = new ArrayList<>(this.subEntrySize);
                    IntStream.range(0, this.subEntrySize).forEach(unused -> messages.add(null));
                    publishCallback = () -> {
                        long creationTime = System.nanoTime();
                        byte[] payload = new byte[msgSize];
                        writeLong(payload, creationTime);
                        for (int j = 0; j < this.subEntrySize; j++) {
                            messages.set(j, codec.messageBuilder().addData(payload).build());
                        }
                        for (int j = 0; j < this.batchSize; j++) {
                            messageBatches.set(j, new MessageBatch(MessageBatch.Compression.NONE, messages));
                        }
                        client.publishBatches(stream, messageBatches);
                    };
                }

                Producer producer = environment.producerBuilder()
                        .subEntrySize(this.subEntrySize)
                        .batchSize(this.batchSize)
                        .stream(stream)
                        .build();
                ConfirmationHandler confirmationHandler = confirmationStatus -> producerConfirm.increment();
                while (true && !Thread.currentThread().isInterrupted()) {
                    beforePublishingCallback.run();
                    rateLimiterCallback.run();
                    long creationTime = System.nanoTime();
                    byte[] payload = new byte[msgSize];
                    writeLong(payload, creationTime);
//                    publishCallback.run();
                    producer.send(producer.messageBuilder().addData(payload).build(), confirmationHandler);
                }


            };

        }).collect(Collectors.toList());

        AtomicInteger consumerSequence = new AtomicInteger(0);
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
                if (creditRequestCondition.getAsBoolean()) {
                    client.credit(correlationId, creditToAsk);
                }
            };
            final PerformanceMetrics metrics = this.performanceMetrics;
            Client.MessageListener messageListener = (correlationId, offset, data) -> {
//                metrics.latency(System.nanoTime() - readLong(data.getBodyAsBinary()), TimeUnit.NANOSECONDS);
            };

            String stream = stream();
            Address consumerAddress = consumerBrokerLocator.get(stream);
            Client consumer = client(new Client.ClientParameters()
                    .host(consumerAddress.host).port(consumerAddress.port)
                    .chunkListener(chunkListener)
                    .messageListener(messageListener));

            LOGGER.info("Connecting to {} to consume from {}", consumerAddress, stream);
            consumer.subscribe(consumerSequence.getAndIncrement(), stream, this.offset, this.initialCredit);

            return consumer;
        }).collect(Collectors.toList()));

        shutdownService.wrap(closeStep("Closing consumers", () -> {
            for (Client consumer : consumers) {
                consumer.close();
            }
        }));

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

        this.performanceMetrics.start(metricsHeader);
        shutdownService.wrap(closeStep("Closing metrics", () -> this.performanceMetrics.close()));

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
        return new Client(parameters.eventLoopGroup(this.eventLoopGroup).codec(codec)
                .metricsCollector(this.metricsCollector)
                .username(this.username).password(this.password)
        );
    }

    private String stream() {
        return streams.get(streamDispatching++ % streams.size());
    }

    interface BrokerLocator {

        Address get(String hint);

    }

    interface Topology {

        Client.StreamMetadata getMetadata(String stream);

    }

    static class Address {

        final String host;
        final int port;

        Address(String host, int port) {
            Objects.requireNonNull(host, "host argument cannot be null");
            this.host = host;
            this.port = port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Address address = (Address) o;
            return port == address.port &&
                    host.equals(address.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, port);
        }
    }

    static class RoundRobinAddressLocator implements BrokerLocator {

        private final List<Address> addresses;
        private final AtomicInteger sequence = new AtomicInteger(0);

        RoundRobinAddressLocator(List<Address> addresses) {
            this.addresses = addresses;
        }

        @Override
        public Address get(String hintIgnored) {
            return addresses.get(sequence.getAndIncrement() % addresses.size());
        }
    }

    static class LeaderOnlyBrokerLocator implements BrokerLocator {

        private final Topology topology;

        LeaderOnlyBrokerLocator(Topology topology) {
            this.topology = topology;
        }

        @Override
        public Address get(String hint) {
            Client.StreamMetadata metadata = topology.getMetadata(hint);
            if (metadata == null || metadata.getResponseCode() != Constants.RESPONSE_CODE_OK) {
                throw new IllegalArgumentException("Could not find metadata for stream: " + hint);
            }
            return new Address(metadata.getLeader().getHost(), metadata.getLeader().getPort());
        }
    }

    static class RoundRobinReplicaBrokerLocator implements BrokerLocator {

        private final Topology topology;
        private final Map<String, AtomicInteger> sequences = new HashMap<>();

        RoundRobinReplicaBrokerLocator(Topology topology) {
            this.topology = topology;
        }


        @Override
        public Address get(String hint) {
            Client.StreamMetadata metadata = topology.getMetadata(hint);
            if (metadata == null || metadata.getResponseCode() != Constants.RESPONSE_CODE_OK) {
                throw new IllegalArgumentException("Could not find metadata for stream: " + hint);
            }
            if (metadata.getReplicas() == null || metadata.getReplicas().isEmpty()) {
                return new Address(metadata.getLeader().getHost(), metadata.getLeader().getPort());
            } else {
                AtomicInteger sequence = sequences.computeIfAbsent(hint, s -> new AtomicInteger(0));
                Client.Broker replica = metadata.getReplicas().get(sequence.getAndIncrement() % metadata.getReplicas().size());
                return new Address(replica.getHost(), replica.getPort());
            }
        }
    }

    static class MapTopology implements Topology {

        private static final Comparator<Client.Broker> BROKER_COMPARATOR = Comparator.comparing(Client.Broker::getHost)
                .thenComparingInt(Client.Broker::getPort);

        private final Map<String, Client.StreamMetadata> metadata;

        MapTopology(Map<String, Client.StreamMetadata> metadata) {
            for (Map.Entry<String, Client.StreamMetadata> entry : metadata.entrySet()) {
                ArrayList<Client.Broker> replicas = new ArrayList<>(entry.getValue().getReplicas());
                Collections.sort(replicas, BROKER_COMPARATOR);
                entry.setValue(new Client.StreamMetadata(
                        entry.getValue().getStream(), entry.getValue().getResponseCode(),
                        entry.getValue().getLeader(),
                        replicas
                ));
            }

            this.metadata = metadata;
        }

        @Override
        public Client.StreamMetadata getMetadata(String stream) {
            return metadata.get(stream);
        }
    }

}
