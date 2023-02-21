// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.perf.Utils.ENVIRONMENT_VARIABLE_LOOKUP;
import static com.rabbitmq.stream.perf.Utils.ENVIRONMENT_VARIABLE_PREFIX;
import static com.rabbitmq.stream.perf.Utils.OPTION_TO_ENVIRONMENT_VARIABLE;
import static java.lang.String.format;
import static java.time.Duration.ofMillis;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AddressResolver;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.EnvironmentBuilder.TlsConfiguration;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.perf.ShutdownService.CloseCallback;
import com.rabbitmq.stream.perf.Utils.CreditSettings;
import com.rabbitmq.stream.perf.Utils.NamedThreadFactory;
import com.rabbitmq.stream.perf.Utils.PerformanceMicrometerMetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.internal.PlatformDependent;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@CommandLine.Command(
    name = "stream-perf-test",
    mixinStandardHelpOptions = false,
    showDefaultValues = true,
    description = "Tests the performance of stream queues in RabbitMQ.")
public class StreamPerfTest implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamPerfTest.class);
  private static final Map<String, String> CODEC_ALIASES =
      new HashMap<String, String>() {
        {
          put("qpid", QpidProtonCodec.class.getName());
          put("simple", SimpleCodec.class.getName());
        }
      };
  private final String[] arguments;

  @CommandLine.Mixin
  private final CommandLine.HelpCommand helpCommand = new CommandLine.HelpCommand();
  // for testing
  private final AddressResolver addressResolver;
  private final PrintWriter err, out;
  @Spec CommandSpec spec; // injected by picocli
  int streamDispatching = 0;

  @CommandLine.Option(
      names = {"--uris", "-u"},
      description =
          "servers to connect to, e.g. rabbitmq-stream://localhost:5552, separated by commas",
      defaultValue = "rabbitmq-stream://localhost:5552",
      split = ",")
  private List<String> uris;

  @CommandLine.Option(
      names = {"--producers", "-x"},
      description = "number of producers",
      defaultValue = "1",
      converter = Utils.NotNegativeIntegerTypeConverter.class)
  private int producers;

  @CommandLine.Option(
      names = {"--consumers", "-y"},
      description = "number of consumers",
      defaultValue = "1",
      converter = Utils.NotNegativeIntegerTypeConverter.class)
  private int consumers;

  @CommandLine.Option(
      names = {"--size", "-s"},
      description = "size of messages in bytes",
      defaultValue = "10",
      converter = Utils.NotNegativeIntegerTypeConverter.class)
  private volatile int messageSize;

  @CommandLine.Option(
      names = {"--confirms", "-c"},
      description = "outstanding confirms",
      defaultValue = "10000",
      converter = Utils.NotNegativeIntegerTypeConverter.class)
  private int confirms;

  @CommandLine.Option(
      names = {"--stream-count", "-sc"},
      description = "number of streams to send and consume from. Examples: 10, 1-10.",
      defaultValue = "1",
      converter = Utils.RangeTypeConverter.class)
  private String streamCount;

  @CommandLine.Option(
      names = {"--streams", "-st"},
      description = "stream(s) to send to and consume from, separated by commas",
      defaultValue = "stream",
      split = ",")
  private List<String> streams;

  @CommandLine.Option(
      names = {"--delete-streams", "-ds"},
      description = "whether to delete stream(s) after the run or not",
      defaultValue = "false")
  private boolean deleteStreams;

  @CommandLine.Option(
      names = {"--offset", "-o"},
      description =
          "offset to start listening from. "
              + "Valid values are 'first', 'last', 'next', an unsigned long, "
              + "or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z).",
      defaultValue = "next",
      converter = Utils.OffsetSpecificationTypeConverter.class)
  private OffsetSpecification offset;

  @CommandLine.Option(
      names = {"--rate", "-r"},
      description = "maximum rate of published messages",
      defaultValue = "-1")
  private int rate;

  @CommandLine.Option(
      names = {"--batch-size", "-bs"},
      description = "size of a batch of published messages",
      defaultValue = "100",
      converter = Utils.PositiveIntegerTypeConverter.class)
  private int batchSize;

  @CommandLine.Option(
      names = {"--batch-publishing-delay", "-bpd"},
      description = "Period to send a batch of messages in milliseconds",
      defaultValue = "100",
      converter = Utils.GreaterThanOrEqualToZeroIntegerTypeConverter.class)
  private int batchPublishingDelay;

  @CommandLine.Option(
      names = {"--sub-entry-size", "-ses"},
      description = "number of messages packed into a normal message entry",
      defaultValue = "1",
      converter = Utils.PositiveIntegerTypeConverter.class)
  private int subEntrySize;

  @CommandLine.Option(
      names = {"--compression", "-co"},
      description =
          "compression codec to use for sub-entries. Values: none, gzip, snappy, lz4, zstd.",
      defaultValue = "none",
      converter = Utils.CompressionTypeConverter.class)
  private Compression compression;

  @CommandLine.Option(
      names = {"--codec", "-cc"},
      description = "class of codec to use. Aliases: qpid, simple.",
      defaultValue = "qpid")
  private String codecClass;

  @CommandLine.Option(
      names = {"--max-length-bytes", "-mlb"},
      description = "max size of created streams, use 0 for no limit",
      defaultValue = "20gb",
      converter = Utils.ByteCapacityTypeConverter.class)
  private ByteCapacity maxLengthBytes;

  private ByteCapacity maxSegmentSize;

  @CommandLine.Option(
      names = {"--max-age", "-ma"},
      description =
          "max age of segments using the ISO 8601 duration format, "
              + "e.g. PT10M30S for 10 minutes 30 seconds, P5DT8H for 5 days 8 hours.",
      converter = Utils.DurationTypeConverter.class)
  private Duration maxAge;

  @CommandLine.Option(
      names = {"--leader-locator", "-ll"},
      description =
          "leader locator strategy for created stream. "
              + "Possible values: client-local, balanced (RabbitMQ 3.10), least-leaders, random.",
      converter = Utils.LeaderLocatorTypeConverter.class,
      defaultValue = "least-leaders")
  private LeaderLocator leaderLocator;

  @CommandLine.Option(
      names = {"--store-every", "-se"},
      description = "the frequency of offset storage",
      defaultValue = "0")
  private int storeEvery;

  @CommandLine.Option(
      names = {"--version", "-v"},
      description = "show version information",
      defaultValue = "false")
  private boolean version;

  @CommandLine.Option(
      names = {"--summary-file", "-sf"},
      description = "generate a summary file with metrics",
      defaultValue = "false")
  private boolean summaryFile;

  @CommandLine.Option(
      names = {"--producers-by-connection", "-pbc"},
      description = "number of producers by connection. Value must be between 1 and 255.",
      defaultValue = "1",
      converter = Utils.OneTo255RangeIntegerTypeConverter.class)
  private int producersByConnection;

  @CommandLine.Option(
      names = {"--producer-names", "-pn"},
      description =
          "naming strategy for producer names. Valid values are 'uuid' or a pattern with "
              + "stream name and producer index as arguments. "
              + "If set, a publishing ID is automatically assigned to each outbound message.",
      defaultValue = "",
      showDefaultValue = CommandLine.Help.Visibility.NEVER,
      converter = Utils.NameStrategyConverter.class)
  private BiFunction<String, Integer, String> producerNameStrategy;

  @CommandLine.Option(
      names = {"--tracking-consumers-by-connection", "-ccbc"},
      description = "number of tracking consumers by connection. Value must be between 1 and 255.",
      defaultValue = "50",
      converter = Utils.OneTo255RangeIntegerTypeConverter.class)
  private int trackingConsumersByConnection;

  @CommandLine.Option(
      names = {"--consumers-by-connection", "-cbc"},
      description = "number of consumers by connection. Value must be between 1 and 255.",
      defaultValue = "1",
      converter = Utils.OneTo255RangeIntegerTypeConverter.class)
  private int consumersByConnection;

  @CommandLine.Option(
      names = {"--load-balancer", "-lb"},
      description = "assume URIs point to a load balancer",
      defaultValue = "false")
  private boolean loadBalancer;

  @CommandLine.Option(
      names = {"--consumer-names", "-cn"},
      description =
          "naming strategy for consumer names. Valid values are 'uuid' or a pattern with "
              + "stream name and consumer index as arguments.",
      defaultValue = "%s-%d",
      converter = Utils.NameStrategyConverter.class)
  private BiFunction<String, Integer, String> consumerNameStrategy;

  @CommandLine.Option(
      names = {"--metrics-byte-rates", "-mbr"},
      description = "include written and read byte rates in metrics",
      defaultValue = "false")
  private boolean includeByteRates;

  @CommandLine.Option(
      names = {"--memory-report", "-mr"},
      description = "report information on memory settings and usage",
      defaultValue = "false")
  private boolean memoryReport;

  @CommandLine.Option(
      names = {"--server-name-indication", "-sni"},
      description = "server names for Server Name Indication TLS parameter, separated by commas",
      defaultValue = "",
      converter = Utils.SniServerNamesConverter.class)
  private List<SNIServerName> sniServerNames;

  @CommandLine.Option(
      names = {"--monitoring-port", "-mp"},
      description = "port to launch HTTP monitoring on",
      defaultValue = "8080")
  private int monitoringPort;

  @CommandLine.Option(
      names = {"--environment-variables", "-env"},
      description = "show usage with environment variables",
      defaultValue = "false")
  private boolean environmentVariables;

  @CommandLine.Option(
      names = {"--rpc-timeout", "-rt"},
      description = "RPC timeout in seconds",
      defaultValue = "10",
      converter = Utils.PositiveIntegerTypeConverter.class)
  private int rpcTimeout;

  @CommandLine.Option(
      names = {"--confirm-latency", "-cl"},
      description = "evaluate confirm latency",
      defaultValue = "false")
  private boolean confirmLatency;

  @CommandLine.Option(
      names = {"--super-streams", "-sst"},
      description = "use super streams",
      defaultValue = "false")
  private boolean superStreams;

  @CommandLine.Option(
      names = {"--super-stream-partitions", "-ssp"},
      description = "number of partitions for the super streams",
      defaultValue = "3",
      converter = Utils.PositiveIntegerTypeConverter.class)
  private int superStreamsPartitions;

  @CommandLine.Option(
      names = {"--single-active-consumer", "-sac"},
      description = "use single active consumer",
      defaultValue = "false")
  private boolean singleActiveConsumer;

  @CommandLine.Option(
      names = {"--amqp-uri", "-au"},
      description = "AMQP URI to use to create super stream topology")
  private String amqpUri;

  @CommandLine.Option(
      names = {"--time", "-z"},
      description = "run duration in seconds, unlimited by default",
      defaultValue = "0",
      converter = Utils.GreaterThanOrEqualToZeroIntegerTypeConverter.class)
  private int time;

  @CommandLine.Option(
      names = {"--metrics-tags", "-mt"},
      description = "metrics tags as key-value pairs separated by commas",
      defaultValue = "",
      converter = Utils.MetricsTagsTypeConverter.class)
  private Collection<Tag> metricsTags;

  @CommandLine.Option(
      names = {"--metrics-command-line-arguments", "-mcla"},
      description = "add fixed metrics with command line arguments label",
      defaultValue = "false")
  private boolean metricsCommandLineArguments;

  @CommandLine.Option(
      names = {"--credits", "-cr"},
      description = "initial and additional credits for subscriptions",
      defaultValue = "10:1",
      converter = Utils.CreditsTypeConverter.class)
  private CreditSettings credits;

  @CommandLine.Option(
      names = {"--requested-max-frame-size", "-rmfs"},
      description = "maximum frame size to request",
      defaultValue = "1048576",
      converter = Utils.ByteCapacityTypeConverter.class)
  private ByteCapacity requestedMaxFrameSize;

  @CommandLine.Option(
      names = {"--native-epoll", "-ne"},
      description = "use Netty's native epoll transport (Linux x86-64 only)",
      defaultValue = "false")
  private boolean nativeEpoll;

  @ArgGroup(exclusive = false, multiplicity = "0..1")
  InstanceSyncOptions instanceSyncOptions;

  static class InstanceSyncOptions {

    @CommandLine.Option(
        names = {"--id"},
        description = "Instance ID, for instance synchronization",
        required = true)
    private String id;

    @CommandLine.Option(
        names = {"--expected-instances", "-ei"},
        description =
            "number of expected StreamPerfTest instances "
                + "to synchronize. Default is 0, that is no synchronization."
                + "Test ID is mandatory when instance synchronization is in use.",
        converter = Utils.GreaterThanOrEqualToZeroIntegerTypeConverter.class,
        required = true)
    private int expectedInstances;

    @CommandLine.Option(
        names = {"--instance-sync-timeout", "-ist"},
        description = "Instance synchronization time " + "in seconds. Default is 600 seconds.",
        defaultValue = "600",
        converter = Utils.PositiveIntegerTypeConverter.class,
        required = false)
    private int instanceSyncTimeout;

    @CommandLine.Option(
        names = {"--instance-sync-namespace", "-isn"},
        description = "Kubernetes namespace for " + "instance synchronization",
        defaultValue = "",
        required = false)
    private String instanceSyncNamespace;
  }

  private MetricsCollector metricsCollector;
  private PerformanceMetrics performanceMetrics;
  private List<Monitoring> monitorings;
  private volatile Environment environment;
  private volatile EventLoopGroup eventLoopGroup;
  // constructor for completion script generation
  public StreamPerfTest() {
    this(null, null, null, null);
  }

  public StreamPerfTest(
      String[] arguments,
      PrintStream consoleOut,
      PrintStream consoleErr,
      AddressResolver addressResolver) {
    this.arguments = arguments;
    if (consoleOut == null) {
      consoleOut = System.out;
    }
    if (consoleErr == null) {
      consoleErr = System.err;
    }
    this.out = new PrintWriter(consoleOut, true);
    this.err = new PrintWriter(consoleErr, true);
    this.addressResolver = addressResolver;
  }

  public static void main(String[] args) throws IOException {
    LogUtils.configureLog();
    int exitCode = run(args, System.out, System.err, null).exitCode();
    System.exit(exitCode);
  }

  static RunContext run(
      String[] args,
      PrintStream consoleOut,
      PrintStream consoleErr,
      AddressResolver addressResolver) {
    StreamPerfTest streamPerfTest =
        new StreamPerfTest(args, consoleOut, consoleErr, addressResolver);
    CommandLine commandLine =
        new CommandLine(streamPerfTest).setOut(streamPerfTest.out).setErr(streamPerfTest.err);

    List<Monitoring> monitorings =
        Arrays.asList(new DebugEndpointMonitoring(), new PrometheusEndpointMonitoring());

    monitorings.forEach(m -> commandLine.addMixin(m.getClass().getSimpleName(), m));

    streamPerfTest.monitorings(monitorings);
    return new RunContext(commandLine.execute(args), streamPerfTest);
  }

  static void versionInformation(PrintWriter out) {
    String lineSeparator = System.getProperty("line.separator");
    String version =
        format(
            "RabbitMQ Stream Perf Test %s (%s; %s)",
            Version.VERSION, Version.BUILD, Version.BUILD_TIMESTAMP);
    String info =
        format(
            "Java version: %s, vendor: %s"
                + lineSeparator
                + "Java home: %s"
                + lineSeparator
                + "Default locale: %s, platform encoding: %s"
                + lineSeparator
                + "OS name: %s, version: %s, arch: %s",
            System.getProperty("java.version"),
            System.getProperty("java.vendor"),
            System.getProperty("java.home"),
            Locale.getDefault().toString(),
            Charset.defaultCharset(),
            System.getProperty("os.name"),
            System.getProperty("os.version"),
            System.getProperty("os.arch"));
    out.println("\u001B[1m" + version);
    out.println("\u001B[0m" + info);
  }

  private static Codec createCodec(String className) {
    className = CODEC_ALIASES.getOrDefault(className, className);
    try {
      return (Codec) Class.forName(className).getConstructor().newInstance();
    } catch (Exception e) {
      throw new StreamException("Exception while creating codec " + className, e);
    }
  }

  private static boolean isTls(Collection<String> uris) {
    return uris.stream().anyMatch(uri -> uri.toLowerCase().startsWith("rabbitmq-stream+tls"));
  }

  private static String stream(List<String> streams, int i) {
    return streams.get(i % streams.size());
  }

  @CommandLine.Option(
      names = {"--stream-max-segment-size-bytes", "-smssb"},
      description = "max size of segments",
      defaultValue = "500mb",
      converter = Utils.ByteCapacityTypeConverter.class)
  public void setMaxSegmentSize(ByteCapacity in) {
    if (in != null && in.compareTo(StreamCreator.MAX_SEGMENT_SIZE) > 0) {
      throw new ParameterException(
          spec.commandLine(),
          "The maximum segment size cannot be more than " + StreamCreator.MAX_SEGMENT_SIZE);
    }
    this.maxSegmentSize = in;
  }

  @Override
  public Integer call() throws Exception {
    maybeDisplayVersion();
    maybeDisplayEnvironmentVariablesHelp();
    overridePropertiesWithEnvironmentVariables();

    Codec codec = createCodec(this.codecClass);

    ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;

    CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
    meterRegistry.config().commonTags(this.metricsTags);
    String metricsPrefix = "rabbitmq.stream";
    if (this.metricsCommandLineArguments) {
      Tags tags;
      if (this.arguments == null || this.arguments.length == 0) {
        tags = Tags.of("command_line", "");
      } else {
        tags = Tags.of("command_line", Utils.commandLineMetrics(this.arguments));
      }
      Gauge.builder(metricsPrefix + ".args", () -> Integer.valueOf(1))
          .tags(tags)
          .register(meterRegistry);
    }
    this.metricsCollector = new PerformanceMicrometerMetricsCollector(meterRegistry, metricsPrefix);

    Counter producerConfirm = meterRegistry.counter(metricsPrefix + ".producer_confirmed");

    Supplier<String> memoryReportSupplier;
    if (this.memoryReport) {
      long physicalMemory = Utils.physicalMemory();
      String physicalMemoryReport =
          physicalMemory == 0
              ? ""
              : format(
                  ", physical memory %s (%d bytes)",
                  Utils.formatByte(physicalMemory), physicalMemory);
      this.out.println(
          format(
              "Max memory %s (%d bytes), max direct memory %s (%d bytes)%s",
              Utils.formatByte(Runtime.getRuntime().maxMemory()),
              Runtime.getRuntime().maxMemory(),
              Utils.formatByte(PlatformDependent.maxDirectMemory()),
              PlatformDependent.maxDirectMemory(),
              physicalMemoryReport));

      if (byteBufAllocator instanceof ByteBufAllocatorMetricProvider) {
        ByteBufAllocatorMetric allocatorMetric =
            ((ByteBufAllocatorMetricProvider) byteBufAllocator).metric();
        memoryReportSupplier =
            () -> {
              long usedHeapMemory = allocatorMetric.usedHeapMemory();
              long usedDirectMemory = allocatorMetric.usedDirectMemory();
              return format(
                  "Used heap memory %s (%d bytes), used direct memory %s (%d bytes)",
                  Utils.formatByte(usedHeapMemory),
                  usedHeapMemory,
                  Utils.formatByte(usedDirectMemory),
                  usedDirectMemory);
            };
      } else {
        memoryReportSupplier = () -> "";
      }
    } else {
      memoryReportSupplier = () -> "";
    }

    this.messageSize = this.messageSize < 8 ? 8 : this.messageSize; // we need to store a long in it

    ShutdownService shutdownService = new ShutdownService();
    Thread shutdownServiceShutdownHook = new Thread(() -> shutdownService.close());
    Runtime.getRuntime().addShutdownHook(shutdownServiceShutdownHook);

    try {

      if (meterRegistry.getRegistries().isEmpty()) {
        // we need at least one to do the calculations
        meterRegistry.add(Utils.dropwizardMeterRegistry());
      }

      this.performanceMetrics =
          new DefaultPerformanceMetrics(
              meterRegistry,
              metricsPrefix,
              this.summaryFile,
              this.includeByteRates,
              this.confirmLatency,
              memoryReportSupplier,
              this.out);

      ScheduledExecutorService envExecutor =
          Executors.newScheduledThreadPool(
              Math.max(Runtime.getRuntime().availableProcessors(), this.producers),
              new NamedThreadFactory("stream-perf-test-env-"));

      shutdownService.wrap(
          closeStep("Closing environment executor", () -> envExecutor.shutdownNow()));

      boolean tls = isTls(this.uris);
      AddressResolver addrResolver;
      if (loadBalancer) {
        int defaultPort = tls ? Client.DEFAULT_TLS_PORT : Client.DEFAULT_PORT;
        List<Address> addresses =
            this.uris.stream()
                .map(
                    uri -> {
                      try {
                        return new URI(uri);
                      } catch (URISyntaxException e) {
                        throw new IllegalArgumentException(
                            "Error while parsing URI " + uri + ": " + e.getMessage());
                      }
                    })
                .map(
                    uriItem ->
                        new Address(
                            uriItem.getHost() == null ? "localhost" : uriItem.getHost(),
                            uriItem.getPort() == -1 ? defaultPort : uriItem.getPort()))
                .collect(Collectors.toList());
        AtomicInteger connectionAttemptCount = new AtomicInteger(0);
        addrResolver =
            address -> addresses.get(connectionAttemptCount.getAndIncrement() % addresses.size());
      } else {
        if (this.addressResolver == null) {
          addrResolver = address -> address;
        } else {
          addrResolver = this.addressResolver; // should happen only in tests
        }
      }

      java.util.function.Consumer<Bootstrap> bootstrapCustomizer;
      if (this.nativeEpoll) {
        this.eventLoopGroup = new EpollEventLoopGroup();
        bootstrapCustomizer = b -> b.channel(EpollSocketChannel.class);
      } else {
        this.eventLoopGroup = new NioEventLoopGroup();
        bootstrapCustomizer = b -> {};
      }

      EnvironmentBuilder environmentBuilder =
          Environment.builder()
              .id("stream-perf-test")
              .uris(this.uris)
              .addressResolver(addrResolver)
              .scheduledExecutorService(envExecutor)
              .metricsCollector(metricsCollector)
              .netty()
              .byteBufAllocator(byteBufAllocator)
              .eventLoopGroup(eventLoopGroup)
              .bootstrapCustomizer(bootstrapCustomizer)
              .environmentBuilder()
              .codec(codec)
              .maxProducersByConnection(this.producersByConnection)
              .maxTrackingConsumersByConnection(this.trackingConsumersByConnection)
              .maxConsumersByConnection(this.consumersByConnection)
              .rpcTimeout(Duration.ofSeconds(this.rpcTimeout))
              .requestedMaxFrameSize((int) this.requestedMaxFrameSize.toBytes());

      java.util.function.Consumer<io.netty.channel.Channel> channelCustomizer = channel -> {};

      if (tls) {
        TlsConfiguration tlsConfiguration = environmentBuilder.tls();
        tlsConfiguration =
            tlsConfiguration.sslContext(
                SslContextBuilder.forClient()
                    .trustManager(Utils.TRUST_EVERYTHING_TRUST_MANAGER)
                    .build());
        environmentBuilder = tlsConfiguration.environmentBuilder();
        if (!this.sniServerNames.isEmpty()) {
          channelCustomizer =
              channelCustomizer.andThen(
                  ch -> {
                    SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
                    if (sslHandler != null) {
                      SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
                      sslParameters.setServerNames(this.sniServerNames);
                      sslHandler.engine().setSSLParameters(sslParameters);
                    }
                  });
        }
      }

      this.environment =
          environmentBuilder
              .netty()
              .channelCustomizer(channelCustomizer)
              .environmentBuilder()
              .build();
      if (!isRunTimeLimited()) {
        shutdownService.wrap(
            closeStep(
                "Closing Netty event loop group",
                () -> {
                  if (!eventLoopGroup.isShuttingDown() || !eventLoopGroup.isShutdown()) {
                    eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
                  }
                }));
        shutdownService.wrap(closeStep("Closing environment", () -> environment.close()));
      }

      MonitoringContext monitoringContext =
          new MonitoringContext(this.monitoringPort, meterRegistry, environment, this.out);
      this.monitorings.forEach(m -> m.configure(monitoringContext));

      streams = Utils.streams(this.streamCount, this.streams);

      AtomicReference<Channel> amqpChannel = new AtomicReference<>();
      Connection amqpConnection;
      if (this.superStreams) {
        amqpConnection = Utils.amqpConnection(this.amqpUri, uris, tls, this.sniServerNames);
        if (this.deleteStreams) {
          // we keep it open for deletion, so adding a close step
          shutdownService.wrap(
              closeStep("Closing AMQP connection for super streams", () -> amqpConnection.close()));
        }
        amqpChannel.set(amqpConnection.createChannel());
      } else {
        amqpConnection = null;
      }

      for (String stream : streams) {
        if (this.superStreams) {
          List<String> partitions =
              Utils.superStreamPartitions(stream, this.superStreamsPartitions);
          for (String partition : partitions) {
            createStream(environment, partition);
          }

          Utils.declareSuperStreamExchangeAndBindings(amqpChannel.get(), stream, partitions);

        } else {
          createStream(environment, stream);
        }
      }

      if (this.deleteStreams) {
        shutdownService.wrap(
            closeStep(
                "Deleting stream(s)",
                () -> {
                  for (String stream : streams) {
                    if (this.superStreams) {
                      List<String> partitions =
                          Utils.superStreamPartitions(stream, this.superStreamsPartitions);
                      for (String partition : partitions) {
                        environment.deleteStream(partition);
                      }
                      Utils.deleteSuperStreamExchange(amqpChannel.get(), stream);

                    } else {
                      LOGGER.debug("Deleting {}", stream);
                      try {
                        environment.deleteStream(stream);
                        LOGGER.debug("Deleted {}", stream);
                      } catch (Exception e) {
                        LOGGER.warn("Could not delete stream {}: {}", stream, e.getMessage());
                      }
                    }
                  }
                }));
      } else {
        if (this.superStreams) {
          // we don't want to delete the super streams at the end, so we close the AMQP connection
          amqpConnection.close();
        }
      }

      List<Producer> producers = Collections.synchronizedList(new ArrayList<>(this.producers));
      List<Runnable> producerRunnables =
          IntStream.range(0, this.producers)
              .mapToObj(
                  i -> {
                    Runnable rateLimiterCallback;
                    if (this.rate > 0) {
                      RateLimiter rateLimiter = RateLimiter.create(this.rate);
                      rateLimiterCallback = () -> rateLimiter.acquire(1);
                    } else {
                      rateLimiterCallback = () -> {};
                    }

                    String stream = stream(this.streams, i);
                    ProducerBuilder producerBuilder =
                        environment
                            .producerBuilder()
                            .batchPublishingDelay(ofMillis(this.batchPublishingDelay));

                    String producerName = this.producerNameStrategy.apply(stream, i + 1);
                    if (producerName != null && !producerName.trim().isEmpty()) {
                      producerBuilder =
                          producerBuilder.name(producerName).confirmTimeout(Duration.ZERO);
                    }

                    java.util.function.Consumer<MessageBuilder> messageBuilderConsumer;
                    if (this.superStreams) {
                      producerBuilder
                          .superStream(stream)
                          .routing(msg -> msg.getProperties().getMessageIdAsString());
                      AtomicLong messageIdSequence = new AtomicLong(0);
                      messageBuilderConsumer =
                          mg -> mg.properties().messageId(messageIdSequence.getAndIncrement());
                    } else {
                      messageBuilderConsumer = mg -> {};
                      producerBuilder.stream(stream);
                    }

                    Producer producer =
                        producerBuilder
                            .subEntrySize(this.subEntrySize)
                            .batchSize(this.batchSize)
                            .compression(
                                this.compression == Compression.NONE ? null : this.compression)
                            .maxUnconfirmedMessages(this.confirms)
                            .build();

                    AtomicLong messageCount = new AtomicLong(0);
                    ConfirmationHandler confirmationHandler;
                    if (this.confirmLatency) {
                      final PerformanceMetrics metrics = this.performanceMetrics;
                      final int divisor = Utils.downSamplingDivisor(this.rate);
                      confirmationHandler =
                          confirmationStatus -> {
                            if (confirmationStatus.isConfirmed()) {
                              producerConfirm.increment();
                              // at very high throughput ( > 1 M / s), the histogram can
                              // become a bottleneck,
                              // so we downsample and calculate latency for every x message
                              // this should not affect the metric much
                              if (messageCount.incrementAndGet() % divisor == 0) {
                                try {
                                  long time =
                                      Utils.readLong(
                                          confirmationStatus.getMessage().getBodyAsBinary());
                                  // see below why we use current time to measure latency
                                  metrics.confirmLatency(
                                      System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
                                } catch (Exception e) {
                                  // not able to read the body, something wrong?
                                }
                              }
                            }
                          };
                    } else {
                      confirmationHandler =
                          confirmationStatus -> {
                            if (confirmationStatus.isConfirmed()) {
                              producerConfirm.increment();
                            }
                          };
                    }

                    producers.add(producer);

                    return (Runnable)
                        () -> {
                          final int msgSize = this.messageSize;

                          try {
                            while (true && !Thread.currentThread().isInterrupted()) {
                              rateLimiterCallback.run();
                              // Using current time for interoperability with other tools
                              // and also across different processes.
                              // This is good enough to measure duration/latency this way
                              // in a performance tool.
                              long creationTime = System.currentTimeMillis();
                              byte[] payload = new byte[msgSize];
                              Utils.writeLong(payload, creationTime);
                              MessageBuilder messageBuilder = producer.messageBuilder();
                              messageBuilderConsumer.accept(messageBuilder);
                              producer.send(
                                  messageBuilder.addData(payload).build(), confirmationHandler);
                            }
                          } catch (Exception e) {
                            if (e instanceof InterruptedException
                                || (e.getCause() != null
                                    && e.getCause() instanceof InterruptedException)) {
                              LOGGER.info("Publisher #{} thread interrupted", i, e);
                            } else {
                              LOGGER.warn("Publisher #{} crashed", i, e);
                            }
                          }
                        };
                  })
              .collect(Collectors.toList());

      if (this.instanceSyncOptions != null) {
        String namespace = this.instanceSyncOptions.instanceSyncNamespace;
        if (namespace == null || namespace.trim().isEmpty()) {
          namespace = System.getenv("MY_POD_NAMESPACE");
        }
        InstanceSynchronization instanceSynchronization =
            Utils.defaultInstanceSynchronization(
                this.instanceSyncOptions.id,
                this.instanceSyncOptions.expectedInstances,
                namespace,
                Duration.ofSeconds(this.instanceSyncOptions.instanceSyncTimeout),
                this.out);
        instanceSynchronization.synchronize();
      }

      monitoringContext.start();
      shutdownService.wrap(closeStep("Closing monitoring context", monitoringContext::close));

      List<Consumer> consumers =
          Collections.synchronizedList(
              IntStream.range(0, this.consumers)
                  .mapToObj(
                      i -> {
                        final PerformanceMetrics metrics = this.performanceMetrics;

                        AtomicLong messageCount = new AtomicLong(0);
                        String stream = stream(streams, i);
                        ConsumerBuilder consumerBuilder =
                            environment
                                .consumerBuilder()
                                .offset(this.offset)
                                .credits(this.credits.initial(), this.credits.additional());

                        if (this.superStreams) {
                          consumerBuilder.superStream(stream);
                        } else {
                          consumerBuilder.stream(stream);
                        }

                        if (this.singleActiveConsumer) {
                          consumerBuilder.singleActiveConsumer();
                          // single active consumer requires a name
                          if (this.storeEvery == 0) {
                            this.storeEvery = 10_000;
                          }
                        }

                        if (this.storeEvery > 0) {
                          String consumerName = this.consumerNameStrategy.apply(stream, i + 1);
                          consumerBuilder =
                              consumerBuilder
                                  .name(consumerName)
                                  .autoTrackingStrategy()
                                  .messageCountBeforeStorage(this.storeEvery)
                                  .builder();
                        }

                        // we assume the publishing rate is the same order as the consuming rate
                        // we actually don't want to downsample for low rates
                        final int divisor = Utils.downSamplingDivisor(this.rate);
                        consumerBuilder =
                            consumerBuilder.messageHandler(
                                (context, message) -> {
                                  // at very high throughput ( > 1 M / s), the histogram can
                                  // become a bottleneck,
                                  // so we downsample and calculate latency for every x message
                                  // this should not affect the metric much
                                  if (messageCount.incrementAndGet() % divisor == 0) {
                                    try {
                                      long time = Utils.readLong(message.getBodyAsBinary());
                                      // see above why we use current time to measure latency
                                      metrics.latency(
                                          System.currentTimeMillis() - time, TimeUnit.MILLISECONDS);
                                    } catch (Exception e) {
                                      // not able to read the body, maybe not a message from the
                                      // tool
                                    }
                                    metrics.offset(context.offset());
                                  }
                                });

                        Consumer consumer = consumerBuilder.build();
                        return consumer;
                      })
                  .collect(Collectors.toList()));

      if (!isRunTimeLimited()) {
        shutdownService.wrap(
            closeStep(
                "Closing consumers",
                () -> {
                  for (Consumer consumer : consumers) {
                    consumer.close();
                  }
                }));
      }

      ExecutorService executorService;
      if (this.producers > 0) {
        executorService =
            Executors.newFixedThreadPool(
                this.producers, new NamedThreadFactory("stream-perf-test-publishers-"));
        for (Runnable producer : producerRunnables) {
          this.out.println("Starting producer");
          executorService.submit(producer);
        }
      } else {
        executorService = null;
      }

      if (!isRunTimeLimited()) {
        shutdownService.wrap(
            closeStep(
                "Closing producers",
                () -> {
                  for (Producer p : producers) {
                    p.close();
                  }
                }));

        shutdownService.wrap(
            closeStep(
                "Closing producers executor service",
                () -> {
                  if (executorService != null) {
                    executorService.shutdownNow();
                  }
                }));
      }

      String metricsHeader = "Arguments: " + String.join(" ", arguments);

      this.performanceMetrics.start(metricsHeader);
      shutdownService.wrap(closeStep("Closing metrics", () -> this.performanceMetrics.close()));

      CountDownLatch latch = new CountDownLatch(1);

      Thread shutdownHook = new Thread(() -> latch.countDown());
      Runtime.getRuntime().addShutdownHook(shutdownHook);
      try {
        if (isRunTimeLimited()) {
          latch.await(this.time, TimeUnit.SECONDS);
        } else {
          latch.await();
        }
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (InterruptedException e) {
        // moving on to the closing sequence
      }
    } finally {
      shutdownService.close();
      Runtime.getRuntime().removeShutdownHook(shutdownServiceShutdownHook);
    }

    return 0;
  }

  private void createStream(Environment environment, String stream) {
    StreamCreator streamCreator =
        environment.streamCreator().stream(stream)
            .maxSegmentSizeBytes(this.maxSegmentSize)
            .leaderLocator(this.leaderLocator);

    if (this.maxLengthBytes.toBytes() != 0) {
      streamCreator.maxLengthBytes(this.maxLengthBytes);
    }

    if (this.maxAge != null) {
      streamCreator.maxAge(this.maxAge);
    }

    try {
      streamCreator.create();
    } catch (StreamException e) {
      if (e.getCode() == Constants.RESPONSE_CODE_PRECONDITION_FAILED) {
        String message =
            String.format(
                "Warning: stream '%s' already exists, but with different properties than "
                    + "max-length-bytes=%s, stream-max-segment-size-bytes=%s, queue-leader-locator=%s",
                stream, this.maxLengthBytes, this.maxSegmentSize, this.leaderLocator);
        if (this.maxAge != null) {
          message += String.format(", max-age=%s", this.maxAge);
        }
        this.out.println(message);
      } else {
        throw e;
      }
    }
  }

  private void overridePropertiesWithEnvironmentVariables() throws Exception {
    Function<String, String> optionToEnvMappings =
        OPTION_TO_ENVIRONMENT_VARIABLE
            .andThen(ENVIRONMENT_VARIABLE_PREFIX)
            .andThen(ENVIRONMENT_VARIABLE_LOOKUP);

    Utils.assignValuesToCommand(this, optionToEnvMappings);
    this.monitorings.forEach(
        command -> {
          try {
            Utils.assignValuesToCommand(command, optionToEnvMappings);
          } catch (Exception e) {
            LOGGER.warn(
                "Error while trying to assign environment variables to command {}",
                command.getClass());
          }
        });
  }

  private void maybeDisplayEnvironmentVariablesHelp() {
    if (this.environmentVariables) {
      Collection<Object> commands = new ArrayList<>(this.monitorings.size() + 1);
      commands.add(this);
      commands.addAll(this.monitorings);
      CommandSpec commandSpec = Utils.buildCommandSpec(commands.toArray());
      CommandLine commandLine = new CommandLine(commandSpec);
      CommandLine.usage(commandLine, System.out);
      System.exit(0);
    }
  }

  private void maybeDisplayVersion() {
    if (this.version) {
      versionInformation(this.out);
      System.exit(0);
    }
  }

  private ShutdownService.CloseCallback closeStep(
      String message, ShutdownService.CloseCallback callback) {
    return new CloseCallback() {
      @Override
      public void run() throws Exception {
        LOGGER.debug(message);
        callback.run();
      }

      @Override
      public String toString() {
        return message;
      }
    };
  }

  private boolean isRunTimeLimited() {
    return this.time > 0;
  }

  // for testing
  void close() {
    if (this.isRunTimeLimited()) {
      this.environment.close();
      this.eventLoopGroup.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    }
  }

  public void monitorings(List<Monitoring> monitorings) {
    this.monitorings = monitorings;
  }

  static class RunContext {

    private final int exitCode;
    private final StreamPerfTest command;

    private RunContext(int exitCode, StreamPerfTest command) {
      this.exitCode = exitCode;
      this.command = command;
    }

    int exitCode() {
      return this.exitCode;
    }

    StreamPerfTest command() {
      return this.command;
    }
  }
}
