// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static java.lang.String.format;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamCreator;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.metrics.MicrometerMetricsCollector;
import com.rabbitmq.stream.perf.Utils.NamedThreadFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "stream-perf-test",
    mixinStandardHelpOptions = false,
    showDefaultValues = true,
    version = "perftest 0.1",
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

  int streamDispatching = 0;
  private volatile Codec codec;

  @CommandLine.Option(
      names = {"--uris", "-u"},
      description =
          "servers to connect to, e.g. rabbitmq-stream://localhost:5551, separated by commas",
      defaultValue = "rabbitmq-stream://localhost:5551",
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
      names = {"--offset", "-o"},
      description =
          "offset to start listening from. "
              + "Valid values are 'first', 'last', 'next', an unsigned long, "
              + "or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z).",
      defaultValue = "first",
      converter = Utils.OffsetSpecificationTypeConverter.class)
  private OffsetSpecification offset;

  @CommandLine.Option(
      names = {"--pre-declared", "-p"},
      description = "whether streams are already declared or not",
      defaultValue = "false")
  private boolean preDeclared;

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
      names = {"--sub-entry-size", "-ses"},
      description = "number of messages packed into a normal message entry",
      defaultValue = "1",
      converter = Utils.PositiveIntegerTypeConverter.class)
  private int subEntrySize;

  @CommandLine.Option(
      names = {"--codec", "-cc"},
      description = "class of codec to use. Aliases: qpid, simple.",
      defaultValue = "qpid")
  private String codecClass;

  @CommandLine.Option(
      names = {"--max-length-bytes", "-mlb"},
      description = "max size of created streams",
      defaultValue = "20gb",
      converter = Utils.ByteCapacityTypeConverter.class)
  private ByteCapacity maxLengthBytes;

  @CommandLine.Option(
      names = {"--max-segment-size", "-mss"},
      description = "max size of segments",
      defaultValue = "500mb",
      converter = Utils.ByteCapacityTypeConverter.class)
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
              + "Possible values: client-local, least-leaders, random.",
      converter = Utils.LeaderLocatorTypeConverter.class,
      defaultValue = "least-leaders")
  private LeaderLocator leaderLocator;

  @CommandLine.Option(
      names = {"--commit-every", "-ce"},
      description = "the frequency of offset commit",
      defaultValue = "0")
  private int commitEvery;

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
      names = {"--committing-consumers-by-connection", "-ccbc"},
      description =
          "number of committing consumers by connection. Value must be between 1 and 255.",
      defaultValue = "50",
      converter = Utils.OneTo255RangeIntegerTypeConverter.class)
  private int committingConsumersByConnection;

  @CommandLine.Option(
      names = {"--consumers-by-connection", "-cbc"},
      description = "number of consumers by connection. Value must be between 1 and 255.",
      defaultValue = "1",
      converter = Utils.OneTo255RangeIntegerTypeConverter.class)
  private int consumersByConnection;

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

  private static void versionInformation() {
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
            Charset.defaultCharset().toString(),
            System.getProperty("os.name"),
            System.getProperty("os.version"),
            System.getProperty("os.arch"));
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

  @Override
  public Integer call() throws Exception {
    if (this.version) {
      versionInformation();
      System.exit(0);
    }
    this.codec = createCodec(this.codecClass);

    CompositeMeterRegistry meterRegistry = new CompositeMeterRegistry();
    String metricsPrefix = "rabbitmq.stream";
    this.metricsCollector = new MicrometerMetricsCollector(meterRegistry, metricsPrefix);

    Counter producerConfirm = meterRegistry.counter(metricsPrefix + ".producer_confirmed");

    this.performanceMetrics =
        new DefaultPerformanceMetrics(meterRegistry, metricsPrefix, this.summaryFile);

    this.messageSize = this.messageSize < 8 ? 8 : this.messageSize; // we need to store a long in it

    ShutdownService shutdownService = new ShutdownService();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdownService.close()));

    // FIXME add confirm latency

    ScheduledExecutorService envExecutor =
        Executors.newScheduledThreadPool(
            Math.max(Runtime.getRuntime().availableProcessors(), this.producers),
            new NamedThreadFactory("stream-perf-test-env-"));

    shutdownService.wrap(
        closeStep("Closing environment executor", () -> envExecutor.shutdownNow()));

    Environment environment =
        Environment.builder()
            .uris(this.uris)
            .scheduledExecutorService(envExecutor)
            .metricsCollector(metricsCollector)
            .maxProducersByConnection(this.producersByConnection)
            .maxCommittingConsumersByConnection(this.committingConsumersByConnection)
            .maxConsumersByConnection(this.consumersByConnection)
            .build();

    shutdownService.wrap(closeStep("Closing environment(s)", () -> environment.close()));

    streams = Utils.streams(this.streamCount, this.streams);

    if (!preDeclared) {
      for (String stream : streams) {
        StreamCreator streamCreator =
            environment.streamCreator().stream(stream)
                .maxLengthBytes(this.maxLengthBytes)
                .maxSegmentSizeBytes(this.maxSegmentSize)
                .leaderLocator(this.leaderLocator);

        if (this.maxAge != null) {
          streamCreator.maxAge(this.maxAge);
        }

        streamCreator.create();
      }
      shutdownService.wrap(
          closeStep(
              "Deleting stream(s)",
              () -> {
                for (String stream : streams) {
                  LOGGER.debug("Deleting {}", stream);
                  try {
                    environment.deleteStream(stream);
                    LOGGER.debug("Deleted {}", stream);
                  } catch (Exception e) {
                    LOGGER.warn("Could not delete stream {}: {}", stream, e.getMessage());
                  }
                }
              }));
    }

    // FIXME handle metadata update for consumers and publishers
    // they should at least issue a warning that their stream has been deleted and that they're now
    // useless

    List<Producer> producers = Collections.synchronizedList(new ArrayList<>(this.producers));
    List<Runnable> producerRunnables =
        IntStream.range(0, this.producers)
            .mapToObj(
                i -> {
                  Runnable rateLimiterCallback;
                  if (this.rate > 0) {
                    RateLimiter rateLimiter =
                        com.google.common.util.concurrent.RateLimiter.create(this.rate);
                    rateLimiterCallback = () -> rateLimiter.acquire(1);
                  } else {
                    rateLimiterCallback = () -> {};
                  }

                  String stream = stream();

                  Producer producer =
                      environment
                          .producerBuilder()
                          .subEntrySize(this.subEntrySize)
                          .batchSize(this.batchSize)
                          .maxUnconfirmedMessages(this.confirms)
                          .stream(stream)
                          .build();

                  producers.add(producer);

                  return (Runnable)
                      () -> {
                        final int msgSize = this.messageSize;

                        // FIXME do not increment producer confirm in case of publish error
                        ConfirmationHandler confirmationHandler =
                            confirmationStatus -> producerConfirm.increment();
                        while (true && !Thread.currentThread().isInterrupted()) {
                          rateLimiterCallback.run();
                          long creationTime = System.nanoTime();
                          byte[] payload = new byte[msgSize];
                          Utils.writeLong(payload, creationTime);
                          producer.send(
                              producer.messageBuilder().addData(payload).build(),
                              confirmationHandler);
                        }
                      };
                })
            .collect(Collectors.toList());

    List<Consumer> consumers =
        Collections.synchronizedList(
            IntStream.range(0, this.consumers)
                .mapToObj(
                    i -> {
                      final PerformanceMetrics metrics = this.performanceMetrics;

                      AtomicLong messageCount = new AtomicLong(0);
                      String stream = stream();
                      ConsumerBuilder consumerBuilder = environment.consumerBuilder();
                      consumerBuilder = consumerBuilder.stream(stream).offset(this.offset);

                      if (this.commitEvery > 0) {
                        consumerBuilder =
                            consumerBuilder
                                .name(UUID.randomUUID().toString())
                                .autoCommitStrategy()
                                .messageCountBeforeCommit(this.commitEvery)
                                .builder();
                      }

                      consumerBuilder =
                          consumerBuilder.messageHandler(
                              (offset, message) -> {
                                // at very high throughput ( > 1 M / s), the histogram can
                                // become a bottleneck,
                                // so we downsample and calculate latency for every x message
                                // this should not affect the metric much
                                if (messageCount.incrementAndGet() % 100 == 0) {
                                  metrics.latency(
                                      System.nanoTime() - Utils.readLong(message.getBodyAsBinary()),
                                      TimeUnit.NANOSECONDS);
                                }
                              });

                      Consumer consumer = consumerBuilder.build();

                      return consumer;
                    })
                .collect(Collectors.toList()));

    shutdownService.wrap(
        closeStep(
            "Closing consumers",
            () -> {
              for (Consumer consumer : consumers) {
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

  private ShutdownService.CloseCallback closeStep(
      String message, ShutdownService.CloseCallback callback) {
    return () -> {
      LOGGER.debug(message);
      callback.run();
    };
  }

  private String stream() {
    return streams.get(streamDispatching++ % streams.size());
  }
}
