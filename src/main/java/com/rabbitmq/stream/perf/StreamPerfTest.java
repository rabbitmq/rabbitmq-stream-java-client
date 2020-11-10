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

import static java.lang.String.format;

import com.google.common.util.concurrent.RateLimiter;
import com.rabbitmq.stream.*;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.metrics.MicrometerMetricsCollector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
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
          "servers to connect to, e.g. rabbitmq-stream://localhost:5555, separated by commas",
      defaultValue = "rabbitmq-stream://localhost:5555",
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
      names = {"--streams", "-st"},
      description = "stream(s) to send to and consume from, separated by commas",
      defaultValue = "stream1",
      split = ",")
  private List<String> streams;

  @CommandLine.Option(
      names = {"--offset", "-o"},
      description =
          "offset to start listening from. "
              + "Valid values are 'first', 'last', 'next', an unsigned long, or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z).",
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

    Environment environment =
        Environment.builder().uris(this.uris).metricsCollector(metricsCollector).build();

    shutdownService.wrap(closeStep("Closing environment", () -> environment.close()));

    if (!preDeclared) {
      for (String stream : streams) {
        // FIXME use the x-queue-leader-locator argument to spread the streams
        environment.streamCreator().stream(stream)
            .maxLengthBytes(maxLengthBytes)
            .maxSegmentSizeBytes(maxSegmentSize)
            .create();
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

                  ScheduledExecutorService scheduledExecutorService =
                      Executors.newScheduledThreadPool(this.producers);

                  shutdownService.wrap(
                      closeStep(
                          "Closing scheduled executor service",
                          () -> scheduledExecutorService.shutdownNow()));

                  shutdownService.wrap(
                      closeStep(
                          "Closing producers",
                          () -> {
                            for (Producer p : producers) {
                              p.close();
                            }
                          }));

                  return (Runnable)
                      () -> {
                        final int msgSize = this.messageSize;

                        ConfirmationHandler confirmationHandler =
                            confirmationStatus -> producerConfirm.increment();
                        while (true && !Thread.currentThread().isInterrupted()) {
                          rateLimiterCallback.run();
                          long creationTime = System.nanoTime();
                          byte[] payload = new byte[msgSize];
                          writeLong(payload, creationTime);
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
                                      System.nanoTime() - readLong(message.getBodyAsBinary()),
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
            "Closing executor service",
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
