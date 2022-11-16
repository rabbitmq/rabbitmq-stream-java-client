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

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.SocketConfigurator;
import com.rabbitmq.client.SocketConfigurators;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.metrics.MicrometerMetricsCollector;
import com.sun.management.OperatingSystemMXBean;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

class Utils {

  static final X509TrustManager TRUST_EVERYTHING_TRUST_MANAGER = new TrustEverythingTrustManager();
  static final Function<String, String> OPTION_TO_ENVIRONMENT_VARIABLE =
      option -> {
        if (option.startsWith("--")) {
          return option.replace("--", "").replace('-', '_').toUpperCase(Locale.ENGLISH);
        } else if (option.startsWith("-")) {
          return option.substring(1).replace('-', '_').toUpperCase(Locale.ENGLISH);
        } else {
          return option.replace('-', '_').toUpperCase(Locale.ENGLISH);
        }
      };
  static final Function<String, String> ENVIRONMENT_VARIABLE_PREFIX =
      name -> {
        String prefix = System.getenv("RABBITMQ_STREAM_PERF_TEST_ENV_PREFIX");
        if (prefix == null || prefix.trim().isEmpty()) {
          return name;
        }
        if (prefix.endsWith("_")) {
          return prefix + name;
        } else {
          return prefix + "_" + name;
        }
      };
  static final Function<String, String> ENVIRONMENT_VARIABLE_LOOKUP = name -> System.getenv(name);
  private static final LongSupplier TOTAL_MEMORY_SIZE_SUPPLIER;
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final String RANGE_SEPARATOR_1 = "-";
  private static final String RANGE_SEPARATOR_2 = "..";

  // this trick avoids a deprecation warning when compiling on Java 14+
  static {
    Method method;
    try {
      // Java 14+
      method = OperatingSystemMXBean.class.getDeclaredMethod("getTotalMemorySize");
    } catch (NoSuchMethodException nsme) {
      try {
        method = OperatingSystemMXBean.class.getDeclaredMethod("getTotalPhysicalMemorySize");
      } catch (Exception e) {
        throw new RuntimeException("Error while computing method to get total memory size");
      }
    }
    Method m = method;
    TOTAL_MEMORY_SIZE_SUPPLIER =
        () -> {
          OperatingSystemMXBean os =
              (OperatingSystemMXBean)
                  java.lang.management.ManagementFactory.getOperatingSystemMXBean();
          try {
            return (long) m.invoke(os);
          } catch (Exception e) {
            throw new RuntimeException("Could not retrieve total memory size", e);
          }
        };
  }

  static void writeLong(byte[] array, long value) {
    // from Guava Longs
    for (int i = 7; i >= 0; i--) {
      array[i] = (byte) (value & 0xffL);
      value >>= 8;
    }
  }

  static long readLong(byte[] array) {
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

  static List<String> streams(String range, List<String> streams) {
    if (range.contains(RANGE_SEPARATOR_2)) {
      range = range.replace(RANGE_SEPARATOR_2, RANGE_SEPARATOR_1);
    }
    int from, to;
    if (range.contains(RANGE_SEPARATOR_1)) {
      String[] fromTo = range.split(RANGE_SEPARATOR_1);
      from = Integer.parseInt(fromTo[0]);
      to = Integer.parseInt(fromTo[1]) + 1;
    } else {
      int count = Integer.parseInt(range);
      from = 1;
      to = count + 1;
    }
    if (from == 1 && to == 2) {
      return streams;
    } else {
      if (streams.size() != 1) {
        throw new IllegalArgumentException("Enter only 1 stream when --stream-count is specified");
      }
      String format = streams.get(0);
      String streamFormat;
      if (!format.contains("%")) {
        int digits = String.valueOf(to - 1).length();
        streamFormat = format + "-%0" + digits + "d";
      } else {
        streamFormat = format;
      }
      return IntStream.range(from, to)
          .mapToObj(i -> String.format(streamFormat, i))
          .collect(Collectors.toList());
    }
  }

  static String formatByte(double bytes) {
    // based on
    // https://stackoverflow.com/questions/3758606/how-can-i-convert-byte-size-into-a-human-readable-format-in-java
    if (-1000 < bytes && bytes < 1000) {
      return String.valueOf(bytes);
    }
    CharacterIterator ci = new StringCharacterIterator("kMGTPE");
    while (bytes <= -999_950 || bytes >= 999_950) {
      bytes /= 1000;
      ci.next();
    }
    return String.format("%.1f %cB", bytes / 1000.0, ci.current());
  }

  static long physicalMemory() {
    try {
      return TOTAL_MEMORY_SIZE_SUPPLIER.getAsLong();
    } catch (Throwable e) {
      // we can get NoClassDefFoundError, so we catch from Throwable and below
      LOGGER.warn("Could not get physical memory", e);
      return 0;
    }
  }

  private static void throwConversionException(String format, String... arguments) {
    throw new CommandLine.TypeConversionException(String.format(format, (Object[]) arguments));
  }

  static void assignValuesToCommand(Object command, Function<String, String> optionMapping)
      throws Exception {
    LOGGER.debug("Assigning values to command {}", command.getClass());
    Collection<String> arguments = new ArrayList<>();
    Collection<Field> fieldsToAssign = new ArrayList<>();
    for (Field field : command.getClass().getDeclaredFields()) {
      Option option = field.getAnnotation(Option.class);
      if (option == null) {
        LOGGER.debug("No option annotation for field {}", field.getName());
        continue;
      }
      String longOption =
          Arrays.stream(option.names())
              .sorted(Comparator.comparingInt(String::length).reversed())
              .findFirst()
              .get();
      LOGGER.debug("Looking up new value for option {}", longOption);
      String newValue = optionMapping.apply(longOption);

      LOGGER.debug(
          "New value found for option {} (field {}): {}", longOption, field.getName(), newValue);
      if (newValue == null) {
        continue;
      }
      fieldsToAssign.add(field);
      if (field.getType().equals(boolean.class) || field.getType().equals(Boolean.class)) {
        if (Boolean.parseBoolean(newValue)) {
          arguments.add(longOption);
        }
      } else {
        arguments.add(longOption + " " + newValue);
      }
    }
    if (fieldsToAssign.size() > 0) {
      Constructor<?> defaultConstructor = command.getClass().getConstructor();
      Object commandBuffer = defaultConstructor.newInstance();
      String argumentsLine = String.join(" ", arguments);
      LOGGER.debug("Arguments line with extra values: {}", argumentsLine);
      String[] args = argumentsLine.split(" ");
      commandBuffer = CommandLine.populateCommand(commandBuffer, args);
      for (Field field : fieldsToAssign) {
        field.setAccessible(true);
        field.set(command, field.get(commandBuffer));
      }
    }
  }

  static CommandSpec buildCommandSpec(Object... commands) {
    Object mainCommand = commands[0];
    Command commandAnnotation = mainCommand.getClass().getAnnotation(Command.class);
    CommandSpec spec = CommandSpec.create();
    spec.name(commandAnnotation.name());
    spec.mixinStandardHelpOptions(commandAnnotation.mixinStandardHelpOptions());
    for (Object command : commands) {
      for (Field f : command.getClass().getDeclaredFields()) {
        Option annotation = f.getAnnotation(Option.class);
        if (annotation == null) {
          continue;
        }
        String name =
            Arrays.stream(annotation.names())
                .sorted(Comparator.comparingInt(String::length).reversed())
                .findFirst()
                .map(OPTION_TO_ENVIRONMENT_VARIABLE::apply)
                .get();
        spec.addOption(
            OptionSpec.builder(name)
                .type(f.getType())
                .description(annotation.description())
                .paramLabel("<" + name.replace("_", "-") + ">")
                .defaultValue(annotation.defaultValue())
                .showDefaultValue(annotation.showDefaultValue())
                .build());
      }
    }

    return spec;
  }

  static int downSamplingDivisor(int rate) {
    int divisor;
    if (rate > 0) {
      divisor = rate > 100 ? 100 : 1; // no downsampling for small rates
    } else {
      // no rate limitation, downsampling
      divisor = 100;
    }
    return divisor;
  }

  static void declareSuperStreamExchangeAndBindings(
      Channel channel, String superStream, List<String> streams) throws Exception {
    channel.exchangeDeclare(
        superStream,
        BuiltinExchangeType.DIRECT,
        true,
        false,
        Collections.singletonMap("x-super-stream", true));

    for (int i = 0; i < streams.size(); i++) {
      channel.queueBind(
          streams.get(i),
          superStream,
          String.valueOf(i),
          Collections.singletonMap("x-stream-partition-order", i));
    }
  }

  static void deleteSuperStreamExchange(Channel channel, String superStream) throws Exception {
    channel.exchangeDelete(superStream);
  }

  static List<String> superStreamPartitions(String superStream, int partitionCount) {
    int digits = String.valueOf(partitionCount - 1).length();
    String format = superStream + "-%0" + digits + "d";
    return IntStream.range(0, partitionCount)
        .mapToObj(i -> String.format(format, i))
        .collect(Collectors.toList());
  }

  static Connection amqpConnection(
      String amqpUri, List<String> streamUris, boolean isTls, List<SNIServerName> sniServerNames)
      throws Exception {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    if (amqpUri == null || amqpUri.trim().isEmpty()) {
      String streamUriString = streamUris.get(0);
      if (isTls) {
        streamUriString = streamUriString.replaceFirst("rabbitmq-stream\\+tls", "amqps");
      } else {
        streamUriString = streamUriString.replaceFirst("rabbitmq-stream", "amqp");
      }
      URI streamUri = new URI(streamUriString);
      int streamPort = streamUri.getPort();
      if (streamPort != -1) {
        int defaultAmqpPort =
            isTls
                ? ConnectionFactory.DEFAULT_AMQP_OVER_SSL_PORT
                : ConnectionFactory.DEFAULT_AMQP_PORT;
        streamUriString = streamUriString.replaceFirst(":" + streamPort, ":" + defaultAmqpPort);
      }
      connectionFactory.setUri(streamUriString);
    } else {
      connectionFactory.setUri(amqpUri);
    }
    if (isTls) {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(
          new KeyManager[] {},
          new TrustManager[] {TRUST_EVERYTHING_TRUST_MANAGER},
          new SecureRandom());
      connectionFactory.useSslProtocol(sslContext);
      if (!sniServerNames.isEmpty()) {
        SocketConfigurator socketConfigurator =
            socket -> {
              if (socket instanceof SSLSocket) {
                SSLSocket sslSocket = (SSLSocket) socket;
                SSLParameters sslParameters =
                    sslSocket.getSSLParameters() == null
                        ? new SSLParameters()
                        : sslSocket.getSSLParameters();
                sslParameters.setServerNames(sniServerNames);
                sslSocket.setSSLParameters(sslParameters);
              } else {
                LOGGER.warn("SNI parameter set on a non-TLS connection");
              }
            };
        connectionFactory.setSocketConfigurator(
            SocketConfigurators.defaultConfigurator().andThen(socketConfigurator));
      }
    }
    return connectionFactory.newConnection("stream-perf-test-amqp-connection");
  }

  static String commandLineMetrics(String[] args) {
    Map<String, Boolean> filteredOptions = new HashMap<>();
    filteredOptions.put("--uris", true);
    filteredOptions.put("-u", true);
    filteredOptions.put("--prometheus", false);
    filteredOptions.put("--amqp-uri", true);
    filteredOptions.put("--au", true);
    filteredOptions.put("--metrics-command-line-arguments", false);
    filteredOptions.put("-mcla", false);
    filteredOptions.put("--metrics-tags", true);
    filteredOptions.put("-mt", true);

    Collection<String> filtered = new ArrayList<>();
    Iterator<String> iterator = Arrays.stream(args).iterator();
    while (iterator.hasNext()) {
      String option = iterator.next();
      if (filteredOptions.containsKey(option)) {
        if (filteredOptions.get(option)) {
          iterator.next();
        }
      } else {
        filtered.add(option);
      }
    }
    return String.join(" ", filtered);
  }

  static class ByteCapacityTypeConverter implements CommandLine.ITypeConverter<ByteCapacity> {

    @Override
    public ByteCapacity convert(String value) {
      try {
        return ByteCapacity.from(value);
      } catch (IllegalArgumentException e) {
        throw new CommandLine.TypeConversionException(
            "'" + value + "' is not valid, valid example values: 100gb, 50mb");
      }
    }
  }

  static class MetricsTagsTypeConverter implements CommandLine.ITypeConverter<Collection<Tag>> {

    @Override
    public Collection<Tag> convert(String value) {
      if (value == null || value.trim().isEmpty()) {
        return Collections.emptyList();
      } else {
        try {
          Collection<Tag> tags = new ArrayList<>();
          for (String tag : value.split(",")) {
            String[] keyValue = tag.split("=", 2);
            tags.add(Tag.of(keyValue[0], keyValue[1]));
          }
          return tags;
        } catch (Exception e) {
          throw new TypeConversionException(
              String.format("'%s' is not valid, use key/value pairs separated by commas"));
        }
      }
    }
  }

  static class NameStrategyConverter
      implements CommandLine.ITypeConverter<BiFunction<String, Integer, String>> {

    @Override
    public BiFunction<String, Integer, String> convert(String input) {
      if ("uuid".equals(input)) {
        return (stream, index) -> UUID.randomUUID().toString();
      } else {
        return new PatternNameStrategy(input);
      }
    }
  }

  static class SniServerNamesConverter implements ITypeConverter<List<SNIServerName>> {

    @Override
    public List<SNIServerName> convert(String value) {
      if (value == null || value.trim().isEmpty()) {
        return Collections.emptyList();
      } else {
        return Arrays.stream(value.split(","))
            .map(s -> s.trim())
            .map(s -> new SNIHostName(s))
            .collect(Collectors.toList());
      }
    }
  }

  static class RangeTypeConverter implements CommandLine.ITypeConverter<String> {

    @Override
    public String convert(String input) {
      String value;
      if (input.contains(RANGE_SEPARATOR_2)) {
        value = input.replace(RANGE_SEPARATOR_2, RANGE_SEPARATOR_1);
      } else {
        value = input;
      }
      if (value.contains(RANGE_SEPARATOR_1)) {
        String[] fromTo = value.split(RANGE_SEPARATOR_1);
        if (fromTo == null || fromTo.length != 2) {
          throwConversionException("'%s' is not valid, valid examples values: 10, 1-10", input);
        }
        Arrays.stream(fromTo)
            .forEach(
                v -> {
                  try {
                    int i = Integer.parseInt(v);
                    if (i <= 0) {
                      throwConversionException(
                          "'%s' is not valid, the value must be a positive integer", v);
                    }
                  } catch (NumberFormatException e) {
                    throwConversionException(
                        "'%s' is not valid, the value must be a positive integer", v);
                  }
                });
        int from = Integer.parseInt(fromTo[0]);
        int to = Integer.parseInt(fromTo[1]);
        if (from >= to) {
          throwConversionException("'%s' is not valid, valid examples values: 10, 1-10", input);
        }
      } else {
        try {
          int count = Integer.parseInt(value);
          if (count <= 0) {
            throwConversionException(
                "'%s' is not valid, the value must be a positive integer", input);
          }
        } catch (NumberFormatException e) {
          throwConversionException("'%s' is not valid, valid example values: 10, 1-10", input);
        }
      }
      return input;
    }
  }

  static class DurationTypeConverter implements CommandLine.ITypeConverter<Duration> {

    @Override
    public Duration convert(String value) {
      try {
        Duration duration = Duration.parse(value);
        if (duration.isNegative() || duration.isZero()) {
          throw new CommandLine.TypeConversionException(
              "'" + value + "' is not valid, it must be positive");
        }
        return duration;
      } catch (DateTimeParseException e) {
        throw new CommandLine.TypeConversionException(
            "'" + value + "' is not valid, valid example values: PT15M, PT10H");
      }
    }
  }

  static class LeaderLocatorTypeConverter implements CommandLine.ITypeConverter<LeaderLocator> {

    @Override
    public LeaderLocator convert(String value) {
      try {
        return LeaderLocator.from(value);
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(
            "'"
                + value
                + "' is not valid, possible values: "
                + Arrays.stream(LeaderLocator.values())
                    .map(ll -> ll.value())
                    .collect(Collectors.joining(", ")));
      }
    }
  }

  static class OffsetSpecificationTypeConverter
      implements CommandLine.ITypeConverter<OffsetSpecification> {

    private static final Map<String, OffsetSpecification> SPECS =
        Collections.unmodifiableMap(
            new HashMap<String, OffsetSpecification>() {
              {
                put("first", OffsetSpecification.first());
                put("last", OffsetSpecification.last());
                put("next", OffsetSpecification.next());
              }
            });

    @Override
    public OffsetSpecification convert(String value) throws Exception {
      if (value == null || value.trim().isEmpty()) {
        return OffsetSpecification.first();
      }

      if (SPECS.containsKey(value.toLowerCase())) {
        return SPECS.get(value.toLowerCase());
      }

      try {
        long offset = Long.parseUnsignedLong(value);
        return OffsetSpecification.offset(offset);
      } catch (NumberFormatException e) {
        // trying next
      }

      try {
        TemporalAccessor accessor = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(value);
        return OffsetSpecification.timestamp(Instant.from(accessor).toEpochMilli());
      } catch (DateTimeParseException e) {
        throw new CommandLine.TypeConversionException(
            "'"
                + value
                + "' is not a valid offset value, valid values are 'first', 'last', 'next', "
                + "an unsigned long, or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z)");
      }
    }
  }

  static class PositiveIntegerTypeConverter implements CommandLine.ITypeConverter<Integer> {

    @Override
    public Integer convert(String input) {
      try {
        Integer value = Integer.valueOf(input);
        if (value <= 0) {
          throw new IllegalArgumentException();
        }
        return value;
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(input + " is not a positive integer");
      }
    }
  }

  static class GreaterThanOrEqualToZeroIntegerTypeConverter
      implements CommandLine.ITypeConverter<Integer> {

    @Override
    public Integer convert(String input) {
      try {
        Integer value = Integer.valueOf(input);
        if (value < 0) {
          throw new IllegalArgumentException();
        }
        return value;
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(input + " is not greater than or equal to 0");
      }
    }
  }

  static class CompressionTypeConverter implements CommandLine.ITypeConverter<Compression> {

    @Override
    public Compression convert(String input) {
      try {
        return Compression.valueOf(input.toUpperCase(Locale.ENGLISH));
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(
            input
                + " is not a valid compression value. "
                + "Accepted values are "
                + Arrays.stream(Compression.values())
                    .map(Compression::name)
                    .map(String::toLowerCase)
                    .collect(Collectors.joining(", "))
                + ".");
      }
    }
  }

  private abstract static class RangeIntegerTypeConverter
      implements CommandLine.ITypeConverter<Integer> {

    private final int min, max;

    private RangeIntegerTypeConverter(int min, int max) {
      this.min = min;
      this.max = max;
    }

    @Override
    public Integer convert(String input) {
      try {
        Integer value = Integer.valueOf(input);
        if (value < this.min || value > this.max) {
          throw new IllegalArgumentException();
        }
        return value;
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(
            input + " must an integer between " + this.min + " and " + this.max);
      }
    }
  }

  static class OneTo255RangeIntegerTypeConverter extends RangeIntegerTypeConverter {

    OneTo255RangeIntegerTypeConverter() {
      super(1, 255);
    }
  }

  static class NotNegativeIntegerTypeConverter implements CommandLine.ITypeConverter<Integer> {

    @Override
    public Integer convert(String input) {
      try {
        Integer value = Integer.valueOf(input);
        if (value < 0) {
          throw new IllegalArgumentException();
        }
        return value;
      } catch (Exception e) {
        throw new CommandLine.TypeConversionException(input + " is not a non-negative integer");
      }
    }
  }

  static class NamedThreadFactory implements ThreadFactory {

    private final ThreadFactory backingThreaFactory;

    private final String prefix;

    private final AtomicLong count = new AtomicLong(0);

    public NamedThreadFactory(String prefix) {
      this(Executors.defaultThreadFactory(), prefix);
    }

    public NamedThreadFactory(ThreadFactory backingThreadFactory, String prefix) {
      this.backingThreaFactory = backingThreadFactory;
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = this.backingThreaFactory.newThread(r);
      thread.setName(prefix + count.getAndIncrement());
      return thread;
    }
  }

  private static class TrustEverythingTrustManager implements X509TrustManager {

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }

  static final class PatternNameStrategy implements BiFunction<String, Integer, String> {

    private final String pattern;

    PatternNameStrategy(String pattern) {
      this.pattern = pattern;
    }

    @Override
    public String apply(String stream, Integer index) {
      return String.format(pattern, stream, index);
    }
  }

  static class PerformanceMicrometerMetricsCollector extends MicrometerMetricsCollector {

    public PerformanceMicrometerMetricsCollector(MeterRegistry registry, String prefix) {
      super(registry, prefix);
    }

    @Override
    protected Counter createChunkCounter(
        MeterRegistry registry, String prefix, Iterable<Tag> tags) {
      return null;
    }

    @Override
    protected DistributionSummary createChunkSizeDistributionSummary(
        MeterRegistry registry, String prefix, Iterable<Tag> tags) {
      return DistributionSummary.builder(prefix + ".chunk_size")
          .tags(tags)
          .description("chunk size")
          .publishPercentiles(0.5, 0.75, 0.95, 0.99)
          .distributionStatisticExpiry(Duration.ofSeconds(1))
          .serviceLevelObjectives()
          .register(registry);
    }

    @Override
    public void chunk(int entriesCount) {
      this.chunkSize.record(entriesCount);
    }
  }
}
