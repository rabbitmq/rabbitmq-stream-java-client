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

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.metrics.MetricsCollector;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

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

  interface EnvironmentFactory extends AutoCloseable {

    Environment get();
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

  static class SingletonEnvironmentFactory implements EnvironmentFactory {

    private final Environment environment;

    SingletonEnvironmentFactory(EnvironmentBuilder builder) {
      this.environment = builder.build();
    }

    @Override
    public Environment get() {
      return environment;
    }

    @Override
    public void close() throws Exception {
      environment.close();
    }
  }

  static class AlwaysCreateEnvironmentFactory implements EnvironmentFactory {

    private final List<String> uris;
    private final MetricsCollector metricsCollector;
    private final List<Environment> environments = new CopyOnWriteArrayList<>();
    private final AtomicInteger sequence = new AtomicInteger(0);

    AlwaysCreateEnvironmentFactory(List<String> uris, MetricsCollector metricsCollector) {
      this.uris = Collections.unmodifiableList(new ArrayList<>(uris));
      this.metricsCollector = metricsCollector;
    }

    @Override
    public Environment get() {
      List<String> rotatedUris = new ArrayList<>(this.uris);
      Collections.rotate(rotatedUris, -(sequence.getAndIncrement() % this.uris.size()));
      Environment environment =
          Environment.builder().uris(rotatedUris).metricsCollector(metricsCollector).build();
      return environment;
    }

    @Override
    public void close() {
      ExecutorService executorService = Executors.newCachedThreadPool();
      List<Future<?>> tasks = new ArrayList<>(environments.size());
      for (Environment environment : environments) {
        Future<?> task =
            executorService.submit(
                () -> {
                  try {
                    environment.close();
                  } catch (Exception e) {
                    LOGGER.warn("Error while closing environment");
                  }
                });
        tasks.add(task);
      }
      for (Future<?> task : tasks) {
        try {
          task.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
          // OK
        }
      }
      executorService.shutdownNow();
    }
  }
}
