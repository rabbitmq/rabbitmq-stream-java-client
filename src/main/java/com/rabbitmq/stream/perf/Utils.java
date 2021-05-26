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
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

class Utils {

  static final X509TrustManager TRUST_EVERYTHING_TRUST_MANAGER = new TrustEverythingTrustManager();
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final String RANGE_SEPARATOR_1 = "-";
  private static final String RANGE_SEPARATOR_2 = "..";

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

  private static void throwConversionException(String format, String... arguments) {
    throw new CommandLine.TypeConversionException(String.format(format, arguments));
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

    public NamedThreadFactory(ThreadFactory backingThreaFactory, String prefix) {
      this.backingThreaFactory = backingThreaFactory;
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
}
