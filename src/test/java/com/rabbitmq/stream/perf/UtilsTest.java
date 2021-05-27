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
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.perf.Utils.PatternConsumerNameStrategy;
import com.rabbitmq.stream.perf.Utils.RangeTypeConverter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

public class UtilsTest {

  CommandLine.ITypeConverter<OffsetSpecification> offsetSpecificationConverter =
      new Utils.OffsetSpecificationTypeConverter();

  static Stream<Arguments> offsetSpecificationTypeConverterOkArguments() {
    return Stream.of(
        of("", OffsetSpecification.first()),
        of("first", OffsetSpecification.first()),
        of("FIRST", OffsetSpecification.first()),
        of("last", OffsetSpecification.last()),
        of("LAST", OffsetSpecification.last()),
        of("next", OffsetSpecification.next()),
        of("NEXT", OffsetSpecification.next()),
        of("0", OffsetSpecification.offset(0)),
        of("1000", OffsetSpecification.offset(1000)),
        of("9223372036854775817", OffsetSpecification.offset(Long.MAX_VALUE + 10)),
        of("2020-06-03T08:54:57Z", OffsetSpecification.timestamp(1591174497000L)),
        of("2020-06-03T10:54:57+02:00", OffsetSpecification.timestamp(1591174497000L)));
  }

  static Stream<Arguments> streams() {
    Stream<Arguments> arguments =
        Stream.of(
            of("1", "stream", Arrays.asList("stream")),
            of("5", "stream", IntStream.range(1, 6).mapToObj(i -> "stream-" + i).collect(toList())),
            of(
                "10",
                "stream",
                IntStream.range(1, 11).mapToObj(i -> format("stream-%02d", i)).collect(toList())),
            of(
                "1-10",
                "stream",
                IntStream.range(1, 11).mapToObj(i -> format("stream-%02d", i)).collect(toList())),
            of(
                "50-500",
                "stream",
                IntStream.range(50, 501).mapToObj(i -> format("stream-%03d", i)).collect(toList())),
            of(
                "1-10",
                "stream-%d",
                IntStream.range(1, 11).mapToObj(i -> format("stream-%d", i)).collect(toList())),
            of(
                "50-500",
                "stream-%d",
                IntStream.range(50, 501).mapToObj(i -> format("stream-%d", i)).collect(toList())));
    return arguments.flatMap(
        arg -> {
          String range = arg.get()[0].toString();
          return range.contains("-")
              ? Stream.of(
                  of(range, arg.get()[1], arg.get()[2]),
                  of(range.replace("-", ".."), arg.get()[1], arg.get()[2]))
              : Stream.of(arg);
        });
  }

  @ParameterizedTest
  @MethodSource("offsetSpecificationTypeConverterOkArguments")
  void offsetSpecificationTypeConverterOk(String value, OffsetSpecification expected)
      throws Exception {
    assertThat(offsetSpecificationConverter.convert(value)).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(strings = {"foo", "-1", "2020-06-03"})
  void offsetSpecificationTypeConverterKo(String value) {
    assertThatThrownBy(() -> offsetSpecificationConverter.convert(value))
        .isInstanceOf(CommandLine.TypeConversionException.class);
  }

  @ParameterizedTest
  @CsvSource({
    "%s-%d,s1-2",
    "stream-%s-consumer-%d,stream-s1-consumer-2",
    "consumer-%2$d-on-stream-%1$s,consumer-2-on-stream-s1"
  })
  void consumerNameStrategy(String pattern, String expected) {
    BiFunction<String, Integer, String> strategy = new PatternConsumerNameStrategy(pattern);
    assertThat(strategy.apply("s1", 2)).isEqualTo(expected);
  }

  @Test
  void writeReadLongInByteArray() {
    byte[] array = new byte[8];
    LongStream.of(
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            1,
            128,
            256,
            33_000,
            66_000,
            1_000_000,
            new Random().nextLong())
        .forEach(
            value -> {
              Utils.writeLong(array, value);
              assertThat(Utils.readLong(array)).isEqualTo(value);
            });
  }

  @Test
  void rotateList() {
    List<String> hosts = Arrays.asList("host1", "host2", "host3");
    Collections.rotate(hosts, -1);
    assertThat(hosts).isEqualTo(Arrays.asList("host2", "host3", "host1"));
    Collections.rotate(hosts, -1);
    assertThat(hosts).isEqualTo(Arrays.asList("host3", "host1", "host2"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"1..10", "1-10", "1", "10"})
  void rangeConverterOk(String value) {
    assertThat(new RangeTypeConverter().convert(value)).isEqualTo(value);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0",
        "dummy",
        "-1",
        "-1..2",
        "-1-2",
        "10..1",
        "10-1",
        "1..bb",
        "1-bb",
        "bb..1",
        "bb-1",
        "1..10..15",
        "1-10-15"
      })
  void rangeConverterKo(String value) {
    assertThatThrownBy(() -> new RangeTypeConverter().convert(value))
        .isInstanceOf(CommandLine.TypeConversionException.class)
        .hasMessageContaining("not valid");
  }

  @ParameterizedTest
  @MethodSource
  void streams(String range, String input, List<String> expected) {
    assertThat(Utils.streams(range, Collections.singletonList(input)))
        .hasSameSizeAs(expected)
        .isEqualTo(expected);
  }

  @Test
  void streamsStreamListShouldBeUsedWhenStreamCountIsOne() {
    assertThat(Utils.streams("1", Arrays.asList("stream1", "stream2", "stream3")))
        .hasSize(3)
        .isEqualTo(Arrays.asList("stream1", "stream2", "stream3"));
  }

  @Test
  void streamsSpecifyOnlyOneStreamWhenStreamCountIsSpecified() {
    assertThatThrownBy(() -> Utils.streams("2", Arrays.asList("stream1", "stream2")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
