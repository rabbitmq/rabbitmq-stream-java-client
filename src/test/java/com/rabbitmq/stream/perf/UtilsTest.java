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

import static com.rabbitmq.stream.perf.Utils.superStreamPartitions;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.perf.Utils.CompressionTypeConverter;
import com.rabbitmq.stream.perf.Utils.MetricsTagsTypeConverter;
import com.rabbitmq.stream.perf.Utils.NameStrategyConverter;
import com.rabbitmq.stream.perf.Utils.PatternNameStrategy;
import com.rabbitmq.stream.perf.Utils.RangeTypeConverter;
import com.rabbitmq.stream.perf.Utils.SniServerNamesConverter;
import io.micrometer.core.instrument.Tag;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import javax.net.ssl.SNIHostName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.TypeConversionException;

public class UtilsTest {

  CommandLine.ITypeConverter<OffsetSpecification> offsetSpecificationConverter =
      new Utils.OffsetSpecificationTypeConverter();

  CompressionTypeConverter compressionTypeConverter = new CompressionTypeConverter();

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
            of("1", "stream", Collections.singletonList("stream")),
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

  private static Tag tag(String key, String value) {
    return Tag.of(key, value);
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
  @ValueSource(
      strings = {
        "none", "gzip", "snappy", "lz4", "zstd",
        "NONE", "GZIP", "SNAPPY", "LZ4", "ZSTD"
      })
  void compressionTypeConverterOk(String value) {
    assertThat(compressionTypeConverter.convert(value))
        .isEqualTo(Compression.valueOf(value.toUpperCase(Locale.ENGLISH)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "foo", "bar"})
  void compressionTypeConverterKo(String value) {
    assertThatThrownBy(() -> compressionTypeConverter.convert(value))
        .isInstanceOf(TypeConversionException.class)
        .hasMessageContaining("Accepted values are none, gzip, snappy, lz4, zstd");
  }

  @ParameterizedTest
  @CsvSource({
    "%s-%d,s1-2",
    "stream-%s-consumer-%d,stream-s1-consumer-2",
    "consumer-%2$d-on-stream-%1$s,consumer-2-on-stream-s1"
  })
  void consumerNameStrategy(String pattern, String expected) {
    BiFunction<String, Integer, String> strategy = new PatternNameStrategy(pattern);
    assertThat(strategy.apply("s1", 2)).isEqualTo(expected);
  }

  @Test
  void producerConsumerNameStrategyConverterShouldReturnUuidWhenAskedForUuid() {
    NameStrategyConverter nameStrategyConverter = new NameStrategyConverter();
    BiFunction<String, Integer, String> nameStrategy = nameStrategyConverter.convert("uuid");
    String name = nameStrategy.apply("stream", 1);
    UUID.fromString(name);
    assertThat(nameStrategy.apply("stream", 1)).isNotEqualTo(name);
  }

  @Test
  void producerConsumerNameStrategyConverterShouldReturnEmptyStringWhenPatternIsEmptyString() {
    NameStrategyConverter nameStrategyConverter = new NameStrategyConverter();
    BiFunction<String, Integer, String> nameStrategy = nameStrategyConverter.convert("");
    assertThat(nameStrategy.apply("stream", 1)).isEmpty();
    assertThat(nameStrategy.apply("stream", 2)).isEmpty();
  }

  @Test
  void producerConsumerNameStrategyConverterShouldReturnPatternStrategyWhenAsked() {
    NameStrategyConverter nameStrategyConverter = new NameStrategyConverter();
    BiFunction<String, Integer, String> nameStrategy =
        nameStrategyConverter.convert("stream-%s-consumer-%d");
    assertThat(nameStrategy).isInstanceOf(PatternNameStrategy.class);
    assertThat(nameStrategy.apply("s1", 2)).isEqualTo("stream-s1-consumer-2");
  }

  @Test
  void sniServerNamesConverter() throws Exception {
    SniServerNamesConverter converter = new SniServerNamesConverter();
    assertThat(converter.convert("")).isEmpty();
    assertThat(converter.convert("localhost,dummy"))
        .hasSize(2)
        .contains(new SNIHostName("localhost"))
        .contains(new SNIHostName("dummy"));
  }

  @Test
  void metricsTagsConverter() throws Exception {
    MetricsTagsTypeConverter converter = new MetricsTagsTypeConverter();
    assertThat(converter.convert(null)).isNotNull().isEmpty();
    assertThat(converter.convert("")).isNotNull().isEmpty();
    assertThat(converter.convert("  ")).isNotNull().isEmpty();
    assertThat(converter.convert("env=performance,datacenter=eu"))
        .hasSize(2)
        .contains(tag("env", "performance"))
        .contains(tag("datacenter", "eu"));
    assertThat(converter.convert("args=--queue-args \"x-max-length=100000\""))
        .hasSize(1)
        .contains(tag("args", "--queue-args \"x-max-length=100000\""));
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

  @Test
  void assignValuesToCommand() throws Exception {
    TestCommand command = new TestCommand();
    Map<String, String> mappings = new HashMap<>();
    mappings.put("aaa", "42"); // takes only long options
    mappings.put("b", "true");
    mappings.put("offset", "first");
    Utils.assignValuesToCommand(command, option -> mappings.get(option));
    assertThat(command.a).isEqualTo(42);
    assertThat(command.b).isTrue();
    assertThat(command.c).isFalse();
    assertThat(command.offsetSpecification).isEqualTo(OffsetSpecification.first());
  }

  @Test
  void buildCommandSpec() {
    CommandSpec spec = Utils.buildCommandSpec(new TestCommand());
    assertThat(spec.optionsMap()).hasSize(4).containsKeys("AAA", "B", "C", "OFFSET");
  }

  @ParameterizedTest
  @CsvSource({
    "0,100",
    "50,1",
    "1000,100",
  })
  void testDownSamplingDivisor(int rate, int expected) {
    assertThat(Utils.downSamplingDivisor(rate)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "--uris,URIS",
    "--stream-count,STREAM_COUNT",
    "-sc,SC",
    "--sub-entry-size, SUB_ENTRY_SIZE",
    "-ses,SES"
  })
  void optionToEnvironmentVariable(String option, String envVariable) {
    assertThat(Utils.OPTION_TO_ENVIRONMENT_VARIABLE.apply(option)).isEqualTo(envVariable);
  }

  @Test
  void superStreamPartitionsTest() {
    assertThat(superStreamPartitions("stream", 3))
        .hasSize(3)
        .containsExactly("stream-0", "stream-1", "stream-2");
    assertThat(superStreamPartitions("stream", 20))
        .hasSize(20)
        .contains("stream-00", "stream-01", "stream-10", "stream-19");
    assertThat(superStreamPartitions("stream-1", 20))
        .hasSize(20)
        .contains("stream-1-00", "stream-1-01", "stream-1-10", "stream-1-19");
  }

  @Command(name = "test-command")
  static class TestCommand {

    @Option(
        names = {"aaa", "a"},
        defaultValue = "10")
    private final int a = 10;

    @Option(names = "b", defaultValue = "false")
    private final boolean b = false;

    @Option(names = "c", defaultValue = "false")
    private final boolean c = false;

    @CommandLine.Option(
        names = {"offset"},
        defaultValue = "next",
        converter = Utils.OffsetSpecificationTypeConverter.class)
    private final OffsetSpecification offsetSpecification = OffsetSpecification.next();

    public TestCommand() {}
  }
}
