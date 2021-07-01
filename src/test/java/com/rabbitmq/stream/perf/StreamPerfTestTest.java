// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import com.rabbitmq.stream.impl.Client.StreamParametersBuilder;
import com.rabbitmq.stream.impl.TestUtils;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamPerfTestTest {

  static ExecutorService executor = Executors.newSingleThreadExecutor();
  TestUtils.ClientFactory cf;
  Client client;
  AtomicInteger exitCode;
  String s;
  PrintStream originalOut;
  PrintStream testOut;
  OutputStream testOutputStream;

  @BeforeAll
  static void init() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterAll
  static void tearDown() {
    executor.shutdownNow();
  }

  static void waitOneSecond() throws InterruptedException {
    Thread.sleep(1000L);
  }

  ArgumentsBuilder builder() {
    return new ArgumentsBuilder().stream(s).rate(100);
  }

  @BeforeEach
  void initTest(TestInfo info) {
    exitCode = new AtomicInteger(-1);
    client = cf.get();
    s = TestUtils.streamName(info);
    originalOut = System.out;
    testOutputStream = new ByteArrayOutputStream();
    testOut = new PrintStream(testOutputStream);
    System.setOut(testOut);
  }

  @AfterEach
  void tearDownTest() {
    System.setOut(originalOut);
    client.delete(s);
  }

  private void waitRunEnds(int expectedExitCode) throws Exception {
    TestUtils.waitAtMost(20, () -> exitCode.get() == expectedExitCode);
    testOut.flush();
  }

  private void waitRunEnds() throws Exception {
    waitRunEnds(0);
  }

  String consoleOutput() {
    return testOutputStream.toString();
  }

  @Test
  void helpShouldReturnImmediately() throws Exception {
    run(builder().help());
    waitRunEnds();
    assertThat(consoleOutput()).contains("Usage: stream-perf-test");
  }

  @Test
  void versionShouldReturnAppropriateInformation() throws Exception {
    StreamPerfTest.versionInformation(testOut);
    assertThat(consoleOutput()).contains("RabbitMQ Stream Perf Test");
  }

  @Test
  void streamsShouldNotBeDeletedByDefault() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(streamExists(s)).isTrue();
  }

  @Test
  void streamsShouldBeDeletedWithFlag() throws Exception {
    Future<?> run = run(builder().deleteStreams());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(streamDoesNotExists(s)).isTrue();
  }

  @Test
  void sequenceOfStreamsShouldBeCreatedProperly() throws Exception {
    int streamCount = 5;
    Future<?> run =
        run(builder().streamCount(streamCount).producers(5).consumers(5).deleteStreams());

    String[] expectedStreams =
        IntStream.range(1, streamCount + 1)
            .mapToObj(i -> String.format("%s-%d", s, i))
            .collect(Collectors.toList())
            .toArray(new String[] {});

    waitAtMost(
        () -> {
          Map<String, StreamMetadata> metadata = client.metadata(expectedStreams);
          boolean allExists = metadata.values().stream().allMatch(m -> m.isResponseOk());
          return metadata.size() == streamCount && allExists;
        });

    waitOneSecond();

    run.cancel(true);
    waitRunEnds();

    Map<String, StreamMetadata> metadata = client.metadata(expectedStreams);
    assertThat(metadata.values())
        .hasSize(streamCount)
        .allMatch(m -> m.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
  }

  @Test
  void offsetShouldBeStoredWhenOptionIsEnabled() throws Exception {
    Future<?> run = run(builder().storeEvery(10).consumerNames("consumer-%2$d-on-stream-%1$s"));
    waitUntilStreamExists(s);
    String consumerName = "consumer-1-on-stream-" + s;
    long offset = client.queryOffset(consumerName, s);
    waitOneSecond();
    waitAtMost(() -> client.queryOffset(consumerName, s) > offset);
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void offsetShouldNotBeStoredWhenOptionIsNotEnabled() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    String consumerName = s + "-0"; // convention
    assertThat(client.queryOffset(consumerName, s)).isZero();
    waitOneSecond();
    assertThat(client.queryOffset(consumerName, s)).isZero();
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  @DisabledIfTlsNotEnabled
  void shouldConnectWithTls() throws Exception {
    Future<?> run = run(builder().uris("rabbitmq-stream+tls://guest:guest@localhost:5551/%2f"));
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(streamExists(s)).isTrue();
  }

  @Test
  void shouldConnectWithLoadBalancer() throws Exception {
    Future<?> run = run(builder().loadBalancer(true));
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(streamExists(s)).isTrue();
  }

  @Test
  void streamCreationIsIdempotentWhateverTheDifferencesInStreamProperties() throws Exception {
    Response response =
        client.create(
            s,
            new StreamParametersBuilder()
                .maxLengthBytes(ByteCapacity.GB(1))
                .maxSegmentSizeBytes(ByteCapacity.MB(500))
                .leaderLocator(LeaderLocator.LEAST_LEADERS)
                .build());
    assertThat(response.isOk()).isTrue();
    Future<?> run =
        run(
            builder()
                .maxLengthBytes(ByteCapacity.GB(42)) // different than already existing stream
                .streamMaxSegmentSizeBytes(ByteCapacity.MB(500))
                .leaderLocator(LeaderLocator.LEAST_LEADERS));
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput()).contains("Warning: stream '" + s + "'");
  }

  @Test
  void byteRatesShouldBeIncludedWhenOptionIsEnabled() throws Exception {
    Future<?> run = run(builder().byteRates());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput()).contains("written bytes").contains("read bytes");
  }

  boolean streamExists(String stream) {
    return client.metadata(stream).get(stream).isResponseOk();
  }

  boolean streamDoesNotExists(String stream) {
    return client.metadata(stream).get(stream).getResponseCode()
        == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
  }

  void waitUntilStreamExists(String stream) throws Exception {
    waitAtMost(() -> streamExists(stream));
  }

  Future<?> run(ArgumentsBuilder builder) {
    return executor.submit(() -> exitCode.set(StreamPerfTest.run(builder.build().split(" "))));
  }

  static class ArgumentsBuilder {

    private final Map<String, String> arguments = new HashMap<>();

    ArgumentsBuilder uris(String url) {
      arguments.put("uris", url);
      return this;
    }

    ArgumentsBuilder byteRates() {
      arguments.put("metrics-byte-rates", "");
      return this;
    }

    ArgumentsBuilder help() {
      arguments.put("help", "");
      return this;
    }

    ArgumentsBuilder maxLengthBytes(ByteCapacity capacity) {
      arguments.put("max-length-bytes", capacity.toString());
      return this;
    }

    ArgumentsBuilder streamMaxSegmentSizeBytes(ByteCapacity capacity) {
      arguments.put("stream-max-segment-size-bytes", capacity.toString());
      return this;
    }

    ArgumentsBuilder leaderLocator(LeaderLocator leaderLocator) {
      arguments.put(
          "leader-locator", leaderLocator.toString().toLowerCase(Locale.ENGLISH).replace("_", "-"));
      return this;
    }

    ArgumentsBuilder loadBalancer(boolean loadBalancer) {
      arguments.put("load-balancer", String.valueOf(loadBalancer));
      return this;
    }

    ArgumentsBuilder rate(int rate) {
      arguments.put("rate", String.valueOf(rate));
      return this;
    }

    ArgumentsBuilder stream(String stream) {
      arguments.put("streams", stream);
      return this;
    }

    ArgumentsBuilder deleteStreams() {
      arguments.put("delete-streams", "");
      return this;
    }

    ArgumentsBuilder streamCount(int count) {
      arguments.put("stream-count", String.valueOf(count));
      return this;
    }

    ArgumentsBuilder producers(int count) {
      arguments.put("producers", String.valueOf(count));
      return this;
    }

    ArgumentsBuilder consumers(int count) {
      arguments.put("consumers", String.valueOf(count));
      return this;
    }

    ArgumentsBuilder storeEvery(int storeEvery) {
      arguments.put("store-every", String.valueOf(storeEvery));
      return this;
    }

    ArgumentsBuilder consumerNames(String pattern) {
      arguments.put("consumer-names", pattern);
      return this;
    }

    String build() {
      return this.arguments.entrySet().stream()
          .map(e -> "--" + e.getKey() + (e.getValue().isEmpty() ? "" : (" " + e.getValue())))
          .collect(Collectors.joining(" "));
    }
  }
}
