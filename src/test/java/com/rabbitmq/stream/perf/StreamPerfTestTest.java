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

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import com.rabbitmq.stream.impl.TestUtils;
import java.util.HashMap;
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

  @BeforeAll
  static void init() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterAll
  static void tearDown() {
    executor.shutdownNow();
  }

  ArgumentsBuilder builder() {
    return new ArgumentsBuilder().stream(s).rate(100);
  }

  static void waitOneSecond() throws InterruptedException {
    Thread.sleep(1000L);
  }

  @BeforeEach
  void initTest(TestInfo info) {
    exitCode = new AtomicInteger(-1);
    client = cf.get();
    s = TestUtils.streamName(info);
  }

  @AfterEach
  void tearDownTest() {
    client.delete(s);
  }

  private void waitRunEnds(int expectedExitCode) throws Exception {
    waitAtMost(() -> exitCode.get() == expectedExitCode);
  }

  private void waitRunEnds() throws Exception {
    waitRunEnds(0);
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
  void offsetShouldBeCommittedWhenOptionIsEnabled() throws Exception {
    Future<?> run = run(builder().commitEvery(10));
    waitUntilStreamExists(s);
    String consumerName = s + "-0"; // convention
    long offset = client.queryOffset(consumerName, s);
    waitOneSecond();
    waitAtMost(() -> client.queryOffset(consumerName, s) > offset);
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void offsetShouldNotBeCommittedWhenOptionIsNotEnabled() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    String consumerName = s + "-0"; // convention
    assertThat(client.queryOffset(consumerName, s)).isZero();
    waitOneSecond();
    assertThat(client.queryOffset(consumerName, s)).isZero();
    run.cancel(true);
    waitRunEnds();
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

    ArgumentsBuilder commitEvery(int commitEvery) {
      arguments.put("commit-every", String.valueOf(commitEvery));
      return this;
    }

    String build() {
      return this.arguments.entrySet().stream()
          .map(e -> "--" + e.getKey() + (e.getValue().isEmpty() ? "" : (" " + e.getValue())))
          .collect(Collectors.joining(" "));
    }
  }
}
