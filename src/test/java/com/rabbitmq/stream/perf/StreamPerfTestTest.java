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

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.AddressResolver;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamCreator.LeaderLocator;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamMetadata;
import com.rabbitmq.stream.impl.Client.StreamParametersBuilder;
import com.rabbitmq.stream.impl.TestUtils;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
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
  ByteArrayOutputStream out, err;
  boolean checkErrIsEmpty;

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
    out = new ByteArrayOutputStream();
    err = new ByteArrayOutputStream();
    checkErrIsEmpty = true;
  }

  @AfterEach
  void tearDownTest() {
    if (checkErrIsEmpty) {
      assertThat(consoleErrorOutput()).isEmpty();
    }
    client.delete(s);
  }

  private void waitRunEnds(int expectedExitCode) throws Exception {
    waitAtMost(20, () -> exitCode.get() == expectedExitCode);
  }

  private void waitRunEnds() throws Exception {
    waitRunEnds(0);
  }

  String consoleOutput() {
    return out.toString();
  }

  String consoleErrorOutput() {
    return err.toString();
  }

  @Test
  void helpShouldReturnImmediately() throws Exception {
    run(builder().help());
    waitRunEnds();
    assertThat(consoleOutput()).contains("Usage: stream-perf-test");
  }

  @Test
  void versionShouldReturnAppropriateInformation() {
    StreamPerfTest.versionInformation(new PrintStream(out, true));
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
    String consumerName = s + "-0"; // default value when offset tracking is enabled
    assertThat(client.queryOffset(consumerName, s)).isZero();
    waitOneSecond();
    assertThat(client.queryOffset(consumerName, s)).isZero();
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void publishingSequenceShouldBeStoredWhenProducerNamesAreSet() throws Exception {
    Future<?> run = run(builder().producerNames("producer-%2$d-on-stream-%1$s"));
    waitUntilStreamExists(s);
    String producerName = "producer-1-on-stream-" + s;
    long seq = client.queryPublisherSequence(producerName, s);
    waitOneSecond();
    waitAtMost(() -> client.queryPublisherSequence(producerName, s) > seq);
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void publishingSequenceShouldNotBeStoredWhenProducerNamesAreNotSet() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    String producerName = s + "-0"; // shooting in the dark here
    assertThat(client.queryPublisherSequence(producerName, s)).isZero();
    waitOneSecond();
    assertThat(client.queryPublisherSequence(producerName, s)).isZero();
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  @DisabledIfTlsNotEnabled
  void shouldConnectWithTls() throws Exception {
    Future<?> run =
        run(
            builder()
                .uris("rabbitmq-stream+tls://guest:guest@localhost:5551/%2f")
                .serverNameIndication("localhost"));
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

  @Test
  void memoryReportShouldBeIncludedWhenOptionIsEnabled() throws Exception {
    Future<?> run = run(builder().memoryReport());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput()).contains("Max memory").contains("max direct memory");
  }

  @Test
  void subEntriesWithCompressionShouldRun() throws Exception {
    Future<?> run = run(builder().subEntrySize(10).compression(Compression.GZIP));
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void compressionWithoutSubEntriesShouldNotStart() throws Exception {
    run(builder().compression(Compression.GZIP));
    waitRunEnds(1);
    assertThat(consoleErrorOutput())
        .isNotEmpty()
        .contains("Sub-entry batching must be enabled to enable compression");
    checkErrIsEmpty = false;
  }

  @Test
  void monitoringShouldReturnValidEndpoint() throws Exception {
    int monitoringPort = randomNetworkPort();
    Future<?> run =
        run(
            builder()
                .deleteStreams()
                .monitoring(true)
                .monitoringPort(monitoringPort)
                .prometheus(true));
    waitUntilStreamExists(s);
    waitOneSecond();

    waitAtMost(
        10,
        () -> {
          HttpResponse response = httpRequest("http://localhost:" + monitoringPort + "/threaddump");
          return response.responseCode == 200
              && response.body.contains("stream-perf-test-publishers-");
        });

    waitAtMost(
        10,
        () -> {
          HttpResponse response = httpRequest("http://localhost:" + monitoringPort + "/metrics");
          return response.responseCode == 200
              && response.body.contains("# HELP rabbitmq_stream_published_total");
        });
    run.cancel(true);
    waitRunEnds();
  }

  private static HttpResponse httpRequest(String urlString) throws Exception {
    URL url = new URL(urlString);
    HttpURLConnection con = (HttpURLConnection) url.openConnection();
    try {
      con.setRequestMethod("GET");
      con.setConnectTimeout(5000);
      con.setReadTimeout(5000);
      int status = con.getResponseCode();
      try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
        String inputLine;
        StringBuffer content = new StringBuffer();
        while ((inputLine = in.readLine()) != null) {
          content.append(inputLine);
        }
        return new HttpResponse(status, content.toString());
      }
    } finally {
      con.disconnect();
    }
  }

  private static class HttpResponse {

    private final int responseCode;
    private final String body;

    private HttpResponse(int responseCode, String body) {
      this.responseCode = responseCode;
      this.body = body;
    }
  }

  private static int randomNetworkPort() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
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
    AddressResolver addressResolver =
        address ->
            address.port() == Client.DEFAULT_PORT
                ? new Address("localhost", Client.DEFAULT_PORT)
                : new Address("localhost", Client.DEFAULT_TLS_PORT);
    return executor.submit(
        () ->
            exitCode.set(
                StreamPerfTest.run(
                    builder.build().split(" "),
                    new PrintStream(out, true),
                    new PrintStream(err, true),
                    addressResolver)));
  }

  static class ArgumentsBuilder {

    private final Map<String, String> arguments = new HashMap<>();

    ArgumentsBuilder uris(String url) {
      arguments.put("uris", url);
      return this;
    }

    ArgumentsBuilder serverNameIndication(String sni) {
      arguments.put("server-name-indication", sni);
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

    ArgumentsBuilder memoryReport() {
      arguments.put("memory-report", "");
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

    ArgumentsBuilder subEntrySize(int subEntrySize) {
      arguments.put("sub-entry-size", String.valueOf(subEntrySize));
      return this;
    }

    ArgumentsBuilder compression(Compression compression) {
      arguments.put("compression", compression.name().toLowerCase(Locale.ENGLISH));
      return this;
    }

    ArgumentsBuilder consumerNames(String pattern) {
      arguments.put("consumer-names", pattern);
      return this;
    }

    ArgumentsBuilder producerNames(String pattern) {
      arguments.put("producer-names", pattern);
      return this;
    }

    ArgumentsBuilder monitoring(boolean monitoring) {
      arguments.put("monitoring", String.valueOf(monitoring));
      return this;
    }

    ArgumentsBuilder monitoringPort(int port) {
      arguments.put("monitoring-port", String.valueOf(port));
      return this;
    }

    ArgumentsBuilder prometheus(boolean prometheus) {
      arguments.put("prometheus", String.valueOf(prometheus));
      return this;
    }

    String build() {
      return this.arguments.entrySet().stream()
          .map(e -> "--" + e.getKey() + (e.getValue().isEmpty() ? "" : (" " + e.getValue())))
          .collect(Collectors.joining(" "));
    }
  }
}
