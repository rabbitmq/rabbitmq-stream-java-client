// Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.CallableConsumer;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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

  private static int randomNetworkPort() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  ArgumentsBuilder builder() {
    return new ArgumentsBuilder().stream(s).rate(1000);
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
    waitAtMost(
        Duration.ofSeconds(30),
        () -> exitCode.get() == expectedExitCode,
        () -> "Expected " + expectedExitCode + " exit code, got " + exitCode.get());
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
    assertThat(consoleOutput())
        .contains("Display help information about the specified command")
        .contains("stream-perf-test");
  }

  @Test
  void versionShouldReturnAppropriateInformation() {
    StreamPerfTest.versionInformation(new PrintWriter(out, true));
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
    long offset = client.queryOffset(consumerName, s).getOffset();
    waitOneSecond();
    waitAtMost(() -> client.queryOffset(consumerName, s).getOffset() > offset);
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void offsetShouldNotBeStoredWhenOptionIsNotEnabled() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    String consumerName = s + "-0"; // default value when offset tracking is enabled
    assertThat(client.queryOffset(consumerName, s).getOffset()).isZero();
    waitOneSecond();
    assertThat(client.queryOffset(consumerName, s).getOffset()).isZero();
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
                .maxLengthBytes(ByteCapacity.GB(42)) // different from already existing stream
                .streamMaxSegmentSizeBytes(ByteCapacity.MB(500))
                .leaderLocator(LeaderLocator.LEAST_LEADERS));
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput()).contains("Warning: stream '" + s + "'");
  }

  @Test
  void exceedingMaxSegmentSizeLimitShouldGenerateError() throws Exception {
    run(builder().streamMaxSegmentSizeBytes(ByteCapacity.TB(1)));
    waitRunEnds(2);
    assertThat(consoleErrorOutput()).contains("The maximum segment size cannot be more than 3GB");
    checkErrIsEmpty = false;
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
        run(builder().deleteStreams().monitoring().monitoringPort(monitoringPort).prometheus());
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
              && response.body.contains("# HELP rabbitmq_stream_published_total")
              && response.body.contains("rabbitmq_stream_chunk_size{quantile=\"0.5\",}")
              && !response.body.contains("chunk_total");
        });
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void metricsTagsShouldShowUpInHttpEndpoint() throws Exception {
    int monitoringPort = randomNetworkPort();
    Future<?> run =
        run(
            builder()
                .deleteStreams()
                .monitoring()
                .monitoringPort(monitoringPort)
                .prometheus()
                .metricsTags("env=performance,datacenter=eu"));
    waitUntilStreamExists(s);
    waitOneSecond();

    waitAtMost(
        10,
        () -> {
          HttpResponse response = httpRequest("http://localhost:" + monitoringPort + "/metrics");
          return response.responseCode == 200
              && response.body.contains("{datacenter=\"eu\",env=\"performance\",}");
        });
    run.cancel(true);
    waitRunEnds();
  }

  @Test
  void publishConfirmLatencyShouldBeIncludedWhenOptionIsEnabled() throws Exception {
    Future<?> run = run(builder().confirmLatency());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput())
        .contains("confirm latency")
        .doesNotContain("confirm latency 95th 0 ms");
  }

  @Test
  void confirmLatencyShouldBeIncluded() throws Exception {
    Future<?> run = run(builder());
    waitUntilStreamExists(s);
    waitOneSecond();
    run.cancel(true);
    waitRunEnds();
    assertThat(consoleOutput())
        .contains("latency 95th")
        .doesNotContain("latency 95th 0 ms")
        .doesNotContain("confirm latency");
  }

  @Test
  void shouldStopWhenTimeIsSet() throws Exception {
    int time = 3;
    long start = System.nanoTime();
    run(builder().time(time));
    waitUntilStreamExists(s);
    waitRunEnds();
    assertThat(Duration.ofNanos(System.nanoTime() - start))
        .isGreaterThanOrEqualTo(Duration.ofSeconds(3));
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void singleActiveConsumersOnSuperStream() throws Exception {
    String consumerName = "app-1";
    Future<?> run =
        run(
            builder()
                .storeEvery(10)
                .streamCount(2)
                .superStreams()
                .superStreamPartitions(3)
                .singleActiveConsumer()
                .storeEvery(10)
                .consumerNames(consumerName)
                .producers(2)
                .consumers(3)
                .deleteStreams());
    List<String> streams =
        IntStream.range(0, 2)
            .mapToObj(i -> s + "-" + (i + 1)) // the 2 super streams
            .flatMap(s -> IntStream.range(0, 3).mapToObj(i -> s + "-" + i)) // partitions
            .collect(Collectors.toList());
    streams.forEach(s -> waitUntilStreamExists(s));
    waitOneSecond();
    streams.forEach(
        wrap(s -> waitAtMost(() -> client.queryOffset(consumerName, s).getOffset() > 0)));
    run.cancel(true);
    waitRunEnds();

    client
        .metadata(streams.toArray(new String[0]))
        .values()
        .forEach(
            m ->
                assertThat(m.getResponseCode())
                    .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
  }

  private static <T> Consumer<T> wrap(CallableConsumer<T> action) {
    return t -> {
      try {
        action.accept(t);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  boolean streamExists(String stream) {
    return client.metadata(stream).get(stream).isResponseOk();
  }

  boolean streamDoesNotExists(String stream) {
    return client.metadata(stream).get(stream).getResponseCode()
        == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
  }

  void waitUntilStreamExists(String stream) {
    try {
      waitAtMost(() -> streamExists(stream));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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

  private static class HttpResponse {

    private final int responseCode;
    private final String body;

    private HttpResponse(int responseCode, String body) {
      this.responseCode = responseCode;
      this.body = body;
    }
  }

  static class ArgumentsBuilder {

    private final Map<String, String> arguments =
        new HashMap<String, String>() {
          {
            put("rpc-timeout", "20");
          }
        };

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

    ArgumentsBuilder confirmLatency() {
      arguments.put("confirm-latency", "");
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

    ArgumentsBuilder monitoring() {
      arguments.put("monitoring", "");
      return this;
    }

    ArgumentsBuilder monitoringPort(int port) {
      arguments.put("monitoring-port", String.valueOf(port));
      return this;
    }

    ArgumentsBuilder prometheus() {
      arguments.put("prometheus", "");
      return this;
    }

    ArgumentsBuilder metricsTags(String tags) {
      arguments.put("metrics-tags", tags);
      return this;
    }

    ArgumentsBuilder superStreams() {
      arguments.put("super-streams", "");
      return this;
    }

    ArgumentsBuilder superStreamPartitions(int partitions) {
      arguments.put("super-stream-partitions", String.valueOf(partitions));
      return this;
    }

    ArgumentsBuilder singleActiveConsumer() {
      arguments.put("single-active-consumer", "");
      return this;
    }

    ArgumentsBuilder time(int time) {
      arguments.put("time", String.valueOf(time));
      return this;
    }

    String build() {
      return this.arguments.entrySet().stream()
          .map(e -> "--" + e.getKey() + (e.getValue().isEmpty() ? "" : (" " + e.getValue())))
          .collect(Collectors.joining(" "));
    }
  }
}
