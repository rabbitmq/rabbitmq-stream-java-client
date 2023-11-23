// Copyright (c) 2021-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.impl.TestUtils.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@TestUtils.DisabledIfStompNotEnabled
public class StompInteroperabilityTest {

  public static final String MESSAGE_ID = "message-id";
  public static final String X_STREAM_OFFSET = "x-stream-offset";
  public static final String MESSAGE_COMMAND = "MESSAGE";
  public static final String ACK_COMMAND = "ACK";
  private static final String NEW_LINE = "\n";
  private static final String NULL = String.valueOf('\0');
  static EventLoopGroup eventLoopGroup;
  TestUtils.ClientFactory cf;
  EnvironmentBuilder environmentBuilder;
  String brokerVersion;
  String stream;
  Environment env;
  Socket socket;
  OutputStream out;
  BufferedReader in;
  ExecutorService executorService;

  @BeforeAll
  static void initAll() {
    eventLoopGroup = new NioEventLoopGroup();
  }

  @AfterAll
  static void afterAll() throws Exception {
    eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
  }

  private static FrameBuilder frameBuilder() {
    return new FrameBuilder();
  }

  static boolean hasHeader(String line, String header) {
    return line.contains(header + ":");
  }

  static String header(String line) {
    return line.split(":")[1].trim();
  }

  static long offset(String line) {
    return Long.valueOf(header(line));
  }

  @BeforeEach
  void init() throws Exception {
    environmentBuilder = Environment.builder();
    env = environmentBuilder.netty().eventLoopGroup(eventLoopGroup).environmentBuilder().build();
    socket = new Socket("localhost", 61613);
    out = socket.getOutputStream();
    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    executorService = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  void tearDown() throws Exception {
    env.close();
    socket.close();
    executorService.shutdownNow();
  }

  void stompConnect() throws Exception {
    byte[] connect =
        frameBuilder()
            .command("CONNECT")
            .header("login", "guest")
            .header("passcode", "guest")
            .build();

    out.write(connect);
    String line;
    while ((line = in.readLine()) != null) {
      if (line.equals(NULL)) {
        break;
      }
    }
  }

  void read(Predicate<String> condition) throws Exception {
    read(condition, Duration.ofSeconds(10));
  }

  void read(Predicate<String> condition, Duration timeout) throws Exception {
    Future<Void> task =
        executorService.submit(
            () -> {
              String line;
              while ((line = in.readLine()) != null && !Thread.currentThread().isInterrupted()) {
                if (condition.test(line)) {
                  break;
                }
              }
              return null;
            });
    try {
      task.get(timeout.toMillis(), MILLISECONDS);
    } catch (TimeoutException e) {
      task.cancel(true);
      throw e;
    }
  }

  @Test
  void publishToStompDestinationConsumeFromStream() throws Exception {
    String messageBody = UUID.randomUUID().toString();
    stompConnect();

    String receipt = UUID.randomUUID().toString();
    byte[] frame =
        frameBuilder()
            .command("SEND")
            .header("destination", "/amq/queue/" + stream)
            .header("content-type", "text/plain")
            .header("content-length", String.valueOf(messageBody.length()))
            .header("some-header", "some header value")
            .header("receipt", receipt)
            .body(messageBody)
            .build();

    out.write(frame);

    AtomicBoolean gotReceipt = new AtomicBoolean(false);
    read(
        line -> {
          gotReceipt.compareAndSet(false, line.contains(receipt));
          return line.equals(NULL) && gotReceipt.get();
        });

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Message> messageReference = new AtomicReference<>();
    env.consumerBuilder().stream(stream)
        .offset(OffsetSpecification.first())
        .messageHandler(
            (context, message1) -> {
              messageReference.set(message1);
              latch.countDown();
            })
        .build();

    assertThat(latchAssert(latch)).completes();
    Message message = messageReference.get();
    assertThat(message.getBodyAsBinary()).isEqualTo(messageBody.getBytes(StandardCharsets.UTF_8));
    assertThat(message.getProperties().getContentType()).isEqualTo("text/plain");

    assertThat(message.getApplicationProperties().get("content-length"))
        .isEqualTo(String.valueOf(messageBody.length()));
    assertThat(message.getApplicationProperties().get("receipt"))
        .isNotNull()
        .isInstanceOf(String.class);
    assertThat(message.getApplicationProperties().get("some-header"))
        .isEqualTo("some header value");

    assertThat(message.getMessageAnnotations().get("x-routing-key")).isEqualTo(stream);
    assertThat(message.getMessageAnnotations().get("x-exchange")).isEqualTo("");
  }

  void stompSubscribe(String stream, String ack, int prefetchCount) throws Exception {
    stompSubscribe(stream, ack, prefetchCount, null);
  }

  void stompSubscribe(String stream, String ack, int prefetchCount, String offset)
      throws Exception {
    String receipt = UUID.randomUUID().toString();
    FrameBuilder builder =
        frameBuilder()
            .command("SUBSCRIBE")
            .header("id", "0")
            .header("destination", "/amq/queue/" + stream)
            .header("ack", ack)
            .header("prefetch-count", String.valueOf(prefetchCount))
            .header("receipt", receipt);

    if (offset != null) {
      builder.header("x-stream-offset", offset);
    }
    out.write(builder.build());
    AtomicBoolean gotReceipt = new AtomicBoolean(false);
    read(
        line -> {
          gotReceipt.compareAndSet(false, line.contains(receipt));
          return line.equals(NULL) && gotReceipt.get();
        });
  }

  @Test
  void publishToStreamConsumeFromStomp() throws Exception {
    byte[] messageBody = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

    stompConnect();
    stompSubscribe(stream, "client", 1);

    CountDownLatch publishLatch = new CountDownLatch(1);
    Producer producer = env.producerBuilder().stream(this.stream).build();
    producer.send(
        producer
            .messageBuilder()
            .addData(messageBody)
            .properties()
            .messageId(42)
            .userId("the user ID".getBytes(StandardCharsets.UTF_8))
            .replyTo("reply to")
            .correlationId("the correlation id")
            .contentType("text/plain")
            .contentEncoding("identity")
            .creationTime(1_000_000)
            .messageBuilder()
            .applicationProperties()
            .entry("some-header", "some header value")
            .messageBuilder()
            .build(),
        confirmationStatus -> publishLatch.countDown());

    assertThat(latchAssert(publishLatch)).completes();

    List<String> lines = new CopyOnWriteArrayList<>();
    read(
        line -> {
          lines.add(line);
          return line.contains(NULL);
        });

    assertThat(lines).contains("MESSAGE");

    String payload = null;
    Map<String, String> headers = new HashMap<>();
    for (String line : lines) {
      if (line.contains(NULL)) {
        payload = line.replace(NULL, "");
      } else if (line.contains(":")) {
        headers.put(line.split(":")[0], line.split(":")[1]);
      }
    }

    assertThat(payload).isNotNull().isEqualTo(new String(messageBody));

    if (beforeMessageContainers(brokerVersion)) {
      assertThat(headers.get("x-message-id-type")).isEqualTo("ulong");
    }
    assertThat(headers.get("amqp-message-id")).isEqualTo("42");
    assertThat(headers.get("message-id")).isNotEqualTo("42");
    assertThat(headers.get("user-id")).isEqualTo("the user ID");
    assertThat(headers.get("reply-to")).isEqualTo("/reply-queue/reply to");
    assertThat(headers.get("content-type")).isEqualTo("text/plain");
    assertThat(headers.get("content-encoding")).isEqualTo("identity");
    assertThat(headers.get("correlation-id")).isEqualTo("the correlation id");
    assertThat(headers.get("timestamp")).isEqualTo("1000"); // in seconds
    assertThat(headers.get("some-header")).isEqualTo("some header value");
    assertThat(headers.get("x-stream-offset")).isNotNull().isEqualTo("0");
  }

  @Test
  void offsetTypeFirstShouldStartConsumingFromBeginning() throws Exception {
    int messageCount = 10_000;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();

    stompConnect();
    int prefetchCount = 100;
    stompSubscribe(stream, "client", prefetchCount, "first");

    AtomicInteger count = new AtomicInteger(0);
    AtomicReference<String> lastMessageId = new AtomicReference<>();
    read(
        line -> {
          if (line.contains(MESSAGE_COMMAND)) {
            count.incrementAndGet();
          }
          if (hasHeader(line, MESSAGE_ID)) {
            lastMessageId.set(header(line));
          }
          if (hasHeader(line, X_STREAM_OFFSET)) {
            long offset = offset(line);
            first.compareAndSet(-1, offset);
            last.set(offset);
          }
          if (line.contains(NULL) && count.get() % prefetchCount == 0) {
            write(
                frameBuilder()
                    .command(ACK_COMMAND)
                    .header(MESSAGE_ID, lastMessageId.get())
                    .build());
          }

          return line.contains(NULL) && count.get() == messageCount;
        });

    assertThat(first.get()).isEqualTo(0);
    assertThat(last.get()).isEqualTo(messageCount - 1);
  }

  void write(byte[] content) {
    try {
      out.write(content);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void offsetTypeLastShouldStartConsumingFromTheLastChunk() throws Exception {
    int messageCount = 10_000;
    long lastOffset = messageCount - 1;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);

    // we need a stream client for offset information
    AtomicLong chunkOffset = new AtomicLong(-1);
    Client streamClient =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(
                    (client1, subscriptionId, offset12, messageCount1, dataSize) -> {
                      client1.credit(subscriptionId, 1);
                      chunkOffset.compareAndSet(-1, offset12);
                      return null;
                    }));
    streamClient.subscribe(b(1), stream, OffsetSpecification.last(), 10);

    stompConnect();
    int prefetchCount = 100;
    stompSubscribe(stream, "client", prefetchCount, "last");

    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong(-1);
    AtomicInteger count = new AtomicInteger(0);
    AtomicReference<String> lastMessageId = new AtomicReference<>();
    read(
        line -> {
          if (line.contains(MESSAGE_COMMAND)) {
            count.incrementAndGet();
          }
          if (hasHeader(line, MESSAGE_ID)) {
            lastMessageId.set(header(line));
          }
          if (hasHeader(line, X_STREAM_OFFSET)) {
            long offset = offset(line);
            first.compareAndSet(-1, offset);
            last.set(offset);
          }
          if (line.contains(NULL) && count.get() % prefetchCount == 0) {
            write(
                frameBuilder()
                    .command(ACK_COMMAND)
                    .header(MESSAGE_ID, lastMessageId.get())
                    .build());
          }

          return line.contains(NULL) && last.get() == lastOffset;
        });

    assertThat(first.get()).isEqualTo(chunkOffset.get());
    assertThat(last.get()).isEqualTo(lastOffset);
  }

  @Test
  void offsetTypeNextShouldReturnNewPublishedMessages() throws Exception {
    int firstWaveMessageCount = 10_000;
    int secondWaveMessageCount = 4_000;
    int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
    TestUtils.publishAndWaitForConfirms(cf, firstWaveMessageCount, stream);

    stompConnect();
    int prefetchCount = 100;
    stompSubscribe(stream, "client", prefetchCount, "next");

    AtomicBoolean receivedSomething = new AtomicBoolean(false);

    // should not receive anything
    assertThatThrownBy(
            () ->
                read(
                    line -> {
                      receivedSomething.set(true);
                      return false;
                    },
                    Duration.ofSeconds(2)))
        .isInstanceOf(TimeoutException.class);

    TestUtils.publishAndWaitForConfirms(cf, secondWaveMessageCount, stream);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();
    AtomicInteger count = new AtomicInteger(0);
    AtomicReference<String> lastMessageId = new AtomicReference<>();
    read(
        line -> {
          if (line.contains(MESSAGE_COMMAND)) {
            count.incrementAndGet();
          }
          if (hasHeader(line, MESSAGE_ID)) {
            lastMessageId.set(header(line));
          }
          if (hasHeader(line, X_STREAM_OFFSET)) {
            long offset = offset(line);
            first.compareAndSet(-1, offset);
            last.set(offset);
          }
          if (line.contains(NULL) && count.get() % prefetchCount == 0) {
            write(
                frameBuilder()
                    .command(ACK_COMMAND)
                    .header(MESSAGE_ID, lastMessageId.get())
                    .build());
          }

          return line.contains(NULL) && last.get() == lastOffset;
        });

    assertThat(first.get()).isEqualTo(firstWaveMessageCount);
    assertThat(last.get()).isEqualTo(lastOffset);
  }

  @Test
  void offsetTypeOffsetShouldStartConsumingFromOffset() throws Exception {
    int messageCount = 10_000;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    int offset = messageCount / 10;

    stompConnect();
    int prefetchCount = 100;
    stompSubscribe(stream, "client", prefetchCount, "offset=" + offset);

    AtomicInteger count = new AtomicInteger(0);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();
    AtomicReference<String> lastMessageId = new AtomicReference<>();
    read(
        line -> {
          if (line.contains(MESSAGE_COMMAND)) {
            count.incrementAndGet();
          }
          if (hasHeader(line, MESSAGE_ID)) {
            lastMessageId.set(header(line));
          }
          if (hasHeader(line, X_STREAM_OFFSET)) {
            long messageOffset = offset(line);
            first.compareAndSet(-1, messageOffset);
            last.set(messageOffset);
          }
          if (line.contains(NULL) && count.get() % prefetchCount == 0) {
            write(
                frameBuilder()
                    .command(ACK_COMMAND)
                    .header(MESSAGE_ID, lastMessageId.get())
                    .build());
          }

          return line.contains(NULL) && count.get() == (messageCount - offset);
        });

    assertThat(first.get()).isEqualTo(offset);
    assertThat(last.get()).isEqualTo(messageCount - 1);
  }

  @Test
  void offsetTypeTimestampShouldStartConsumingFromTimestamp() throws Exception {
    int firstWaveMessageCount = 10_000;
    int secondWaveMessageCount = 4_000;
    int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
    TestUtils.publishAndWaitForConfirms(cf, "first wave ", firstWaveMessageCount, stream);
    Thread.sleep(5000);
    long now = System.currentTimeMillis();
    TestUtils.publishAndWaitForConfirms(cf, "second wave ", secondWaveMessageCount, stream);
    long timestampOffset = now - 1000; // one second earlier

    stompConnect();
    int prefetchCount = 100;
    stompSubscribe(
        stream,
        "client",
        prefetchCount,
        "timestamp=" + timestampOffset / 1_000); // must be in seconds

    Set<String> consumed = ConcurrentHashMap.newKeySet();
    AtomicInteger count = new AtomicInteger(0);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong(-1);
    AtomicReference<String> lastMessageId = new AtomicReference<>();
    read(
        line -> {
          if (line.contains(MESSAGE_COMMAND)) {
            count.incrementAndGet();
          }
          if (hasHeader(line, MESSAGE_ID)) {
            lastMessageId.set(header(line));
          }
          if (hasHeader(line, X_STREAM_OFFSET)) {
            long messageOffset = offset(line);
            first.compareAndSet(-1, messageOffset);
            last.set(messageOffset);
          }
          // get the body
          if (line.contains(NULL)) {
            consumed.add(line.replace(NULL, ""));
          }
          if (line.contains(NULL) && count.get() % prefetchCount == 0) {
            write(
                frameBuilder()
                    .command(ACK_COMMAND)
                    .header(MESSAGE_ID, lastMessageId.get())
                    .build());
          }

          return line.contains(NULL) && last.get() == lastOffset;
        });

    assertThat(first.get()).isEqualTo(firstWaveMessageCount);
    assertThat(last.get()).isEqualTo(lastOffset);
    consumed.stream()
        .forEach(v -> assertThat(v).startsWith("second wave").doesNotStartWith("first wave"));
  }

  private static class FrameBuilder {

    private final StringBuilder builder = new StringBuilder();
    private boolean hasBody = false;

    FrameBuilder command(String command) {
      builder.append(command).append(NEW_LINE);
      return this;
    }

    FrameBuilder header(String key, String value) {
      builder.append(key).append(":").append(value).append(NEW_LINE);
      return this;
    }

    FrameBuilder body(String body) {
      this.hasBody = true;
      builder.append(NEW_LINE).append(body);
      return this;
    }

    byte[] build() {
      return (builder + (hasBody ? "" : NEW_LINE) + NULL).getBytes(StandardCharsets.UTF_8);
    }
  }
}
