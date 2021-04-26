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

package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.doIfNotNull;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfStompNotEnabled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.messaging.simp.stomp.ReactorNettyTcpStompClient;
import org.springframework.messaging.simp.stomp.StompCommand;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.MimeTypeUtils;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@Disabled
//@DisabledIfStompNotEnabled
public class StompInteroperabilityTest {

  static EventLoopGroup eventLoopGroup;

  TestUtils.ClientFactory cf;
  EnvironmentBuilder environmentBuilder;
  String stream;
  Environment env;
  ThreadPoolTaskScheduler taskScheduler;

  @BeforeAll
  static void initAll() {
    eventLoopGroup = new NioEventLoopGroup();
  }

  @AfterAll
  static void afterAll() throws Exception {
    eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
  }

  static long offset(StompHeaders headers) {
    return Long.valueOf(headers.getFirst("x-stream-offset"));
  }

  @BeforeEach
  void init() {
    environmentBuilder = Environment.builder();
    ((StreamEnvironmentBuilder) environmentBuilder).hostResolver(h -> "localhost");
    env = environmentBuilder.eventLoopGroup(eventLoopGroup).build();
    taskScheduler = new ThreadPoolTaskScheduler();
    taskScheduler.setPoolSize(1);
    taskScheduler.initialize();
  }

  @AfterEach
  void tearDown() {
    env.close();
    taskScheduler.destroy();
  }

  ReactorNettyTcpStompClient client() {
    ReactorNettyTcpStompClient client = new ReactorNettyTcpStompClient("localhost", 61613);
    client.setTaskScheduler(taskScheduler);
    return client;
  }

  StompSession session(ReactorNettyTcpStompClient client) throws Exception {
    CountDownLatch connectLatch = new CountDownLatch(1);
    StompHeaders headers = new StompHeaders();
    headers.setLogin("guest");
    headers.setPasscode("guest");
    StompSession session =
        client
            .connect(
                headers,
                new StompSessionHandlerAdapter() {

                  @Override
                  public void handleException(
                      StompSession session,
                      StompCommand command,
                      StompHeaders headers,
                      byte[] payload,
                      Throwable exception) {
                    exception.printStackTrace();
                  }

                  @Override
                  public void handleTransportError(StompSession session, Throwable exception) {
                    exception.printStackTrace();
                  }

                  @Override
                  public void handleFrame(StompHeaders headers, Object payload) {}

                  @Override
                  public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                    connectLatch.countDown();
                  }
                })
            .get();

    assertThat(latchAssert(connectLatch)).completes();
    return session;
  }

  @Test
  void publishToStompDestinationConsumeFromStream() throws Exception {
    byte[] messageBody = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setReceipt(UUID.randomUUID().toString());

      headers.setContentType(MimeTypeUtils.TEXT_PLAIN);
      headers.set("some-header", "some header value");

      CountDownLatch publishLatch = new CountDownLatch(1);
      session.send(headers, messageBody).addReceiptTask(() -> publishLatch.countDown());

      assertThat(latchAssert(publishLatch)).completes();

    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }

    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Message> messageReference = new AtomicReference<>();
    env.consumerBuilder().stream(stream)
        .messageHandler(
            (context, message1) -> {
              messageReference.set(message1);
              latch.countDown();
            })
        .build();

    assertThat(latchAssert(latch)).completes();
    Message message = messageReference.get();
    assertThat(message.getBodyAsBinary()).isEqualTo(messageBody);
    assertThat(message.getProperties().getContentType())
        .isEqualTo(MimeTypeUtils.TEXT_PLAIN.toString());

    assertThat(message.getApplicationProperties().get("content-length"))
        .isEqualTo(String.valueOf(messageBody.length));
    assertThat(message.getApplicationProperties().get("receipt"))
        .isNotNull()
        .isInstanceOf(String.class);
    assertThat(message.getApplicationProperties().get("some-header"))
        .isEqualTo("some header value");

    assertThat(message.getMessageAnnotations().get("x-routing-key")).isEqualTo(stream);
    assertThat(message.getMessageAnnotations().get("x-exchange")).isEqualTo("");
  }

  @Test
  void publishToStreamConsumeFromStomp() throws Exception {
    byte[] messageBody = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);

    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      headers.set("prefetch-count", "1");

      CountDownLatch consumeLatch = new CountDownLatch(1);
      AtomicReference<Object> payloadReference = new AtomicReference<>();
      AtomicReference<StompHeaders> headersReference = new AtomicReference<>();
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              payloadReference.set(payload);
              headersReference.set(headers);
              consumeLatch.countDown();
            }
          });

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
      assertThat(latchAssert(consumeLatch)).completes();

      assertThat(payloadReference.get()).isEqualTo(messageBody);

      headers = headersReference.get();
      assertThat(headers.getFirst("x-message-id-type")).isEqualTo("ulong");
      assertThat(headers.getFirst("amqp-message-id")).isEqualTo("42");
      assertThat(headers.getMessageId()).isNotEqualTo("42");
      assertThat(headers.getFirst("user-id")).isEqualTo("the user ID");
      assertThat(headers.getFirst("reply-to")).isEqualTo("/reply-queue/reply to");
      assertThat(headers.getContentType()).isEqualTo(MimeTypeUtils.TEXT_PLAIN);
      assertThat(headers.getFirst("content-encoding")).isEqualTo("identity");
      assertThat(headers.getFirst("correlation-id")).isEqualTo("the correlation id");
      assertThat(headers.getFirst("timestamp")).isEqualTo("1000"); // in seconds
      assertThat(headers.getFirst("some-header")).isEqualTo("some header value");
      assertThat(headers.getFirst("x-stream-offset")).isNotNull().isEqualTo("0");
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  @Test
  void offsetTypeFirstShouldStartConsumingFromBeginning() throws Exception {
    int messageCount = 5000;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    CountDownLatch latch = new CountDownLatch(messageCount);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();
    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      int prefetchCount = 100;
      headers.set("prefetch-count", String.valueOf(prefetchCount));
      headers.set("x-stream-offset", "first");

      AtomicInteger count = new AtomicInteger(0);

      AtomicReference<StompSession> sessionReference = new AtomicReference<>(session);
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              long offset = offset(headers);
              first.compareAndSet(-1, offset);
              last.set(offset);
              latch.countDown();
              if (count.incrementAndGet() % prefetchCount == 0) {
                sessionReference.get().acknowledge(headers.getMessageId(), true);
              }
            }
          });

      assertThat(latchAssert(latch)).completes();
      assertThat(first.get()).isEqualTo(0);
      assertThat(last.get()).isEqualTo(messageCount - 1);
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  @Test
  void offsetTypeLastShouldStartConsumingFromBeginning() throws Exception {
    int messageCount = 5000;
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
                    }));
    streamClient.subscribe(b(1), stream, OffsetSpecification.last(), 10);

    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();
    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      int prefetchCount = 100;
      headers.set("prefetch-count", String.valueOf(prefetchCount));
      headers.set("x-stream-offset", "last");

      AtomicInteger count = new AtomicInteger(0);

      AtomicReference<StompSession> sessionReference = new AtomicReference<>(session);
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              long offset = offset(headers);
              first.compareAndSet(-1, offset);
              last.set(offset);
              if (offset == lastOffset) {
                latch.countDown();
              }
              if (count.incrementAndGet() % prefetchCount == 0) {
                sessionReference.get().acknowledge(headers.getMessageId(), true);
              }
            }
          });

      assertThat(latchAssert(latch)).completes();
      assertThat(first.get()).isEqualTo(chunkOffset.get());
      assertThat(last.get()).isEqualTo(lastOffset);
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  @Test
  void offsetTypeNextShouldReturnNewPublishedMessages() throws Exception {
    int firstWaveMessageCount = 5000;
    int secondWaveMessageCount = 2000;
    int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
    TestUtils.publishAndWaitForConfirms(cf, firstWaveMessageCount, stream);
    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();

    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      int prefetchCount = 100;
      headers.set("prefetch-count", String.valueOf(prefetchCount));
      headers.set("x-stream-offset", "next");

      AtomicInteger count = new AtomicInteger(0);

      AtomicReference<StompSession> sessionReference = new AtomicReference<>(session);
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              long offset = offset(headers);
              first.compareAndSet(-1, offset);
              last.set(offset);
              if (offset == lastOffset) {
                latch.countDown();
              }
              if (count.incrementAndGet() % prefetchCount == 0) {
                sessionReference.get().acknowledge(headers.getMessageId(), true);
              }
            }
          });

      assertThat(latchAssert(latch))
          .doesNotComplete(Duration.ofSeconds(2)); // should not receive anything
      TestUtils.publishAndWaitForConfirms(cf, secondWaveMessageCount, stream);
      assertThat(latchAssert(latch)).completes();
      assertThat(first.get()).isEqualTo(firstWaveMessageCount);
      assertThat(last.get()).isEqualTo(lastOffset);
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  @Test
  void offsetTypeOffsetShouldStartConsumingFromOffset() throws Exception {
    int messageCount = 5000;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    int offset = messageCount / 10;
    CountDownLatch latch = new CountDownLatch(messageCount - offset);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();

    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      int prefetchCount = 100;
      headers.set("prefetch-count", String.valueOf(prefetchCount));
      headers.set("x-stream-offset", "offset=" + offset);

      AtomicInteger count = new AtomicInteger(0);

      AtomicReference<StompSession> sessionReference = new AtomicReference<>(session);
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              long messageOffset = offset(headers);
              first.compareAndSet(-1, messageOffset);
              last.set(messageOffset);
              latch.countDown();
              if (count.incrementAndGet() % prefetchCount == 0) {
                sessionReference.get().acknowledge(headers.getMessageId(), true);
              }
            }
          });

      assertThat(latchAssert(latch)).completes();
      assertThat(first.get()).isEqualTo(offset);
      assertThat(last.get()).isEqualTo(messageCount - 1);
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  @Test
  void offsetTypeTimestampShouldStartConsumingFromTimestamp() throws Exception {
    int firstWaveMessageCount = 5000;
    int secondWaveMessageCount = 2000;
    int lastOffset = firstWaveMessageCount + secondWaveMessageCount - 1;
    TestUtils.publishAndWaitForConfirms(cf, "first wave ", firstWaveMessageCount, stream);
    Thread.sleep(5000);
    long now = System.currentTimeMillis();
    TestUtils.publishAndWaitForConfirms(cf, "second wave ", secondWaveMessageCount, stream);
    long timestampOffset = now - 1000; // one second earlier

    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong first = new AtomicLong(-1);
    AtomicLong last = new AtomicLong();
    Set<String> consumed = ConcurrentHashMap.newKeySet();

    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = client();
      session = session(client);

      StompHeaders headers = new StompHeaders();
      headers.setDestination("/amq/queue/" + stream);
      headers.setAck("client");
      int prefetchCount = 100;
      headers.set("prefetch-count", String.valueOf(prefetchCount));
      headers.set("x-stream-offset", "timestamp=" + timestampOffset / 1_000); // must be in seconds

      AtomicInteger count = new AtomicInteger(0);

      AtomicReference<StompSession> sessionReference = new AtomicReference<>(session);
      session.subscribe(
          headers,
          new StompFrameHandler() {
            @Override
            public Type getPayloadType(StompHeaders headers) {
              return byte[].class;
            }

            @Override
            public void handleFrame(StompHeaders headers, Object payload) {
              long messageOffset = offset(headers);
              first.compareAndSet(-1, messageOffset);
              last.set(messageOffset);
              consumed.add(new String((byte[]) payload, StandardCharsets.UTF_8));
              if (messageOffset == lastOffset) {
                latch.countDown();
              }
              if (count.incrementAndGet() % prefetchCount == 0) {
                sessionReference.get().acknowledge(headers.getMessageId(), true);
              }
            }
          });

      assertThat(latchAssert(latch)).completes();
      assertThat(first.get()).isEqualTo(firstWaveMessageCount);
      assertThat(last.get()).isEqualTo(lastOffset);
      consumed.stream()
          .forEach(v -> assertThat(v).startsWith("second wave").doesNotStartWith("first wave"));
    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }
}
