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

import static com.rabbitmq.stream.impl.TestUtils.doIfNotNull;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfStompNotEnabled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.messaging.simp.stomp.ReactorNettyTcpStompClient;
import org.springframework.messaging.simp.stomp.StompFrameHandler;
import org.springframework.messaging.simp.stomp.StompHeaders;
import org.springframework.messaging.simp.stomp.StompSession;
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.MimeTypeUtils;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@DisabledIfStompNotEnabled
public class StompInteroperabilityTest {
  static EventLoopGroup eventLoopGroup;
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

  @Test
  void publishToStompDestinationConsumeFromStream() throws Exception {
    byte[] messageBody = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    ReactorNettyTcpStompClient client = null;
    StompSession session = null;
    try {
      client = new ReactorNettyTcpStompClient("localhost", 61613);
      client.setTaskScheduler(taskScheduler);
      StompHeaders headers = new StompHeaders();
      headers.setLogin("guest");
      headers.setPasscode("guest");
      CountDownLatch connectLatch = new CountDownLatch(1);
      session =
          client
              .connect(
                  headers,
                  new StompSessionHandlerAdapter() {
                    @Override
                    public void afterConnected(
                        StompSession session, StompHeaders connectedHeaders) {
                      connectLatch.countDown();
                    }
                  })
              .get();

      assertThat(latchAssert(connectLatch)).completes();

      headers = new StompHeaders();
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
      client = new ReactorNettyTcpStompClient("localhost", 61613);
      client.setTaskScheduler(taskScheduler);
      StompHeaders headers = new StompHeaders();
      headers.setLogin("guest");
      headers.setPasscode("guest");
      CountDownLatch connectLatch = new CountDownLatch(1);
      session =
          client
              .connect(
                  headers,
                  new StompSessionHandlerAdapter() {

                    @Override
                    public Type getPayloadType(StompHeaders headers) {
                      return byte[].class;
                    }

                    @Override
                    public void afterConnected(
                        StompSession session, StompHeaders connectedHeaders) {
                      connectLatch.countDown();
                    }
                  })
              .get();

      assertThat(latchAssert(connectLatch)).completes();

      headers = new StompHeaders();
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

    } finally {
      doIfNotNull(session, StompSession::disconnect);
      doIfNotNull(client, ReactorNettyTcpStompClient::shutdown);
    }
  }

  // TODO test offset specification
}
