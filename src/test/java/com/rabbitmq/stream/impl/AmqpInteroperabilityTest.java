// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.ok;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.*;
import com.rabbitmq.client.impl.LongStringHelper;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SwiftMqCodec;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class AmqpInteroperabilityTest {

  static final Charset UTF8 = StandardCharsets.UTF_8;

  String stream;
  TestUtils.ClientFactory cf;
  String brokerVersion;

  static Stream<Codec> codecs() {
    return Stream.of(new QpidProtonCodec(), new SwiftMqCodec());
  }

  private static HeaderTestConfiguration htc(
      Consumer<Map<String, Object>> headerValue, Consumer<Map<String, Object>> assertion) {
    return new HeaderTestConfiguration(headerValue, assertion);
  }

  private static PropertiesTestConfiguration ptc(
      Predicate<String> condition,
      Consumer<AMQP.BasicProperties.Builder> builder,
      Consumer<Message> assertion) {
    return new PropertiesTestConfiguration(builder, assertion, condition);
  }

  private static PropertiesTestConfiguration ptc(
      Consumer<AMQP.BasicProperties.Builder> builder, Consumer<Message> assertion) {
    return new PropertiesTestConfiguration(builder, assertion, ignored -> true);
  }

  static MessageOperation mo(
      Predicate<String> brokerVersionCondition,
      Consumer<MessageBuilder> messageBuilderConsumer,
      Consumer<Delivery> deliveryConsumer) {
    return new MessageOperation(messageBuilderConsumer, deliveryConsumer, brokerVersionCondition);
  }

  static MessageOperation mo(
      Consumer<MessageBuilder> messageBuilderConsumer, Consumer<Delivery> deliveryConsumer) {
    return new MessageOperation(messageBuilderConsumer, deliveryConsumer, ignored -> true);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void publishToStreamQueueConsumeFromStream(Codec codec) throws Exception {
    int messageCount = 10_000;
    ConnectionFactory connectionFactory = new ConnectionFactory();
    Date timestamp = new Date();

    Supplier<Stream<PropertiesTestConfiguration>> propertiesTestConfigurations =
        () ->
            Stream.of(
                ptc(
                    BEFORE_MESSAGE_CONTAINERS,
                    b -> b.appId("application id"),
                    m ->
                        assertThat(m.getApplicationProperties().get("x-basic-app-id"))
                            .isEqualTo("application id")),
                ptc(
                    AFTER_MESSAGE_CONTAINERS,
                    b -> b.appId("application id"),
                    m -> assertThat(m.getProperties().getGroupId()).isEqualTo("application id")),
                ptc(
                    b -> b.contentEncoding("content encoding"),
                    m ->
                        assertThat(m.getProperties().getContentEncoding())
                            .isEqualTo("content encoding")),
                ptc(
                    b -> b.contentType("content type"),
                    m -> assertThat(m.getProperties().getContentType()).isEqualTo("content type")),
                ptc(
                    b -> b.correlationId("correlation id"),
                    m ->
                        assertThat(m.getProperties().getCorrelationIdAsString())
                            .isEqualTo("correlation id")),
                ptc(
                    BEFORE_MESSAGE_CONTAINERS,
                    b -> b.deliveryMode(2),
                    m ->
                        assertThat(m.getMessageAnnotations().get("x-basic-delivery-mode"))
                            .isEqualTo(UnsignedByte.valueOf("2"))),
                ptc(
                    AFTER_MESSAGE_CONTAINERS,
                    b -> b.deliveryMode(2),
                    m ->
                        assertThat(m.getMessageAnnotations())
                            .doesNotContainKeys(("x-basic-delivery-mode"))),
                ptc(
                    BEFORE_MESSAGE_CONTAINERS,
                    b -> b.expiration(String.valueOf(60_000)),
                    m ->
                        assertThat(m.getMessageAnnotations().get("x-basic-expiration"))
                            .isEqualTo(String.valueOf(60_000))),
                ptc(
                    AFTER_MESSAGE_CONTAINERS,
                    b -> b.expiration(String.valueOf(60_000)),
                    m ->
                        assertThat(m.getMessageAnnotations())
                            .doesNotContainKeys("x-basic-expiration")),
                ptc(
                    b -> b.messageId("message id"),
                    m ->
                        assertThat(m.getProperties().getMessageIdAsString())
                            .isEqualTo("message id")),
                ptc(
                    BEFORE_MESSAGE_CONTAINERS,
                    b -> b.priority(5),
                    m ->
                        assertThat(m.getMessageAnnotations().get("x-basic-priority"))
                            .isEqualTo(UnsignedByte.valueOf("5"))),
                ptc(
                    AFTER_MESSAGE_CONTAINERS,
                    b -> b.priority(5),
                    m ->
                        assertThat(m.getMessageAnnotations())
                            .doesNotContainKeys("x-basic-priority")),
                ptc(
                    b -> b.replyTo("reply to"),
                    m -> assertThat(m.getProperties().getReplyTo()).isEqualTo("reply to")),
                ptc(
                    b -> b.timestamp(timestamp),
                    m ->
                        assertThat(m.getProperties().getCreationTime())
                            .isEqualTo(
                                (timestamp.getTime() / 1000)
                                    * 1000)), // in seconds in 091, in ms in 1.0, so losing some
                // precision
                ptc(
                    BEFORE_MESSAGE_CONTAINERS,
                    b -> b.type("the type"),
                    m ->
                        assertThat(m.getApplicationProperties().get("x-basic-type"))
                            .isEqualTo("the type")),
                ptc(
                    AFTER_MESSAGE_CONTAINERS,
                    b -> b.type("the type"),
                    m ->
                        assertThat(m.getMessageAnnotations().get("x-basic-type"))
                            .isEqualTo("the type")),
                ptc(
                    b -> b.userId("guest"),
                    m ->
                        assertThat(m.getProperties().getUserId())
                            .isEqualTo("guest".getBytes(UTF8))));

    Supplier<Stream<HeaderTestConfiguration>> headerApplicationPropertiesTestConfigurations =
        () ->
            Stream.of(
                htc(
                    h -> h.put("long.string", LongStringHelper.asLongString("long string")),
                    ap -> assertThat(ap.get("long.string")).isEqualTo("long string")),
                htc(
                    h -> h.put("short.string", "short string"),
                    ap -> assertThat(ap.get("short.string")).isEqualTo("short string")),
                htc(
                    h -> h.put("long", Long.MAX_VALUE - 1),
                    ap -> assertThat(ap.get("long")).isEqualTo(Long.MAX_VALUE - 1)),
                htc(
                    h -> h.put("byte", Byte.MAX_VALUE - 1),
                    ap -> assertThat(ap.get("byte")).isEqualTo(Byte.MAX_VALUE - 1)),
                htc(
                    h -> h.put("integer", Integer.MAX_VALUE - 1),
                    ap -> assertThat(ap.get("integer")).isEqualTo(Integer.MAX_VALUE - 1)),
                htc(
                    h -> h.put("double", Double.MAX_VALUE - 1),
                    ap -> assertThat(ap.get("double")).isEqualTo(Double.MAX_VALUE - 1)),
                htc(
                    h -> h.put("float", Float.MAX_VALUE - 1),
                    ap -> assertThat(ap.get("float")).isEqualTo(Float.MAX_VALUE - 1)),
                htc(
                    h -> h.put("boolean", Boolean.FALSE),
                    ap -> assertThat(ap.get("boolean")).isEqualTo(Boolean.FALSE)),
                htc(
                    h -> h.put("binary", "hello".getBytes(UTF8)),
                    ap -> assertThat(ap.get("binary")).isEqualTo("hello".getBytes(UTF8))),
                htc(
                    h -> h.put("timestamp", timestamp),
                    ap ->
                        assertThat(ap.get("timestamp"))
                            .isEqualTo(
                                (timestamp.getTime() / 1000)
                                    * 1000) // in seconds in 091, in ms in 1.0, so losing some
                    // precision
                    ));

    try (Connection amqpConnection = connectionFactory.newConnection()) {
      Channel c = amqpConnection.createChannel();
      c.confirmSelect();

      Map<String, Object> headers = new HashMap<>();
      headerApplicationPropertiesTestConfigurations
          .get()
          .forEach(configuration -> configuration.headerValue.accept(headers));

      AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
      propertiesTestConfigurations
          .get()
          .filter(configuration -> configuration.brokerVersionCondition.test(brokerVersion))
          .forEach(configuration -> configuration.builder.accept(builder));

      AMQP.BasicProperties properties = builder.headers(headers).build();
      for (int i = 0; i < messageCount; i++) {
        c.basicPublish("", stream, properties, ("amqp " + i).getBytes(UTF8));
      }
      c.waitForConfirmsOrDie(10_000);
    }
    CountDownLatch consumedLatch = new CountDownLatch(messageCount);
    Set<String> messageBodies = ConcurrentHashMap.newKeySet(messageCount);
    Set<Message> messages = ConcurrentHashMap.newKeySet(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .codec(codec)
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {
                      messages.add(message);
                      messageBodies.add(new String(message.getBodyAsBinary(), UTF8));
                      consumedLatch.countDown();
                    }));

    client.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(consumedLatch.await(10, SECONDS)).isTrue();
    assertThat(messageBodies).hasSize(messageCount);
    IntStream.range(0, messageCount)
        .forEach(i -> assertThat(messageBodies.contains("amqp " + i)).isTrue());
    Message message = messages.iterator().next();

    propertiesTestConfigurations
        .get()
        .filter(c -> c.brokerVersionCondition.test(brokerVersion))
        .forEach(c -> c.assertion.accept(message));

    assertThat(message.getMessageAnnotations().get("x-exchange")).isEqualTo("");
    assertThat(message.getMessageAnnotations().get("x-routing-key")).isEqualTo(stream);

    headerApplicationPropertiesTestConfigurations
        .get()
        .forEach(c -> c.assertion.accept(message.getApplicationProperties()));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void publishToStreamConsumeFromStreamQueue(Codec codec, TestInfo info) {
    int messageCount = 1_000;
    ConnectionFactory connectionFactory = new ConnectionFactory();
    Date timestamp = new Date();

    UUID messageIdUuid = UUID.randomUUID();
    UUID correlationIdUuid = UUID.randomUUID();
    Supplier<Stream<MessageOperation>> testMessageOperations =
        () ->
            Stream.of(
                mo(
                    mb -> {
                      mb.properties().messageId("the message ID");
                      mb.properties().correlationId("the correlation ID");
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isEqualTo("the message ID");
                      assertThat(d.getProperties().getCorrelationId())
                          .isEqualTo("the correlation ID");
                    }),
                mo(
                    mb -> {
                      mb.properties()
                          .messageId(StringUtils.repeat("*", 300)); // larger than 091 shortstr
                      mb.properties()
                          .correlationId(StringUtils.repeat("*", 300)); // larger than 091 shortstr
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isNull();
                      assertThat(d.getProperties().getCorrelationId()).isNull();
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry(
                              "x-message-id",
                              LongStringHelper.asLongString(StringUtils.repeat("*", 300)))
                          .containsEntry(
                              "x-correlation-id",
                              LongStringHelper.asLongString(StringUtils.repeat("*", 300)));
                    }),
                mo(
                    BEFORE_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId(messageIdUuid);
                      mb.properties().correlationId(correlationIdUuid);
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId())
                          .isEqualTo(messageIdUuid.toString());
                      assertThat(d.getProperties().getCorrelationId())
                          .isEqualTo(correlationIdUuid.toString());
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry("x-message-id-type", LongStringHelper.asLongString("uuid"))
                          .containsEntry(
                              "x-correlation-id-type", LongStringHelper.asLongString("uuid"));
                    }),
                mo(
                    AFTER_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId(messageIdUuid);
                      mb.properties().correlationId(correlationIdUuid);
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId())
                          .isEqualTo("urn:uuid:" + messageIdUuid);
                      assertThat(d.getProperties().getCorrelationId())
                          .isEqualTo("urn:uuid:" + correlationIdUuid);
                    }),
                mo(
                    BEFORE_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId(10);
                      mb.properties().correlationId(20);
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isEqualTo("10");
                      assertThat(d.getProperties().getCorrelationId()).isEqualTo("20");
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry(
                              "x-message-id-type", LongStringHelper.asLongString("ulong"))
                          .containsEntry(
                              "x-correlation-id-type", LongStringHelper.asLongString("ulong"));
                    }),
                mo(
                    AFTER_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId(10);
                      mb.properties().correlationId(20);
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isEqualTo("10");
                      assertThat(d.getProperties().getCorrelationId()).isEqualTo("20");
                      assertThat(d.getProperties().getHeaders())
                          .doesNotContainKeys("x-message-id-type", "x-correlation-id-type");
                    }),
                mo(
                    BEFORE_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId("the message ID".getBytes(UTF8));
                      mb.properties().correlationId("the correlation ID".getBytes(UTF8));
                    },
                    d -> {
                      assertThat(Base64.getDecoder().decode(d.getProperties().getMessageId()))
                          .isEqualTo("the message ID".getBytes(UTF8));
                      assertThat(Base64.getDecoder().decode(d.getProperties().getCorrelationId()))
                          .isEqualTo("the correlation ID".getBytes(UTF8));
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry(
                              "x-message-id-type", LongStringHelper.asLongString("binary"))
                          .containsEntry(
                              "x-correlation-id-type", LongStringHelper.asLongString("binary"));
                    }),
                mo(
                    AFTER_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties().messageId("the message ID".getBytes(UTF8));
                      mb.properties().correlationId("the correlation ID".getBytes(UTF8));
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isNull();
                      assertThat(d.getProperties().getCorrelationId()).isNull();
                      assertThat(d.getProperties().getHeaders().get("x-message-id"))
                          .isEqualTo("the message ID".getBytes(UTF8));
                      assertThat(d.getProperties().getHeaders().get("x-correlation-id"))
                          .isEqualTo("the correlation ID".getBytes(UTF8));
                    }),
                mo(
                    BEFORE_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties()
                          .messageId(
                              StringUtils.repeat("a", 300)
                                  .getBytes(UTF8)); // larger than 091 shortstr
                      mb.properties()
                          .correlationId(
                              StringUtils.repeat("b", 300)
                                  .getBytes(UTF8)); // larger than 091 shortstr
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isNull();
                      assertThat(d.getProperties().getCorrelationId()).isNull();
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry(
                              "x-message-id",
                              LongStringHelper.asLongString(
                                  StringUtils.repeat("a", 300).getBytes(UTF8)))
                          .containsEntry(
                              "x-correlation-id",
                              LongStringHelper.asLongString(
                                  StringUtils.repeat("b", 300).getBytes(UTF8)));
                    }),
                mo(
                    AFTER_MESSAGE_CONTAINERS,
                    mb -> {
                      mb.properties()
                          .messageId(
                              StringUtils.repeat("a", 300)
                                  .getBytes(UTF8)); // larger than 091 shortstr
                      mb.properties()
                          .correlationId(
                              StringUtils.repeat("b", 300)
                                  .getBytes(UTF8)); // larger than 091 shortstr
                    },
                    d -> {
                      assertThat(d.getProperties().getMessageId()).isNull();
                      assertThat(d.getProperties().getCorrelationId()).isNull();
                      assertThat(d.getProperties().getHeaders())
                          .containsEntry(
                              "x-message-id", StringUtils.repeat("a", 300).getBytes(UTF8))
                          .containsEntry(
                              "x-correlation-id", StringUtils.repeat("b", 300).getBytes(UTF8));
                    }));

    testMessageOperations
        .get()
        .filter(
            testMessageOperation -> testMessageOperation.brokerVersionCondition.test(brokerVersion))
        .forEach(
            testMessageOperation -> {
              CountDownLatch confirmLatch = new CountDownLatch(messageCount);
              Client client =
                  cf.get(
                      new Client.ClientParameters()
                          .codec(codec)
                          .publishConfirmListener(
                              (publisherId, publishingId) -> confirmLatch.countDown()));

              String s = streamName(info);
              Client.Response response = client.create(s);
              assertThat(response.isOk()).isTrue();

              Supplier<Stream<MessageOperation>> messageOperations =
                  () ->
                      Stream.of(
                          mo(
                              mb -> mb.properties().userId("the user ID".getBytes(UTF8)),
                              d ->
                                  assertThat(d.getProperties().getUserId())
                                      .isEqualTo("the user ID")),
                          mo(mb -> mb.properties().to("the to address"), d -> {}),
                          mo(mb -> mb.properties().subject("the subject"), d -> {}),
                          mo(
                              mb -> mb.properties().replyTo("the reply to address"),
                              d ->
                                  assertThat(d.getProperties().getReplyTo())
                                      .isEqualTo("the reply to address")),
                          mo(
                              mb -> mb.properties().contentType("the content type"),
                              d ->
                                  assertThat(d.getProperties().getContentType())
                                      .isEqualTo("the content type")),
                          mo(
                              mb -> mb.properties().contentEncoding("the content encoding"),
                              d ->
                                  assertThat(d.getProperties().getContentEncoding())
                                      .isEqualTo("the content encoding")),
                          mo(
                              mb -> mb.properties().absoluteExpiryTime(timestamp.getTime() + 1000),
                              d -> {}),
                          mo(
                              mb -> mb.properties().creationTime(timestamp.getTime()),
                              d ->
                                  assertThat(d.getProperties().getTimestamp().getTime())
                                      .isEqualTo(
                                          (timestamp.getTime() / 1000)
                                              * 1000) // in seconds in 091, in ms in 1.0, so losing
                              // some precision
                              ),
                          mo(mb -> mb.properties().groupId("the group ID"), d -> {}),
                          mo(mb -> mb.properties().groupSequence(10), d -> {}),
                          mo(
                              mb -> mb.properties().replyToGroupId("the reply to group ID"),
                              d -> {}),
                          mo(
                              mb -> mb.applicationProperties().entry("byte", Byte.MAX_VALUE),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("byte", Byte.MAX_VALUE)),
                          mo(
                              mb -> mb.applicationProperties().entry("short", Short.MAX_VALUE),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("short", Short.MAX_VALUE)),
                          mo(
                              mb -> mb.applicationProperties().entry("integer", Integer.MAX_VALUE),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("integer", Integer.MAX_VALUE)),
                          mo(
                              mb -> mb.applicationProperties().entry("long", Long.MAX_VALUE),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("long", Long.MAX_VALUE)),
                          mo(
                              mb -> mb.applicationProperties().entry("string", "a string"),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry(
                                          "string", LongStringHelper.asLongString("a string"))),
                          mo(
                              mb ->
                                  mb.applicationProperties()
                                      .entryTimestamp("timestamp", timestamp.getTime()),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry(
                                          "timestamp",
                                          new Date((timestamp.getTime() / 1000) * 1000))),
                          mo(
                              mb -> mb.applicationProperties().entry("boolean", Boolean.TRUE),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("boolean", Boolean.TRUE)),
                          mo(
                              mb -> mb.applicationProperties().entry("float", 3.14f),
                              d ->
                                  assertThat(d.getProperties().getHeaders())
                                      .containsEntry("float", 3.14f)),
                          mo(
                              mb ->
                                  mb.applicationProperties()
                                      .entry("binary", "hello".getBytes(UTF8)),
                              d -> {
                                LongString expected = LongStringHelper.asLongString("hello");
                                assertThat(d.getProperties().getHeaders())
                                    .containsEntry("binary", expected);
                              }));

              client.declarePublisher(b(1), null, s);
              IntStream.range(0, messageCount)
                  .forEach(
                      i -> {
                        MessageBuilder messageBuilder = client.messageBuilder();
                        messageBuilder.addData(("stream " + i).getBytes(UTF8));

                        testMessageOperation.messageBuilderConsumer.accept(messageBuilder);

                        messageOperations
                            .get()
                            .filter(
                                messageOperation ->
                                    messageOperation.brokerVersionCondition.test(brokerVersion))
                            .forEach(
                                messageOperation ->
                                    messageOperation.messageBuilderConsumer.accept(messageBuilder));

                        client.publish(b(1), singletonList(messageBuilder.build()));
                      });

              try (Connection c = connectionFactory.newConnection()) {
                assertThat(confirmLatch.await(10, SECONDS)).isTrue();

                Channel ch = c.createChannel();
                ch.basicQos(200);
                CountDownLatch consumedLatch = new CountDownLatch(messageCount);
                Set<String> messageBodies = ConcurrentHashMap.newKeySet(messageCount);
                Set<Delivery> messages = ConcurrentHashMap.newKeySet(messageCount);

                ch.basicConsume(
                    s,
                    false,
                    Collections.singletonMap("x-stream-offset", 0),
                    (consumerTag, message) -> {
                      messages.add(message);
                      messageBodies.add(new String(message.getBody(), UTF8));
                      consumedLatch.countDown();
                      ch.basicAck(message.getEnvelope().getDeliveryTag(), false);
                    },
                    consumerTag -> {});

                assertThat(consumedLatch.await(10, SECONDS)).isTrue();
                assertThat(messageBodies).hasSize(messageCount);
                IntStream.range(0, messageCount)
                    .forEach(i -> assertThat(messageBodies.contains("stream " + i)).isTrue());
                Delivery message = messages.iterator().next();

                assertThat(message.getEnvelope().getExchange()).isEmpty();
                assertThat(message.getEnvelope().getRoutingKey()).isEqualTo(s);
                assertThat(message.getProperties().getHeaders()).containsKey("x-stream-offset");

                testMessageOperation.deliveryConsumer.accept(message);

                messageOperations
                    .get()
                    .filter(
                        messageOperation ->
                            messageOperation.brokerVersionCondition.test(brokerVersion))
                    .forEach(messageOperation -> messageOperation.deliveryConsumer.accept(message));

              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              response = client.delete(s);
              assertThat(response.isOk()).isTrue();
            });
  }

  @ParameterizedTest
  @MethodSource("codecs")
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_7)
  void messageWithEmptyBodyAndPropertiesShouldBeConvertedInAmqp(Codec codec) throws Exception {
    Client client = cf.get(new ClientParameters().codec(codec));
    Response response = client.declarePublisher(b(1), null, stream);
    assertThat(response).is(ok());
    Message message = codec.messageBuilder().properties().messageId(1L).messageBuilder().build();
    client.publish(b(1), singletonList(message));

    CountDownLatch consumeLatch = new CountDownLatch(1);
    AtomicReference<Delivery> delivery = new AtomicReference<>();
    ConnectionFactory cf = new ConnectionFactory();
    try (Connection c = cf.newConnection()) {
      Channel ch = c.createChannel();
      ch.basicQos(10);
      ch.basicConsume(
          stream,
          false,
          Collections.singletonMap("x-stream-offset", "first"),
          (consumerTag, message1) -> {
            ch.basicAck(message1.getEnvelope().getDeliveryTag(), false);
            delivery.set(message1);
            consumeLatch.countDown();
          },
          consumerTag -> {});

      assertThat(latchAssert(consumeLatch)).completes();
    }
  }

  private static class PropertiesTestConfiguration {
    final Consumer<AMQP.BasicProperties.Builder> builder;
    final Consumer<Message> assertion;
    final Predicate<String> brokerVersionCondition;

    private PropertiesTestConfiguration(
        Consumer<AMQP.BasicProperties.Builder> builder,
        Consumer<Message> assertion,
        Predicate<String> brokerVersionCondition) {
      this.builder = builder;
      this.assertion = assertion;
      this.brokerVersionCondition = brokerVersionCondition;
    }
  }

  private static class HeaderTestConfiguration {
    final Consumer<Map<String, Object>> headerValue;
    final Consumer<Map<String, Object>> assertion;

    HeaderTestConfiguration(
        Consumer<Map<String, Object>> headerValue, Consumer<Map<String, Object>> assertion) {
      this.headerValue = headerValue;
      this.assertion = assertion;
    }
  }

  private static class MessageOperation {
    final Consumer<MessageBuilder> messageBuilderConsumer;
    final Consumer<Delivery> deliveryConsumer;
    final Predicate<String> brokerVersionCondition;

    MessageOperation(
        Consumer<MessageBuilder> messageBuilderConsumer,
        Consumer<Delivery> deliveryConsumer,
        Predicate<String> brokerVersionCondition) {
      this.messageBuilderConsumer = messageBuilderConsumer;
      this.deliveryConsumer = deliveryConsumer;
      this.brokerVersionCondition = brokerVersionCondition;
    }
  }

  private static final Predicate<String> BEFORE_MESSAGE_CONTAINERS =
      TestUtils::beforeMessageContainers;

  private static final Predicate<String> AFTER_MESSAGE_CONTAINERS =
      TestUtils::afterMessageContainers;
}
