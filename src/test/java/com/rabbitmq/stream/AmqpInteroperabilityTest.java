// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

package com.rabbitmq.stream;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.LongStringHelper;
import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SwiftMqCodec;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class AmqpInteroperabilityTest {

    static final Charset UTF8 = StandardCharsets.UTF_8;

    String stream;
    TestUtils.ClientFactory cf;

    static Stream<Codec> publishToStreamQueueConsumeFromStreamArguments() {
        return Stream.of(new QpidProtonCodec(), new SwiftMqCodec());
    }

    private static HeaderTestConfiguration htc(Consumer<Map<String, Object>> headerValue, Consumer<Map<String, Object>> assertion) {
        return new HeaderTestConfiguration(
                headerValue, assertion
        );
    }

    private static PropertiesTestConfiguration ptc(Consumer<AMQP.BasicProperties.Builder> builder, Consumer<Message> assertion) {
        return new PropertiesTestConfiguration(builder, assertion);
    }

    @ParameterizedTest
    @MethodSource("publishToStreamQueueConsumeFromStreamArguments")
    void publishToStreamQueueConsumeFromStream(Codec codec) throws Exception {
        int messageCount = 10;
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Date timestamp = new Date();

        Supplier<Stream<PropertiesTestConfiguration>> propertiesTestConfigurations = () -> Stream.of(
                ptc(b -> b.appId("application id"), m -> assertThat(m.getApplicationProperties().get("x-basic-app-id")).isEqualTo("application id")),
                ptc(b -> b.contentEncoding("content encoding"), m -> assertThat(m.getProperties().getContentEncoding()).isEqualTo("content encoding")),
                ptc(b -> b.contentType("content type"), m -> assertThat(m.getProperties().getContentType()).isEqualTo("content type")),
                ptc(b -> b.correlationId("correlation id"), m -> assertThat(m.getProperties().getCorrelationIdAsString()).isEqualTo("correlation id")),
                ptc(b -> b.deliveryMode(2), m -> assertThat(m.getMessageAnnotations().get("x-basic-delivery-mode")).isEqualTo(UnsignedByte.valueOf("2"))),
                ptc(b -> b.expiration(String.valueOf(timestamp.getTime() * 2)), m -> assertThat(m.getMessageAnnotations().get("x-basic-expiration")).isEqualTo(String.valueOf(timestamp.getTime() * 2))),
                ptc(b -> b.messageId("message id"), m -> assertThat(m.getProperties().getMessageIdAsString()).isEqualTo("message id")),
                ptc(b -> b.priority(5), m -> assertThat(m.getMessageAnnotations().get("x-basic-priority")).isEqualTo(UnsignedByte.valueOf("5"))),
                ptc(b -> b.replyTo("reply to"), m -> assertThat(m.getProperties().getReplyTo()).isEqualTo("/queue/reply to")),
                ptc(b -> b.timestamp(timestamp), m -> assertThat(m.getProperties().getCreationTime())
                    .isEqualTo((timestamp.getTime() / 1000) * 1000)), // in seconds in 091, in ms in 1.0, so losing some precision
                ptc(b -> b.type("the type"), m -> assertThat(m.getApplicationProperties().get("x-basic-type")).isEqualTo("the type")),
                ptc(b -> b.userId("guest"), m -> assertThat(m.getProperties().getUserId()).isEqualTo("guest".getBytes(UTF8)))
        );

        Supplier<Stream<HeaderTestConfiguration>> headerApplicationPropertiesTestConfigurations = () -> Stream.of(
                htc(
                        h -> h.put("long.string", LongStringHelper.asLongString("long string")),
                        ap -> assertThat(ap.get("long.string")).isEqualTo("long string")
                ),
                htc(
                        h -> h.put("short.string", "short string"),
                        ap -> assertThat(ap.get("short.string")).isEqualTo("short string")
                ),
                htc(
                        h -> h.put("long", Long.MAX_VALUE - 1),
                        ap -> assertThat(ap.get("long")).isEqualTo(Long.MAX_VALUE - 1)
                ),
                htc(
                        h -> h.put("byte", Byte.MAX_VALUE - 1),
                        ap -> assertThat(ap.get("byte")).isEqualTo(Byte.MAX_VALUE - 1)
                ),
                htc(
                        h -> h.put("integer", Integer.MAX_VALUE - 1),
                        ap -> assertThat(ap.get("integer")).isEqualTo(Integer.MAX_VALUE - 1)
                ),
                htc(
                        h -> h.put("double", Double.MAX_VALUE - 1),
                        ap -> assertThat(ap.get("double")).isEqualTo(Double.MAX_VALUE - 1)
                ),
                htc(
                        h -> h.put("float", Float.MAX_VALUE - 1),
                        ap -> assertThat(ap.get("float")).isEqualTo(Float.MAX_VALUE - 1)
                ),
                htc(
                        h -> h.put("boolean", Boolean.FALSE),
                        ap -> assertThat(ap.get("boolean")).isEqualTo(Boolean.FALSE)
                ),
                htc(
                        h -> h.put("binary", "hello".getBytes(UTF8)),
                        ap -> assertThat(ap.get("binary")).isEqualTo("hello".getBytes(UTF8))
                ),
                htc(
                        h -> h.put("timestamp", timestamp),
                        ap -> assertThat(ap.get("timestamp")).isEqualTo((timestamp.getTime() / 1000) * 1000) // in seconds in 091, in ms in 1.0, so losing some precision
                )
        );

        try (Connection amqpConnection = connectionFactory.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.confirmSelect();

            Map<String, Object> headers = new HashMap<>();
            headerApplicationPropertiesTestConfigurations.get().forEach(configuration -> configuration.headerValue.accept(headers));

            AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
            propertiesTestConfigurations.get().forEach(configuration -> configuration.builder.accept(builder));

            AMQP.BasicProperties properties = builder.headers(headers).build();
            for (int i = 0; i < messageCount; i++) {
                c.basicPublish("", stream, properties, ("amqp " + i).getBytes(UTF8));
            }
            c.waitForConfirmsOrDie(10_000);
        }
        CountDownLatch consumedLatch = new CountDownLatch(messageCount);
        Set<String> messageBodies = ConcurrentHashMap.newKeySet(messageCount);
        Set<Message> messages = ConcurrentHashMap.newKeySet(messageCount);
        Client client = cf.get(new Client.ClientParameters()
                .codec(codec)
                .chunkListener((client1, subscriptionId, offset, messageCount1, dataSize) -> client1.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> {
                    messages.add(message);
                    messageBodies.add(new String(message.getBodyAsBinary(), UTF8));
                    consumedLatch.countDown();
                })
        );

        client.subscribe(1, stream, OffsetSpecification.first(), 10);
        assertThat(consumedLatch.await(10, SECONDS)).isTrue();
        assertThat(messageBodies).hasSize(messageCount);
        IntStream.range(0, messageCount).forEach(i -> assertThat(messageBodies.contains("amqp " + i)).isTrue());
        Message message = messages.iterator().next();

        propertiesTestConfigurations.get().forEach(c -> c.assertion.accept(message));

        assertThat(message.getMessageAnnotations().get("x-exchange")).isEqualTo("");
        assertThat(message.getMessageAnnotations().get("x-routing-key")).isEqualTo(stream);

        headerApplicationPropertiesTestConfigurations.get().forEach(c -> c.assertion.accept(message.getApplicationProperties()));
    }

    private static class PropertiesTestConfiguration {
        final Consumer<AMQP.BasicProperties.Builder> builder;
        final Consumer<Message> assertion;

        private PropertiesTestConfiguration(Consumer<AMQP.BasicProperties.Builder> builder, Consumer<Message> assertion) {
            this.builder = builder;
            this.assertion = assertion;
        }
    }

    private static class HeaderTestConfiguration {
        final Consumer<Map<String, Object>> headerValue;
        final Consumer<Map<String, Object>> assertion;

        HeaderTestConfiguration(Consumer<Map<String, Object>> headerValue, Consumer<Map<String, Object>> assertion) {
            this.headerValue = headerValue;
            this.assertion = assertion;
        }
    }

}
