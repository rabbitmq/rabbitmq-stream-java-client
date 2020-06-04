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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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

    @ParameterizedTest
    @MethodSource("publishToStreamQueueConsumeFromStreamArguments")
    void publishToStreamQueueConsumeFromStream(Codec codec) throws Exception {
        int messageCount = 10000;
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Date timestamp = new Date();
        try (Connection amqpConnection = connectionFactory.newConnection();
             Channel c = amqpConnection.createChannel()) {
            c.confirmSelect();
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .appId("application id")
                    .clusterId("cluster id") // TODO
                    .contentEncoding("content encoding")
                    .contentType("content type")
                    .correlationId("correlation id")
                    .deliveryMode(2) // TODO
                    .expiration(String.valueOf(System.currentTimeMillis() * 2)) // TODO
                    .messageId("message id")
                    .priority(5)
                    .replyTo("reply to")
                    .timestamp(timestamp)
                    .type("the type")
//                    .userId("guest") // TODO
                    .build();
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
        assertThat(message.getApplicationProperties().get("x-basic-app-id")).isEqualTo("application id");
        assertThat(message.getProperties().getContentEncoding()).isEqualTo("content encoding");
        assertThat(message.getProperties().getContentType()).isEqualTo("content type");
        assertThat(message.getProperties().getCorrelationIdAsString()).isEqualTo("correlation id");
        assertThat(message.getProperties().getMessageIdAsString()).isEqualTo("message id");
        assertThat(message.getProperties().getReplyTo()).isEqualTo("/queue/reply to");
        assertThat(message.getProperties().getCreationTime()).isEqualTo(timestamp.getTime() / 1000);
        assertThat(message.getApplicationProperties().get("x-basic-type")).isEqualTo("the type");
    }

}
