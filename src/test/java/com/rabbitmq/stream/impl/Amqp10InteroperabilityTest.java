// Copyright (c) 2023 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.ClientFactory;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SwiftMqCodec;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.swiftmq.amqp.AMQPContext;
import com.swiftmq.amqp.v100.client.Connection;
import com.swiftmq.amqp.v100.client.Producer;
import com.swiftmq.amqp.v100.client.QoS;
import com.swiftmq.amqp.v100.client.Session;
import com.swiftmq.amqp.v100.generated.messaging.delivery_state.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpSequence;
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue;
import com.swiftmq.amqp.v100.generated.messaging.message_format.ApplicationProperties;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.generated.messaging.message_format.MessageAnnotations;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPString;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import com.swiftmq.amqp.v100.types.AMQPType;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@TestUtils.DisabledIfAmqp10NotEnabled
public class Amqp10InteroperabilityTest {

  String stream;
  ClientFactory cf;
  Connection connection;
  Session session;

  private static InstanceOfAssertFactory<
          org.apache.qpid.proton.amqp.messaging.AmqpValue,
          ObjectAssert<org.apache.qpid.proton.amqp.messaging.AmqpValue>>
      qpidAmqpValue() {
    return InstanceOfAssertFactories.type(org.apache.qpid.proton.amqp.messaging.AmqpValue.class);
  }

  @BeforeEach
  void init() throws Exception {
    AMQPContext ctx = new AMQPContext(AMQPContext.CLIENT);
    connection = new Connection(ctx, "localhost", 5672, "guest", "guest");

    connection.connect();
    session = connection.createSession(100, 100);
  }

  @AfterEach
  void tearDown() {
    connection.close();
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_9)
  void publishToStreamQueueConsumeFromStream() throws Exception {
    Producer p = session.createProducer("/amq/queue/" + stream, QoS.AT_LEAST_ONCE);
    AMQPMessage message = new AMQPMessage();
    Properties properties = new Properties();
    properties.setContentType(new AMQPSymbol("text/plain"));
    properties.setGroupId(new AMQPString("my-group"));
    properties.setGroupSequence(new SequenceNo(42L));
    message.setProperties(properties);

    Map<AMQPType, AMQPType> applicationProperties = new HashMap<>();
    applicationProperties.put(new AMQPString("foo"), new AMQPString("bar"));
    message.setApplicationProperties(new ApplicationProperties(applicationProperties));
    Map<AMQPType, AMQPType> messageAnnotations = new HashMap<>();
    messageAnnotations.put(new AMQPSymbol("x-route"), new AMQPString("dummy"));
    message.setMessageAnnotations(new MessageAnnotations(messageAnnotations));

    message.addData(new Data("hello".getBytes(StandardCharsets.UTF_8)));

    p.send(message);
    p.close();

    Message msg = consumeMessage();

    assertThat(msg.getBodyAsBinary()).isEqualTo("hello".getBytes(StandardCharsets.UTF_8));
    com.rabbitmq.stream.Properties props = msg.getProperties();
    assertThat(props.getContentType()).isEqualTo("text/plain");
    assertThat(props.getGroupId()).isEqualTo("my-group");
    assertThat(props.getGroupSequence()).isEqualTo(42L);

    assertThat(msg.getApplicationProperties())
        .hasSameSizeAs(applicationProperties)
        .containsEntry("foo", "bar");

    assertThat(msg.getMessageAnnotations())
        .hasSize(messageAnnotations.size() + 2)
        .containsEntry("x-route", "dummy")
        .containsEntry("x-exchange", "")
        .containsEntry("x-routing-key", stream);
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_9)
  void publishAmqpValueToStreamQueueConsumeFromStream() throws Exception {
    Producer p = session.createProducer("/amq/queue/" + stream, QoS.AT_LEAST_ONCE);
    AMQPMessage message = new AMQPMessage();
    message.setAmqpValue(new AmqpValue(new AMQPString("hello")));
    p.send(message);
    p.close();

    Message msg = consumeMessage();
    assertThatThrownBy(() -> msg.getBodyAsBinary()).isInstanceOf(IllegalStateException.class);
    assertThat(msg.getBody())
        .asInstanceOf(qpidAmqpValue())
        .matches(v -> v.getValue().equals("hello"));
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_9)
  void publishDataSectionsToStreamQueueConsumeFromStream() throws Exception {
    Producer p = session.createProducer("/amq/queue/" + stream, QoS.AT_LEAST_ONCE);
    AMQPMessage message = new AMQPMessage();
    String body = "hello brave new world";
    Arrays.stream(body.split(" "))
        .forEach(d -> message.addData(new Data(d.getBytes(StandardCharsets.UTF_8))));
    p.send(message);
    p.close();

    // QPid does not support body with multiple sections, so using SwiftMQ
    Message msg = consumeMessage(new SwiftMqCodec());
    @SuppressWarnings("unchecked")
    String receivedBody =
        ((List<Data>) msg.getBody())
            .stream()
                .map(d -> new String(d.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining(" "));
    assertThat(receivedBody).isEqualTo(receivedBody);
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_9)
  void publishSequenceSectionsToStreamQueueConsumeFromStream() throws Exception {
    Producer p = session.createProducer("/amq/queue/" + stream, QoS.AT_LEAST_ONCE);
    AMQPMessage message = new AMQPMessage();
    List<AMQPType> sequence = new ArrayList<>();
    sequence.add(new AMQPString("hello"));
    sequence.add(new AMQPString("brave"));
    message.addAmqpSequence(new AmqpSequence(sequence));
    sequence = new ArrayList<>();
    sequence.add(new AMQPString("new"));
    sequence.add(new AMQPString("world"));
    message.addAmqpSequence(new AmqpSequence(sequence));
    p.send(message);
    p.close();

    Message msg = consumeMessage();
    assertThat(msg.getBody())
        .isInstanceOf(org.apache.qpid.proton.amqp.messaging.AmqpSequence.class);
    org.apache.qpid.proton.amqp.messaging.AmqpSequence body =
        (org.apache.qpid.proton.amqp.messaging.AmqpSequence) msg.getBody();
    // SwiftMQ does not seem to read any AMQP sequences, and QPid reads only the first one
    @SuppressWarnings("unchecked")
    List<String> bodyValue = body.getValue();
    assertThat(bodyValue).containsExactly("hello", "brave");
  }

  Message consumeMessage() {
    return consumeMessage(new QpidProtonCodec());
  }

  Message consumeMessage(Codec codec) {
    AtomicReference<Message> messageReference = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    Client client =
        cf.get(
            new ClientParameters()
                .codec(codec)
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        msg) -> {
                      messageReference.set(msg);
                      latch.countDown();
                    }));
    client.subscribe((byte) 0, stream, OffsetSpecification.first(), 10);
    assertThat(latchAssert(latch)).completes();
    return messageReference.get();
  }
}
