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

import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfMqttNotEnabled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@DisabledIfMqttNotEnabled
public class MqttInteroperabilityTest {

  static EventLoopGroup eventLoopGroup;
  EnvironmentBuilder environmentBuilder;
  String stream;
  Environment env;

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
  }

  @AfterEach
  void tearDown() {
    env.close();
  }

  @Test
  void publishToMqttTopicConsumeFromStream() throws Exception {
    try (Connection c = new ConnectionFactory().newConnection()) {
      Channel ch = c.createChannel();
      ch.queueBind(stream, "amq.topic", stream);
    }

    byte[] messageBody = UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    String uri = "tcp://localhost:1883";
    String clientId = "mqtt-test";
    try (IMqttClient c = new MqttClient(uri, clientId, null)) {
      MqttConnectOptions opts = new MqttConnectOptions();
      opts.setUserName("guest");
      opts.setPassword("guest".toCharArray());
      opts.setCleanSession(true);
      c.connect(opts);

      MqttTopic topic = c.getTopic(stream);
      MqttMessage message = new MqttMessage(messageBody);
      message.setQos(0);
      message.setRetained(false);
      MqttDeliveryToken token = topic.publish(message);
      token.waitForCompletion();

      c.disconnect(5000);
    }

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
    assertThat(message.getBodyAsBinary()).isEqualTo(messageBody);
    // see
    // https://github.com/rabbitmq/rabbitmq-mqtt/blob/ebcb6dabf0e2b2f34315bc90530ca7791330df24/src/rabbit_mqtt_processor.erl#L856-L860
    assertThat(message.getMessageAnnotations().get("x-basic-delivery-mode"))
        .isEqualTo(UnsignedByte.valueOf("1"));
    assertThat(message.getApplicationProperties().get("x-mqtt-publish-qos"))
        .isEqualTo(Byte.valueOf("0"));
    assertThat(message.getApplicationProperties().get("x-mqtt-dup")).isEqualTo(Boolean.FALSE);
  }
}
