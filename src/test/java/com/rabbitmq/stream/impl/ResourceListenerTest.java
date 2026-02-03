// Copyright (c) 2025-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Resource.State.CLOSED;
import static com.rabbitmq.stream.Resource.State.CLOSING;
import static com.rabbitmq.stream.Resource.State.OPEN;
import static com.rabbitmq.stream.Resource.State.OPENING;
import static com.rabbitmq.stream.Resource.State.RECOVERING;
import static com.rabbitmq.stream.impl.Assertions.assertThat;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.Resource;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@StreamTestInfrastructure
public class ResourceListenerTest {

  String stream;
  EventLoopGroup eventLoopGroup;
  Environment environment;

  @BeforeEach
  void init() {
    environment =
        Environment.builder()
            .recoveryBackOffDelayPolicy(BackOffDelayPolicy.fixed(Duration.ofSeconds(1)))
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder()
            .build();
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @Test
  void publisherListenersShouldBeCalledDuringLifecycle() {
    Queue<Resource.State> states = new ConcurrentLinkedQueue<>();
    TestUtils.Sync sync = TestUtils.sync(1);
    Producer producer =
        environment.producerBuilder().stream(stream)
            .listeners(
                context -> {
                  states.add(context.currentState());
                  if (context.currentState() == OPEN) {
                    sync.down();
                  }
                })
            .build();

    assertThat(sync).completes();
    sync.reset(1);

    Cli.listProducerConnections().forEach(c -> Cli.killConnection(c.clientProvidedName()));

    assertThat(sync).completes();

    producer.close();
    Assertions.assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED);
  }

  @Test
  void consumerListenersShouldBeCalledDuringLifecycle() {
    Queue<Resource.State> states = new ConcurrentLinkedQueue<>();
    TestUtils.Sync sync = TestUtils.sync(1);
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .messageHandler((ctx, msg) -> {})
            .listeners(
                context -> {
                  states.add(context.currentState());
                  if (context.currentState() == OPEN) {
                    sync.down();
                  }
                })
            .build();

    assertThat(sync).completes();
    sync.reset(1);

    Cli.listConsumerConnections().forEach(c -> Cli.killConnection(c.clientProvidedName()));

    assertThat(sync).completes();

    consumer.close();
    Assertions.assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED);
  }

  @Test
  void listenersForNamedConsumerShouldBeCalledDuringLifecycle() {
    Queue<Resource.State> states = new ConcurrentLinkedQueue<>();
    TestUtils.Sync sync = TestUtils.sync(1);
    Consumer consumer =
        environment.consumerBuilder().stream(stream)
            .name("app-1")
            .messageHandler((ctx, msg) -> {})
            .listeners(
                context -> {
                  states.add(context.currentState());
                  if (context.currentState() == OPEN) {
                    sync.down();
                  }
                })
            .build();

    assertThat(sync).completes();
    sync.reset(1);

    Cli.listProducerConnections().forEach(c -> Cli.killConnection(c.clientProvidedName()));

    assertThat(sync).completes();

    consumer.close();
    Assertions.assertThat(states).containsExactly(OPENING, OPEN, RECOVERING, OPEN, CLOSING, CLOSED);
  }
}
