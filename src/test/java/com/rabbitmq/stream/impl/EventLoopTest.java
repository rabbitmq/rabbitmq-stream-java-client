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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EventLoopTest {

  EventExecutorGroup eventExecutorGroup;
  EventLoop loop;
  EventLoop.Client<State> client;

  @BeforeEach
  void beforeEach() {
    eventExecutorGroup = new DefaultEventExecutorGroup(1);
    loop = new EventLoop(eventExecutorGroup, Duration.ofSeconds(10));
    client = loop.register(State::new);
  }

  @AfterEach
  void afterEach() {
    loop.close();
    eventExecutorGroup.shutdownGracefully();
  }

  @Test
  void submittedTasksAreAppliedInOrder() {
    client.submit(s -> s.a = 42);
    Integer a = client.query(s -> s.a);
    assertThat(a).isEqualTo(42);

    client.submit(
        s -> {
          s.a = 1;
          s.b = 2;
        });
    a = client.query(s -> s.a);
    Integer b = client.query(s -> s.b);
    assertThat(a).isEqualTo(1);
    assertThat(b).isEqualTo(2);

    client.close();
    assertThatThrownBy(() -> client.submit(s -> {})).isInstanceOf(IllegalStateException.class);
    loop.close();
    assertThatThrownBy(() -> loop.register(State::new)).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void taskThrowingExceptionDoesNotStopTheLoop() {
    client.submit(
        s -> {
          throw new RuntimeException("task failure (expected test failure)");
        });
    client.submit(s -> s.a = 42);
    Integer a = client.query(s -> s.a);
    assertThat(a).isEqualTo(42);
  }

  @Test
  void queryThrowingExceptionReturnsNullAndDoesNotStopTheLoop() {
    Integer result =
        client.query(
            s -> {
              throw new RuntimeException("query failure (expected test failure)");
            });
    assertThat(result).isNull();

    client.submit(s -> s.a = 42);
    Integer a = client.query(s -> s.a);
    assertThat(a).isEqualTo(42);
  }

  @Test
  void queryCalledFromTheLoopThreadDoesNotDeadlock() {
    client.submit(s -> s.a = 42);
    Integer result = client.query(s -> client.query(inner -> inner.a));
    assertThat(result).isEqualTo(42);
  }

  @Test
  void closingClientAfterLoopCloseDoesNotThrow() {
    loop.close();
    assertThatCode(() -> client.close()).doesNotThrowAnyException();
  }

  @Test
  void closedClientRejectsFurtherTasks() {
    client.close();
    assertThat(client.isClosed()).isTrue();
    assertThatThrownBy(() -> client.submit(s -> {})).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void submittingToAnOpenClientAfterLoopCloseThrows() {
    loop.close();
    assertThat(client.isClosed()).isFalse();
    assertThatThrownBy(() -> client.submit(s -> {})).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void stateOfOneClientIsNotVisibleToAnother() {
    EventLoop.Client<State> other = loop.register(State::new);
    client.submit(s -> s.a = 42);
    Integer otherA = other.query(s -> s.a);
    assertThat(otherA).isNull();
  }

  static class State {

    private Integer a, b;
  }
}
