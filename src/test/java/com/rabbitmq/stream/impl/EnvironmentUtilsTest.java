// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.declareSuperStreamTopology;
import static com.rabbitmq.stream.impl.TestUtils.deleteSuperStreamTopology;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.rabbitmq.stream.Environment;
import io.netty.channel.EventLoopGroup;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@StreamTestInfrastructure
public class EnvironmentUtilsTest {

  EventLoopGroup eventLoopGroup;
  TestUtils.ClientFactory cf;
  Client configurationClient;
  String superStream;

  @BeforeEach
  void init(TestInfo info) {
    superStream = TestUtils.streamName(info);
    configurationClient = cf.get();
    declareSuperStreamTopology(configurationClient, superStream, "a", "b", "c");
  }

  @AfterEach
  void tearDown() {
    deleteSuperStreamTopology(configurationClient, superStream);
  }

  @Test
  void locatorOperationShouldRunOperationOnLocator() {
    try (Environment env = environment(false)) {
      List<String> partitions =
          EnvironmentUtils.locatorOperation(env, c -> c.partitions(superStream));
      assertThat(partitions)
          .containsExactly(superStream + "-a", superStream + "-b", superStream + "-c");
      List<String> route = EnvironmentUtils.locatorOperation(env, c -> c.route("b", superStream));
      assertThat(route).containsExactly(superStream + "-b");
      route = EnvironmentUtils.locatorOperation(env, c -> c.route("z", superStream));
      assertThat(route).isEmpty();
    }
  }

  @Test
  void locatorOperationShouldInitializeLazyEnvironment() {
    try (Environment env = environment(true)) {
      List<String> partitions =
          EnvironmentUtils.locatorOperation(env, c -> c.partitions(superStream));
      assertThat(partitions).hasSize(3);
    }
  }

  @Test
  void locatorOperationShouldFailOnClosedEnvironment() {
    Environment env = environment(false);
    env.close();
    assertThatThrownBy(() -> EnvironmentUtils.locatorOperation(env, c -> c.partitions(superStream)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void locatorOperationShouldFailOnOtherEnvironmentImplementation() {
    Environment env = mock(Environment.class);
    assertThatThrownBy(() -> EnvironmentUtils.locatorOperation(env, c -> c.partitions(superStream)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private Environment environment(boolean lazy) {
    return Environment.builder()
        .netty()
        .eventLoopGroup(eventLoopGroup)
        .environmentBuilder()
        .lazyInitialization(lazy)
        .build();
  }
}
