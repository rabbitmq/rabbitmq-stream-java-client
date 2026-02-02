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

import static com.rabbitmq.stream.StreamCreator.LeaderLocator.CLIENT_LOCAL;
import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_4_3_0;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfNotCluster;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

@DisabledIfNotCluster
@StreamTestInfrastructure
public class ClusterTest {

  private static final int NODE1_PORT = 5552;
  private static final int NODE2_PORT = 5553;
  Environment environment;
  TestInfo testInfo;
  EventLoopGroup eventLoopGroup;
  EnvironmentBuilder environmentBuilder;
  TestUtils.ClientFactory cf;

  @BeforeEach
  void init(TestInfo info) {
    environmentBuilder =
        Environment.builder()
            .port(NODE1_PORT)
            .netty()
            .eventLoopGroup(eventLoopGroup)
            .environmentBuilder();
    this.testInfo = info;
  }

  @AfterEach
  void tearDown() {
    if (environment != null) {
      environment.close();
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_3_0)
  @Disabled
  void resolveOffsetSpecShouldReturnResolvedOffsetEvenFromNonMemberNode() {
    environment = environmentBuilder.build();
    String stream = TestUtils.streamName(testInfo);
    environment
        .streamCreator()
        .name(stream)
        .leaderLocator(CLIENT_LOCAL)
        .initialMemberCount(1)
        .create();
    try {
      Client client = cf.get(new ClientParameters().port(NODE2_PORT));
      ClientTest.doResolveOffsetSpecShouldReturnResolvedOffset(cf, client, stream);
    } finally {
      environment.deleteStream(stream);
    }
  }
}
