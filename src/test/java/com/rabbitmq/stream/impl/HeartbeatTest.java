// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class HeartbeatTest {

  TestUtils.ClientFactory cf;

  @Test
  void heartbeat() throws Exception {
    ChannelTrafficShapingHandler channelTrafficShapingHandler =
        new ChannelTrafficShapingHandler(0, 0);
    try (Client client =
        cf.get(
            new Client.ClientParameters()
                .requestedHeartbeat(Duration.ofSeconds(2))
                .channelCustomizer(ch -> ch.pipeline().addFirst(channelTrafficShapingHandler)))) {
      TrafficCounter trafficCounter = channelTrafficShapingHandler.trafficCounter();
      long writtenAfterOpening = trafficCounter.cumulativeWrittenBytes();
      long readAfterOpening = trafficCounter.currentReadBytes();

      int heartbeatFrameSize = 4 + 2 + 2;
      int heartbeatFrameCount = 3;
      int expectedAdditionalBytes = heartbeatFrameCount * heartbeatFrameSize;

      waitAtMost(
          15,
          () ->
              trafficCounter.cumulativeWrittenBytes()
                      >= writtenAfterOpening + expectedAdditionalBytes
                  && trafficCounter.cumulativeReadBytes()
                      >= readAfterOpening + expectedAdditionalBytes);
      assertThat(client.isOpen()).isTrue();
    }
  }
}
