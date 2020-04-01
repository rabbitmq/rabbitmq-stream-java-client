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

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.rabbitmq.stream.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class HeartbeatTest {

    static EventLoopGroup eventLoopGroup;

    @BeforeAll
    static void initSuite() {
        eventLoopGroup = new NioEventLoopGroup();
    }

    @AfterAll
    static void tearDownSuite() throws Exception {
        eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
    }

    Client client(Client.ClientParameters parameters) {
        Client client = new Client(parameters.eventLoopGroup(eventLoopGroup));
        return client;
    }

    @Test
    void heartbeat() throws Exception {
        ChannelTrafficShapingHandler channelTrafficShapingHandler = new ChannelTrafficShapingHandler(0, 0);
        try (Client client = client(new Client.ClientParameters()
                .requestedHeartbeat(Duration.ofSeconds(2))
                .channelCustomizer(ch -> ch.pipeline().addFirst(channelTrafficShapingHandler)))) {
            TrafficCounter trafficCounter = channelTrafficShapingHandler.trafficCounter();
            long writtenAfterOpening = trafficCounter.cumulativeWrittenBytes();
            long readAfterOpening = trafficCounter.currentReadBytes();

            int heartbeatFrameSize = 4 + 2 + 2;
            int heartbeatFrameCount = 3;
            int expectedAdditionalBytes = heartbeatFrameCount * heartbeatFrameSize;

            waitAtMost(15, () -> trafficCounter.cumulativeWrittenBytes() >= writtenAfterOpening + expectedAdditionalBytes &&
                    trafficCounter.cumulativeReadBytes() >= readAfterOpening + expectedAdditionalBytes
            );
            assertThat(client.isOpen()).isTrue();
        }
    }

}
