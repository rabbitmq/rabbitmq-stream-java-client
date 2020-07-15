// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream;

import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
import io.netty.channel.EventLoopGroup;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public interface EnvironmentBuilder {

    // FIXME add urls parameters for the locator

    StreamEnvironmentBuilder host(String host);

    StreamEnvironmentBuilder port(int port);

    StreamEnvironmentBuilder codec(Codec codec);

    EnvironmentBuilder eventLoopGroup(EventLoopGroup eventLoopGroup);

    StreamEnvironmentBuilder saslConfiguration(SaslConfiguration saslConfiguration);

    StreamEnvironmentBuilder credentialsProvider(CredentialsProvider credentialsProvider);

    StreamEnvironmentBuilder username(String username);

    StreamEnvironmentBuilder password(String password);

    StreamEnvironmentBuilder virtualHost(String virtualHost);

    StreamEnvironmentBuilder requestedHeartbeat(Duration requestedHeartbeat);

    StreamEnvironmentBuilder requestedMaxFrameSize(int requestedMaxFrameSize);

    StreamEnvironmentBuilder channelCustomizer(ChannelCustomizer channelCustomizer);

    StreamEnvironmentBuilder chunkChecksum(ChunkChecksum chunkChecksum);

    StreamEnvironmentBuilder clientProperties(Map<String, String> clientProperties);

    StreamEnvironmentBuilder clientProperty(String key, String value);

    StreamEnvironmentBuilder metricsCollector(MetricsCollector metricsCollector);

    EnvironmentBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

    Environment build();

}
