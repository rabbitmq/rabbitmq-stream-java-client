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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public interface EnvironmentBuilder {

    // FIXME add urls parameters for the locator

    EnvironmentBuilder uri(String uri);

    EnvironmentBuilder uris(List<String> uris);

    EnvironmentBuilder host(String host);

    EnvironmentBuilder port(int port);

    EnvironmentBuilder codec(Codec codec);

    EnvironmentBuilder eventLoopGroup(EventLoopGroup eventLoopGroup);

    EnvironmentBuilder saslConfiguration(SaslConfiguration saslConfiguration);

    EnvironmentBuilder credentialsProvider(CredentialsProvider credentialsProvider);

    EnvironmentBuilder username(String username);

    EnvironmentBuilder password(String password);

    EnvironmentBuilder virtualHost(String virtualHost);

    EnvironmentBuilder requestedHeartbeat(Duration requestedHeartbeat);

    EnvironmentBuilder requestedMaxFrameSize(int requestedMaxFrameSize);

    EnvironmentBuilder channelCustomizer(ChannelCustomizer channelCustomizer);

    EnvironmentBuilder chunkChecksum(ChunkChecksum chunkChecksum);

    EnvironmentBuilder clientProperties(Map<String, String> clientProperties);

    EnvironmentBuilder clientProperty(String key, String value);

    EnvironmentBuilder metricsCollector(MetricsCollector metricsCollector);

    /**
     * Set the {@link ScheduledExecutorService} used to:
     * <ul>
     *     <li>Schedule producers batch sending</li>
     *     <li>Handle connection recovery</li>
     * </ul>
     * @param scheduledExecutorService the service to use
     * @return this builder instance
     */
    EnvironmentBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

    EnvironmentBuilder recoveryBackOffDelayPolicy(BackOffDelayPolicy recoveryBackOffDelayPolicy);

    Environment build();

}
