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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

class StreamEnvironmentBuilder implements EnvironmentBuilder {

    private final Client.ClientParameters clientParameters = new Client.ClientParameters();
    private ScheduledExecutorService scheduledExecutorService;
    private List<URI> uris = Collections.emptyList();

    @Override
    public StreamEnvironmentBuilder uri(String uri) {
        try {
            this.uris = Collections.singletonList(new URI(uri));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid URI: " + uri, e);
        }
        return this;
    }

    @Override
    public StreamEnvironmentBuilder uris(List<String> uris) {
        if (uris == null) {
            throw new IllegalArgumentException("URIs parameter cannot be null");
        }
        this.uris = uris.stream().map(uriString -> {
            try {
                return new URI(uriString);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URI: " + uriString, e);
            }
        }).collect(Collectors.toList());
        return this;
    }

    public StreamEnvironmentBuilder host(String host) {
        this.clientParameters.host(host);
        return this;
    }

    public StreamEnvironmentBuilder port(int port) {
        this.clientParameters.port(port);
        return this;
    }

    public StreamEnvironmentBuilder codec(Codec codec) {
        this.clientParameters.codec(codec);
        return this;
    }

    public EnvironmentBuilder eventLoopGroup(EventLoopGroup eventLoopGroup) {
        this.clientParameters.eventLoopGroup(eventLoopGroup);
        return this;
    }

    public StreamEnvironmentBuilder saslConfiguration(SaslConfiguration saslConfiguration) {
        this.clientParameters.saslConfiguration(saslConfiguration);
        return this;
    }

    public StreamEnvironmentBuilder credentialsProvider(CredentialsProvider credentialsProvider) {
        this.clientParameters.credentialsProvider(credentialsProvider);
        return this;
    }

    public StreamEnvironmentBuilder username(String username) {
        this.clientParameters.username(username);
        return this;
    }

    public StreamEnvironmentBuilder password(String password) {
        this.clientParameters.password(password);
        return this;
    }

    public StreamEnvironmentBuilder virtualHost(String virtualHost) {
        this.clientParameters.virtualHost(virtualHost);
        return this;
    }

    public StreamEnvironmentBuilder requestedHeartbeat(Duration requestedHeartbeat) {
        this.clientParameters.requestedHeartbeat(requestedHeartbeat);
        return this;
    }

    public StreamEnvironmentBuilder requestedMaxFrameSize(int requestedMaxFrameSize) {
        this.clientParameters.requestedMaxFrameSize(requestedMaxFrameSize);
        return this;
    }

    public StreamEnvironmentBuilder channelCustomizer(ChannelCustomizer channelCustomizer) {
        this.clientParameters.channelCustomizer(channelCustomizer);
        return this;
    }

    public StreamEnvironmentBuilder chunkChecksum(ChunkChecksum chunkChecksum) {
        this.clientParameters.chunkChecksum(chunkChecksum);
        return this;
    }

    public StreamEnvironmentBuilder clientProperties(Map<String, String> clientProperties) {
        this.clientParameters.clientProperties(clientProperties);
        return this;
    }

    public StreamEnvironmentBuilder clientProperty(String key, String value) {
        this.clientParameters.clientProperty(key, value);
        return this;
    }

    public StreamEnvironmentBuilder metricsCollector(MetricsCollector metricsCollector) {
        this.clientParameters.metricsCollector(metricsCollector);
        return this;
    }

    public EnvironmentBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    @Override
    public Environment build() {
        return new StreamEnvironment(scheduledExecutorService, clientParameters, uris);
    }
}
