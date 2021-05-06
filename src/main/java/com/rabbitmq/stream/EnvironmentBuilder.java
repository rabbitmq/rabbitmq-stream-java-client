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

package com.rabbitmq.stream;

import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

/**
 * API to configure and create an {@link Environment}.
 *
 * @see EnvironmentBuilder
 */
public interface EnvironmentBuilder {

  /**
   * The URI of a node to connect to.
   *
   * <p>URI must be of the form <code>rabbitmq-stream://guest:guest@localhost:5551/%2f</code>.
   *
   * @param uri
   * @return this builder instance
   */
  EnvironmentBuilder uri(String uri);

  /**
   * A list of URIs of nodes of the same cluster to use to connect to.
   *
   * <p>URIs must be of the form <code>rabbitmq-stream://guest:guest@localhost:5551/%2f</code>.
   *
   * @param uris
   * @return this builder instance
   */
  EnvironmentBuilder uris(List<String> uris);

  /**
   * An {@link AddressResolver} to potentially change resolved node address to connect to.
   *
   * <p>Applications can use this abstraction to make sure connection attempts ignore metadata hings
   * and always go to a single point like a load balancer.
   *
   * @param addressResolver
   * @return this builder instance
   */
  EnvironmentBuilder addressResolver(AddressResolver addressResolver);

  /**
   * The host to connect to.
   *
   * @param host
   * @return this builder instance
   */
  EnvironmentBuilder host(String host);

  /**
   * The port to use to connect.
   *
   * @param port
   * @return this builder instance
   */
  EnvironmentBuilder port(int port);

  /**
   * The AMQP 1.0 codec used to encode and decode AMQP 1.0 messages.
   *
   * @param codec
   * @return this builder instance
   */
  EnvironmentBuilder codec(Codec codec);

  /**
   * The Netty {@link EventLoopGroup} instance to use.
   *
   * <p>The environment uses its own instance by default. It is the developer's responsibility to
   * close the {@link EventLoopGroup} they provide.
   *
   * @param eventLoopGroup
   * @return this builder instance
   */
  EnvironmentBuilder eventLoopGroup(EventLoopGroup eventLoopGroup);

  /**
   * The SASL configuration to use.
   *
   * @param saslConfiguration
   * @return this builder instance
   * @see #credentialsProvider(CredentialsProvider)
   */
  EnvironmentBuilder saslConfiguration(SaslConfiguration saslConfiguration);

  /**
   * The {@link CredentialsProvider} to use.
   *
   * @param credentialsProvider
   * @return this builder instance
   * @see #saslConfiguration(SaslConfiguration)
   */
  EnvironmentBuilder credentialsProvider(CredentialsProvider credentialsProvider);

  /**
   * The username to use.
   *
   * @param username
   * @return this builder instance
   */
  EnvironmentBuilder username(String username);

  /**
   * The password to use.
   *
   * @param password
   * @return this builder instance
   */
  EnvironmentBuilder password(String password);

  /**
   * The virtual host to connect to.
   *
   * @param virtualHost
   * @return this builder instance
   */
  EnvironmentBuilder virtualHost(String virtualHost);

  /**
   * The hearbeat to request.
   *
   * <p>Default is 60 seconds.
   *
   * @param requestedHeartbeat
   * @return this builder instance
   */
  EnvironmentBuilder requestedHeartbeat(Duration requestedHeartbeat);

  /**
   * The maximum frame size to request.
   *
   * <p>Default is 1048576.
   *
   * @param requestedMaxFrameSize
   * @return this builder instance
   */
  EnvironmentBuilder requestedMaxFrameSize(int requestedMaxFrameSize);

  /**
   * An extension point to customize Netty's {@link io.netty.channel.Channel}s used for connection.
   *
   * @param channelCustomizer
   * @return this builder instance
   */
  EnvironmentBuilder channelCustomizer(ChannelCustomizer channelCustomizer);

  /**
   * The checksum strategy used for chunk checksum.
   *
   * <p>The default is CRC32 based on JDK implementation.
   *
   * @param chunkChecksum
   * @return this builder instance
   */
  EnvironmentBuilder chunkChecksum(ChunkChecksum chunkChecksum);

  /**
   * Custom client properties to add to default client properties.
   *
   * @param clientProperties
   * @return this builder instance
   */
  EnvironmentBuilder clientProperties(Map<String, String> clientProperties);

  /**
   * Add a custom client properties to default client properties.
   *
   * @param key
   * @param value
   * @return this builder instance
   */
  EnvironmentBuilder clientProperty(String key, String value);

  /**
   * Set up a {@link MetricsCollector}.
   *
   * @param metricsCollector
   * @return this builder instance
   */
  EnvironmentBuilder metricsCollector(MetricsCollector metricsCollector);

  /**
   * The maximum number of producers allocated to a single connection.
   *
   * <p>Default is 256, which is the maximum value.
   *
   * @param maxProducersByConnection
   * @return this builder instance
   */
  EnvironmentBuilder maxProducersByConnection(int maxProducersByConnection);

  /**
   * The maximum number of committing consumers allocated to a single connection.
   *
   * <p>Default is 50, which is the maximum value.
   *
   * @param maxCommittingConsumersByConnection
   * @return this builder instance
   */
  EnvironmentBuilder maxCommittingConsumersByConnection(int maxCommittingConsumersByConnection);

  /**
   * The maximum number of consumers allocated to a single connection.
   *
   * <p>Default is 256, which is the maximum value.
   *
   * @param maxConsumersByConnection
   * @return this builder instance
   */
  EnvironmentBuilder maxConsumersByConnection(int maxConsumersByConnection);

  /**
   * Set the {@link ScheduledExecutorService} used to:
   *
   * <ul>
   *   <li>Schedule producers batch sending
   *   <li>Handle connection recovery
   *   <li>Handle topology update
   * </ul>
   *
   * @param scheduledExecutorService the service to use
   * @return this builder instance
   */
  EnvironmentBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService);

  /**
   * The {@link BackOffDelayPolicy} to use for connection recovery.
   *
   * <p>The default is a fixed delay of 5 seconds.
   *
   * @param recoveryBackOffDelayPolicy
   * @return this builder instance
   */
  EnvironmentBuilder recoveryBackOffDelayPolicy(BackOffDelayPolicy recoveryBackOffDelayPolicy);

  /**
   * The {@link BackOffDelayPolicy} to use for topology recovery.
   *
   * <p>Topology recovery can kick in when streams leaders and replicas move from nodes to nodes in
   * the cluster.
   *
   * <p>The default is a fixed delay of 5 seconds.
   *
   * @param topologyUpdateBackOffDelayPolicy
   * @return this builder instance
   */
  EnvironmentBuilder topologyUpdateBackOffDelayPolicy(
      BackOffDelayPolicy topologyUpdateBackOffDelayPolicy);

  /**
   * Create the {@link Environment} instance.
   *
   * @return the configured environment
   */
  Environment build();
}
