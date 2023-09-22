// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * API to configure and create an {@link Environment}.
 *
 * @see EnvironmentBuilder
 */
public interface EnvironmentBuilder {

  /**
   * The URI of a node to connect to.
   *
   * <p>URI must be of the form <code>rabbitmq-stream://guest:guest@localhost:5552/%2f</code>.
   *
   * @param uri
   * @return this builder instance
   */
  EnvironmentBuilder uri(String uri);

  /**
   * A list of URIs of nodes of the same cluster to use to connect to.
   *
   * <p>URIs must be of the form <code>rabbitmq-stream://guest:guest@localhost:5552/%2f</code>.
   *
   * @param uris
   * @return this builder instance
   */
  EnvironmentBuilder uris(List<String> uris);

  /**
   * An {@link AddressResolver} to potentially change resolved node address to connect to.
   *
   * <p>Applications can use this abstraction to make sure connection attempts ignore metadata hints
   * and always go to a single point like a load balancer.
   *
   * <p>The default implementation does not perform any logic, it just returns the passed-in
   * address.
   *
   * <p><i>The default implementation is overridden automatically if the following conditions are
   * met: the host to connect to is <code>localhost</code>, the user is <code>guest</code>, and no
   * address resolver has been provided. The client will then always tries to connect to <code>
   * localhost</code> to facilitate local development. Just provide a pass-through address resolver
   * to avoid this behavior, e.g.:</i>
   *
   * <pre>
   * Environment.builder()
   *   .addressResolver(address -> address)
   *   .build();
   * </pre>
   *
   * @param addressResolver
   * @return this builder instance
   * @see <a href="https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/">"Connecting to
   *     Streams" blog post</a>
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
   * Informational ID for this environment instance.
   *
   * <p>This is currently used as a prefix for connection names. The broker does not enforce any
   * kind of uniqueness based on this property. Default to <code>rabbitmq-stream</code>.
   *
   * @param id
   * @return this builder instance
   */
  EnvironmentBuilder id(String id);

  /**
   * Compression codec factory to use for compression in sub-entry batching.
   *
   * @param compressionCodecFactory
   * @return this builder instance
   * @see ProducerBuilder#subEntrySize(int)
   * @see ProducerBuilder#compression(Compression)
   */
  EnvironmentBuilder compressionCodecFactory(CompressionCodecFactory compressionCodecFactory);

  /**
   * Timeout for RPC calls.
   *
   * <p>Default is 10 seconds.
   *
   * @param timeout
   * @return this builder instance
   */
  EnvironmentBuilder rpcTimeout(Duration timeout);

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
   * The heartbeat to request.
   *
   * <p>Default is 60 seconds.
   *
   * @param requestedHeartbeat
   * @return this builder instance
   * @see <a href="https://rabbitmq.com/stream.html#protocol">Stream plugin documentation</a>
   */
  EnvironmentBuilder requestedHeartbeat(Duration requestedHeartbeat);

  /**
   * The maximum frame size to request.
   *
   * <p>Default is 1048576.
   *
   * @param requestedMaxFrameSize
   * @return this builder instance
   * @see <a href="https://rabbitmq.com/stream.html#protocol">Stream plugin documentation</a>
   */
  EnvironmentBuilder requestedMaxFrameSize(int requestedMaxFrameSize);

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

  EnvironmentBuilder observationCollector(ObservationCollector<?> observationCollector);

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
   * The maximum number of tracking consumers allocated to a single connection.
   *
   * <p>Default is 50, which is the maximum value.
   *
   * @param maxTrackingConsumersByConnection
   * @return this builder instance
   */
  EnvironmentBuilder maxTrackingConsumersByConnection(int maxTrackingConsumersByConnection);

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
   * To delay the connection opening until necessary.
   *
   * <p>No connection will be open before it is necessary (for stream management or
   * producer/consumer creation).
   *
   * <p>Default is false.
   *
   * @param lazy
   * @return this builder instance
   */
  EnvironmentBuilder lazyInitialization(boolean lazy);

  /**
   * Flag to force the connection to a stream replica for consumers.
   *
   * <p>The library will always prefer to connect to a stream replica to consume from, but it will
   * fall back to the stream leader if no replica is available. This is the default behavior. Set
   * this flag to <code>true</code> to make the library wait for a replica to become available if
   * only the stream leader is available. This can lead to longer recovery time but help to offload
   * a stream leader and let it deal only with write requests.
   *
   * <p>Note the library performs only 5 attempts to locate a replica before falling back to the
   * leader when the flag is set to <code>true</code>.
   *
   * <p>The {@link #recoveryBackOffDelayPolicy(BackOffDelayPolicy)} and {@link
   * #topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy)} policies control the time between
   * attempts.
   *
   * <p><b>Do not set this flag to <code>true</code> when streams have only 1 member (the leader),
   * e.g. for local development.</b>
   *
   * <p>Default is false.
   *
   * @param forceReplica whether to force the connection to a replica or not
   * @return this builder instance
   * @see #recoveryBackOffDelayPolicy(BackOffDelayPolicy)
   * @see #topologyUpdateBackOffDelayPolicy(BackOffDelayPolicy)
   */
  EnvironmentBuilder forceReplicaForConsumers(boolean forceReplica);

  /**
   * Create the {@link Environment} instance.
   *
   * @return the configured environment
   */
  Environment build();

  /**
   * Enable and configure TLS.
   *
   * @return the TLS configuration helper
   */
  TlsConfiguration tls();

  /** Helper to configure TLS. */
  interface TlsConfiguration {

    /**
     * Enable hostname verification.
     *
     * <p>Hostname verification is enabled by default.
     *
     * @return the TLS configuration helper
     */
    TlsConfiguration hostnameVerification();

    /**
     * Enable or disable hostname verification.
     *
     * <p>Hostname verification is enabled by default.
     *
     * @param hostnameVerification
     * @return the TLS configuration helper
     */
    TlsConfiguration hostnameVerification(boolean hostnameVerification);

    /**
     * Netty {@link SslContext} for TLS connections.
     *
     * <p>Use {@link SslContextBuilder#forClient()} to configure and create an instance.
     *
     * @param sslContext
     * @return the TLS configuration helper
     */
    TlsConfiguration sslContext(SslContext sslContext);

    /**
     * Convenience method to set a {@link SslContext} that trusts all servers.
     *
     * <p>When this feature is enabled, no peer verification is performed, which <strong>provides no
     * protection against Man-in-the-Middle (MITM) attacks</strong>.
     *
     * <p><strong>Use this only in development and QA environments</strong>.
     */
    TlsConfiguration trustEverything();

    /**
     * Go back to the environment builder
     *
     * @return the environment builder
     */
    EnvironmentBuilder environmentBuilder();
  }

  /**
   * Helper to configure netty.
   *
   * @return Netty configuration helper
   */
  NettyConfiguration netty();

  /** Helper to configure Netty */
  interface NettyConfiguration {

    /**
     * The {@link EventLoopGroup} instance to use.
     *
     * <p>The environment uses its own instance by default. It is the developer's responsibility to
     * close the {@link EventLoopGroup} they provide.
     *
     * @param eventLoopGroup
     * @return the Netty configuration helper
     */
    NettyConfiguration eventLoopGroup(EventLoopGroup eventLoopGroup);

    /**
     * Netty's {@link io.netty.buffer.ByteBuf} allocator.
     *
     * @param byteBufAllocator
     * @return the Netty configuration helper
     */
    NettyConfiguration byteBufAllocator(ByteBufAllocator byteBufAllocator);

    /**
     * An extension point to customize Netty's {@link io.netty.channel.Channel}s used for
     * connections.
     *
     * @param channelCustomizer
     * @return the Netty configuration helper
     */
    NettyConfiguration channelCustomizer(Consumer<Channel> channelCustomizer);

    /**
     * An extension point to customize Netty's {@link Bootstrap}s used to configure connections.
     *
     * @param bootstrapCustomizer
     * @return the Netty configuration helper
     */
    NettyConfiguration bootstrapCustomizer(Consumer<Bootstrap> bootstrapCustomizer);

    /**
     * Go back to the environment builder
     *
     * @return the environment builder
     */
    EnvironmentBuilder environmentBuilder();
  }
}
