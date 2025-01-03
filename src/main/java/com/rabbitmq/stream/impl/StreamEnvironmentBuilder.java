// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.Utils.DEFAULT_ADDRESS_RESOLVER;
import static com.rabbitmq.stream.impl.Utils.noOpConsumer;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamEnvironmentBuilder implements EnvironmentBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironmentBuilder.class);

  private String id = "rabbitmq-stream";
  private final Client.ClientParameters clientParameters = new Client.ClientParameters();
  private final DefaultTlsConfiguration tls = new DefaultTlsConfiguration(this);
  private final DefaultNettyConfiguration netty = new DefaultNettyConfiguration(this);
  private ScheduledExecutorService scheduledExecutorService;
  private List<URI> uris = Collections.emptyList();
  private BackOffDelayPolicy recoveryBackOffDelayPolicy =
      BackOffDelayPolicy.fixed(Duration.ofSeconds(5));
  private BackOffDelayPolicy topologyBackOffDelayPolicy =
      BackOffDelayPolicy.fixedWithInitialDelay(Duration.ofSeconds(5), Duration.ofSeconds(1));
  private AddressResolver addressResolver = DEFAULT_ADDRESS_RESOLVER;
  private int maxProducersByConnection = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT;
  private int maxTrackingConsumersByConnection =
      ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT;
  private int maxConsumersByConnection = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT;
  private CompressionCodecFactory compressionCodecFactory;
  private boolean lazyInit = false;
  private boolean forceReplicaForConsumers = false;
  private boolean forceLeaderForProducers = true;
  private Function<Client.ClientParameters, Client> clientFactory = Client::new;
  private ObservationCollector<?> observationCollector = ObservationCollector.NO_OP;
  private Duration producerNodeRetryDelay = Duration.ofMillis(500);
  private Duration consumerNodeRetryDelay = Duration.ofMillis(1000);
  private int locatorConnectionCount = -1;

  public StreamEnvironmentBuilder() {}

  private static URI toUri(String uriString) {
    try {
      URI uri = new URI(uriString);
      if (!"rabbitmq-stream".equalsIgnoreCase(uri.getScheme())
          && !"rabbitmq-stream+tls".equalsIgnoreCase(uri.getScheme())) {
        throw new IllegalArgumentException(
            "Wrong scheme in rabbitmq-stream URI: "
                + uri.getScheme()
                + ". Should be rabbitmq-stream or rabbitmq-stream+tls");
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + uriString, e);
    }
  }

  @Override
  public StreamEnvironmentBuilder uri(String uriString) {
    URI uri = toUri(uriString);
    this.uris = Collections.singletonList(uri);
    if (uri.getScheme().toLowerCase().endsWith("+tls")) {
      this.tls.enable();
    }
    return this;
  }

  @Override
  public StreamEnvironmentBuilder uris(List<String> uris) {
    if (uris == null) {
      throw new IllegalArgumentException("URIs parameter cannot be null");
    }
    this.uris = uris.stream().map(StreamEnvironmentBuilder::toUri).collect(Collectors.toList());
    boolean tls =
        this.uris.stream().anyMatch(uri -> uri.getScheme().toLowerCase().endsWith("+tls"));
    if (tls) {
      this.tls.enable();
    }
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

  @Override
  public EnvironmentBuilder compressionCodecFactory(
      CompressionCodecFactory compressionCodecFactory) {
    this.compressionCodecFactory = compressionCodecFactory;
    return this;
  }

  @Override
  public EnvironmentBuilder id(String id) {
    this.id = id;
    return this;
  }

  @Override
  public EnvironmentBuilder rpcTimeout(Duration timeout) {
    this.clientParameters.rpcTimeout(timeout);
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

  public EnvironmentBuilder scheduledExecutorService(
      ScheduledExecutorService scheduledExecutorService) {
    this.scheduledExecutorService = scheduledExecutorService;
    return this;
  }

  @Override
  public EnvironmentBuilder recoveryBackOffDelayPolicy(
      BackOffDelayPolicy recoveryBackOffDelayPolicy) {
    this.recoveryBackOffDelayPolicy = recoveryBackOffDelayPolicy;
    return this;
  }

  @Override
  public EnvironmentBuilder topologyUpdateBackOffDelayPolicy(
      BackOffDelayPolicy topologyUpdateBackOffDelayPolicy) {
    this.topologyBackOffDelayPolicy = topologyUpdateBackOffDelayPolicy;
    return this;
  }

  @Override
  public EnvironmentBuilder addressResolver(AddressResolver addressResolver) {
    this.addressResolver = addressResolver;
    return this;
  }

  @Override
  public EnvironmentBuilder maxProducersByConnection(int maxProducersByConnection) {
    if (maxProducersByConnection < 1
        || maxProducersByConnection > ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT) {
      throw new IllegalArgumentException(
          "maxProducersByConnection must be between 1 and "
              + ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT);
    }
    this.maxProducersByConnection = maxProducersByConnection;
    return this;
  }

  @Override
  public EnvironmentBuilder maxTrackingConsumersByConnection(int maxTrackingConsumersByConnection) {
    if (maxTrackingConsumersByConnection < 1
        || maxTrackingConsumersByConnection
            > ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT) {
      throw new IllegalArgumentException(
          "maxTrackingConsumersByConnection must be between 1 and "
              + ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT);
    }
    this.maxTrackingConsumersByConnection = maxTrackingConsumersByConnection;
    return this;
  }

  @Override
  public EnvironmentBuilder maxConsumersByConnection(int maxConsumersByConnection) {
    if (maxConsumersByConnection < 1
        || maxConsumersByConnection > ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT) {
      throw new IllegalArgumentException(
          "maxConsumersByConnection must be between 1 and "
              + ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT);
    }
    this.maxConsumersByConnection = maxConsumersByConnection;
    return this;
  }

  @Override
  public EnvironmentBuilder lazyInitialization(boolean lazy) {
    this.lazyInit = lazy;
    return this;
  }

  @Override
  public EnvironmentBuilder forceReplicaForConsumers(boolean forceReplica) {
    this.forceReplicaForConsumers = forceReplica;
    return this;
  }

  @Override
  public EnvironmentBuilder forceLeaderForProducers(boolean forceLeader) {
    this.forceLeaderForProducers = forceLeader;
    return this;
  }

  @Override
  public TlsConfiguration tls() {
    this.tls.enable();
    return this.tls;
  }

  @Override
  public NettyConfiguration netty() {
    return this.netty;
  }

  StreamEnvironmentBuilder clientFactory(Function<Client.ClientParameters, Client> clientFactory) {
    this.clientFactory = clientFactory;
    return this;
  }

  @Override
  public EnvironmentBuilder observationCollector(ObservationCollector<?> observationCollector) {
    this.observationCollector = observationCollector;
    return this;
  }

  StreamEnvironmentBuilder producerNodeRetryDelay(Duration producerNodeRetryDelay) {
    this.producerNodeRetryDelay = producerNodeRetryDelay;
    return this;
  }

  StreamEnvironmentBuilder consumerNodeRetryDelay(Duration consumerNodeRetryDelay) {
    this.consumerNodeRetryDelay = consumerNodeRetryDelay;
    return this;
  }

  @Override
  public StreamEnvironmentBuilder locatorConnectionCount(int locatorCount) {
    this.locatorConnectionCount = locatorCount;
    return this;
  }

  @Override
  public Environment build() {
    if (this.compressionCodecFactory == null) {
      this.clientParameters.compressionCodecFactory(CompressionCodecs.DEFAULT);
    } else {
      this.clientParameters.compressionCodecFactory(this.compressionCodecFactory);
    }
    this.id = this.id == null ? "rabbitmq-stream" : this.id;
    Function<ClientConnectionType, String> connectionNamingStrategy =
        Utils.defaultConnectionNamingStrategy(this.id + "-");
    this.clientParameters.eventLoopGroup(this.netty.eventLoopGroup);
    this.clientParameters.byteBufAllocator(this.netty.byteBufAllocator);
    this.clientParameters.channelCustomizer(this.netty.channelCustomizer);
    this.clientParameters.bootstrapCustomizer(this.netty.bootstrapCustomizer);

    return new StreamEnvironment(
        scheduledExecutorService,
        clientParameters,
        uris,
        recoveryBackOffDelayPolicy,
        topologyBackOffDelayPolicy,
        addressResolver,
        maxProducersByConnection,
        maxTrackingConsumersByConnection,
        maxConsumersByConnection,
        tls,
        netty.byteBufAllocator,
        lazyInit,
        connectionNamingStrategy,
        this.clientFactory,
        this.observationCollector,
        this.forceReplicaForConsumers,
        this.forceLeaderForProducers,
        this.producerNodeRetryDelay,
        this.consumerNodeRetryDelay,
        this.locatorConnectionCount);
  }

  static final class DefaultTlsConfiguration implements TlsConfiguration {

    private final EnvironmentBuilder environmentBuilder;

    private boolean enabled = false;
    private boolean hostnameVerification = true;
    private SslContext sslContext;

    private DefaultTlsConfiguration(EnvironmentBuilder environmentBuilder) {
      this.environmentBuilder = environmentBuilder;
    }

    @Override
    public TlsConfiguration hostnameVerification() {
      this.hostnameVerification = true;
      return this;
    }

    @Override
    public TlsConfiguration hostnameVerification(boolean hostnameVerification) {
      this.hostnameVerification = hostnameVerification;
      return this;
    }

    @Override
    public TlsConfiguration sslContext(SslContext sslContext) {
      this.sslContext = sslContext;
      return this;
    }

    @Override
    public TlsConfiguration trustEverything() {
      LOGGER.warn(
          "SECURITY ALERT: this feature trusts every server certificate, effectively disabling peer verification. "
              + "This is convenient for local development but offers no protection against man-in-the-middle attacks. "
              + "Please see https://www.rabbitmq.com/docs/ssl to learn more about peer certificate verification.");
      try {
        this.sslContext(
            SslContextBuilder.forClient()
                .trustManager(Utils.TRUST_EVERYTHING_TRUST_MANAGER)
                .build());
      } catch (SSLException e) {
        throw new StreamException("Error while creating Netty SSL context", e);
      }
      return this;
    }

    @Override
    public EnvironmentBuilder environmentBuilder() {
      return this.environmentBuilder;
    }

    void enable() {
      this.enabled = true;
    }

    public boolean enabled() {
      return enabled;
    }

    public boolean hostnameVerificationEnabled() {
      return hostnameVerification;
    }

    public SslContext sslContext() {
      return sslContext;
    }
  }

  static class DefaultNettyConfiguration implements NettyConfiguration {

    private final EnvironmentBuilder environmentBuilder;
    private EventLoopGroup eventLoopGroup;
    private ByteBufAllocator byteBufAllocator = ByteBufAllocator.DEFAULT;
    private Consumer<Channel> channelCustomizer = noOpConsumer();
    private Consumer<Bootstrap> bootstrapCustomizer = noOpConsumer();

    private DefaultNettyConfiguration(EnvironmentBuilder environmentBuilder) {
      this.environmentBuilder = environmentBuilder;
    }

    @Override
    public NettyConfiguration eventLoopGroup(EventLoopGroup eventLoopGroup) {
      this.eventLoopGroup = eventLoopGroup;
      return this;
    }

    @Override
    public NettyConfiguration byteBufAllocator(ByteBufAllocator byteBufAllocator) {
      this.byteBufAllocator = byteBufAllocator;
      return this;
    }

    @Override
    public NettyConfiguration channelCustomizer(Consumer<Channel> channelCustomizer) {
      this.channelCustomizer = channelCustomizer;
      return this;
    }

    @Override
    public NettyConfiguration bootstrapCustomizer(Consumer<Bootstrap> bootstrapCustomizer) {
      this.bootstrapCustomizer = bootstrapCustomizer;
      return this;
    }

    @Override
    public EnvironmentBuilder environmentBuilder() {
      return this.environmentBuilder;
    }
  }
}
