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

import com.rabbitmq.stream.AddressResolver;
import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.ChannelCustomizer;
import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.metrics.MetricsCollector;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.SaslConfiguration;
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
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamEnvironmentBuilder implements EnvironmentBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironmentBuilder.class);

  private final Client.ClientParameters clientParameters = new Client.ClientParameters();
  private final DefaultTlsConfiguration tls = new DefaultTlsConfiguration(this);
  private ScheduledExecutorService scheduledExecutorService;
  private List<URI> uris = Collections.emptyList();
  private BackOffDelayPolicy recoveryBackOffDelayPolicy =
      BackOffDelayPolicy.fixed(Duration.ofSeconds(5));
  private BackOffDelayPolicy topologyBackOffDelayPolicy =
      BackOffDelayPolicy.fixedWithInitialDelay(Duration.ofSeconds(5), Duration.ofSeconds(1));
  private AddressResolver addressResolver = address -> address;
  private int maxProducersByConnection = ProducersCoordinator.MAX_PRODUCERS_PER_CLIENT;
  private int maxTrackingConsumersByConnection =
      ProducersCoordinator.MAX_TRACKING_CONSUMERS_PER_CLIENT;
  private int maxConsumersByConnection = ConsumersCoordinator.MAX_SUBSCRIPTIONS_PER_CLIENT;

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
  public EnvironmentBuilder maxTrackingConsumersByConnection(
      int maxTrackingConsumersByConnection) {
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
  public TlsConfiguration tls() {
    this.tls.enable();
    return this.tls;
  }

  @Override
  public Environment build() {
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
        tls);
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
              + "Please see https://www.rabbitmq.com/ssl.html to learn more about peer certificate verification.");
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
}
