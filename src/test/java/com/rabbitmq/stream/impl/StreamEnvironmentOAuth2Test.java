// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Constants.CODE_PRODUCER_NOT_AVAILABLE;
import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.HttpTestUtils.generateKeyPair;
import static com.rabbitmq.stream.impl.HttpTestUtils.oAuth2TokenHttpHandler;
import static com.rabbitmq.stream.impl.TestUtils.localhost;
import static com.rabbitmq.stream.impl.TestUtils.localhostTls;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofSeconds;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.impl.StreamEnvironmentBuilder.DefaultOAuth2Configuration;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfOauth2AuthBackendNotEnabled;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import com.rabbitmq.stream.oauth2.TokenCredentialsManager;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.EventLoopGroup;
import java.security.KeyStore;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@StreamTestInfrastructure
@DisabledIfOauth2AuthBackendNotEnabled
public class StreamEnvironmentOAuth2Test {

  EnvironmentBuilder environmentBuilder;

  String stream;
  TestUtils.ClientFactory cf;
  EventLoopGroup eventLoopGroup;

  HttpServer server;
  String contextPath = "/uaa/oauth/token";
  int port;

  @BeforeEach
  void init() throws Exception {
    environmentBuilder = Environment.builder();
    environmentBuilder.addressResolver(
        add -> add.port() == Client.DEFAULT_PORT ? localhost() : localhostTls());
    environmentBuilder.netty().eventLoopGroup(eventLoopGroup);
    this.port = TestUtils.randomNetworkPort();
  }

  @AfterEach
  void tearDown() {
    if (this.server != null) {
      server.stop(0);
    }
  }

  @Test
  void tokenWithHttpShouldBeRefreshedWhenItExpires() throws Exception {
    tokenShouldBeRefreshedWhenItExpires(null);
  }

  @Test
  void tokenWithHttpsShouldBeRefreshedWhenItExpires() throws Exception {
    tokenShouldBeRefreshedWhenItExpires(generateKeyPair());
  }

  @Test
  void environmentShouldNotWorkAfterTokenExpires() throws Exception {
    Duration tokenLifetime = ofSeconds(3);
    AtomicInteger serverCallCount = new AtomicInteger(0);
    Sync tokenRefreshedSync = sync(2);
    HttpHandler httpHandler =
        oAuth2TokenHttpHandler(
            () -> currentTimeMillis() + tokenLifetime.toMillis(), tokenRefreshedSync::down);
    this.server = start(httpHandler, null);

    try (Environment env =
        environmentBuilder
            .oauth2()
            .tokenEndpointUri(uri())
            .clientId("rabbitmq")
            .clientSecret("rabbitmq")
            .grantType("client_credentials")
            .environmentBuilder()
            .build()) {
      Producer producer = env.producerBuilder().stream(stream).build();
      Sync consumeSync = sync();
      StreamConsumer consumer =
          (StreamConsumer)
              env.consumerBuilder().stream(stream)
                  .messageHandler((ctx, msg) -> consumeSync.down())
                  .build();
      producer.send(producer.messageBuilder().build(), ctx -> {});
      assertThat(consumeSync).completes();
      assertThat(tokenRefreshedSync).completes();
      org.assertj.core.api.Assertions.assertThat(consumer.state() == Resource.State.OPEN);

      // stopping the token server, there won't be attempts to re-authenticate
      this.server.stop(0);

      waitAtMost(
          () -> {
            try {
              env.streamExists(stream);
              return false;
            } catch (Exception e) {
              return true;
            }
          });

      AtomicInteger lastResponseCode = new AtomicInteger(-1);
      waitAtMost(
          () -> {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean confirmed = new AtomicBoolean(false);
            producer.send(
                producer.messageBuilder().build(),
                ctx -> {
                  lastResponseCode.set(ctx.getCode());
                  confirmed.set(ctx.isConfirmed());
                  latch.countDown();
                });
            boolean receivedCallback = latch.await(10, TimeUnit.SECONDS);
            return receivedCallback && !confirmed.get();
          });
      org.assertj.core.api.Assertions.assertThat(lastResponseCode)
          .hasValue(CODE_PRODUCER_NOT_AVAILABLE);

      waitAtMost(() -> consumer.state() != Resource.State.OPEN);
    }
  }

  private void tokenShouldBeRefreshedWhenItExpires(KeyStore ks) throws Exception {
    int tokenRefreshCount = 3;
    Sync tokenRefreshedSync = sync(tokenRefreshCount);
    Duration tokenLifetime = ofSeconds(3);
    HttpHandler httpHandler =
        oAuth2TokenHttpHandler(
            () -> currentTimeMillis() + tokenLifetime.toMillis(), tokenRefreshedSync::down);

    this.server = start(httpHandler, ks);

    SSLContext sslContext = null;
    if (ks != null) {
      sslContext = SSLContext.getInstance("TLS");
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(ks);
      sslContext.init(null, tmf.getTrustManagers(), null);
    }

    DefaultOAuth2Configuration oauth = (DefaultOAuth2Configuration) environmentBuilder.oauth2();
    // the broker works at the second level for expiration
    // we have to make sure to renew fast enough for short-lived tokens
    oauth.refreshDelayStrategy(TokenCredentialsManager.ratioRefreshDelayStrategy(0.4f));
    try (Environment env =
        environmentBuilder
            .oauth2()
            .tokenEndpointUri(uri(sslContext))
            .clientId("rabbitmq")
            .clientSecret("rabbitmq")
            .grantType("client_credentials")
            .sslContext(sslContext)
            .environmentBuilder()
            .build()) {

      Producer producer = env.producerBuilder().stream(stream).build();

      Sync consumeSync = sync();
      env.consumerBuilder().stream(stream).messageHandler((ctx, msg) -> consumeSync.down()).build();
      assertThat(tokenRefreshedSync).completes(tokenLifetime.multipliedBy(tokenRefreshCount + 1));

      producer.send(producer.messageBuilder().build(), ctx -> {});
      assertThat(consumeSync).completes();
    }
  }

  private String uri() {
    return this.uri(null);
  }

  private String uri(SSLContext sslContext) {
    return String.format(
        "%s://localhost:%d%s", sslContext == null ? "http" : "https", this.port, this.contextPath);
  }

  private HttpServer start(HttpHandler handler, KeyStore ks) {
    return HttpTestUtils.startServer(port, contextPath, ks, handler);
  }
}
