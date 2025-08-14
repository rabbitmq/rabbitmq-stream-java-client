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

import static com.rabbitmq.stream.impl.Assertions.*;
import static com.rabbitmq.stream.impl.Client.cp;
import static com.rabbitmq.stream.impl.HttpTestUtils.generateKeyPair;
import static com.rabbitmq.stream.impl.HttpTestUtils.oAuth2TokenHttpHandler;
import static com.rabbitmq.stream.impl.TestUtils.sync;
import static java.lang.System.currentTimeMillis;
import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.impl.TestUtils.ClientFactory;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfOauth2AuthBackendNotEnabled;
import com.rabbitmq.stream.impl.TestUtils.Sync;
import com.rabbitmq.stream.oauth2.GsonTokenParser;
import com.rabbitmq.stream.oauth2.HttpTokenRequester;
import com.rabbitmq.stream.oauth2.TokenCredentialsManager;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@StreamTestInfrastructure
@DisabledIfOauth2AuthBackendNotEnabled
public class OAuth2ClientTest {

  ClientFactory cf;
  HttpServer server;
  String contextPath = "/uaa/oauth/token";
  int port;
  ScheduledExecutorService scheduledExecutorService;

  @BeforeEach
  void init() throws Exception {
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    this.port = TestUtils.randomNetworkPort();
  }

  @AfterEach
  void tearDown() {
    this.scheduledExecutorService.shutdown();
    if (this.server != null) {
      server.stop(0);
    }
  }

  @Test
  void openingConnectionWithValidTokenShouldSucceed() {
    this.server =
        start(oAuth2TokenHttpHandler(() -> currentTimeMillis() + ofMinutes(60).toMillis()));

    TokenCredentialsManager tokenCredentialsManager = createTokenCredentialsManager();
    try (Client ignored = cf.get(cp().credentialsManager(tokenCredentialsManager))) {}
  }

  @Test
  void openingConnectionWithExpiredTokenShouldFail() {
    this.server =
        start(oAuth2TokenHttpHandler(() -> currentTimeMillis() - ofMinutes(60).toMillis()));

    TokenCredentialsManager tokenCredentialsManager = createTokenCredentialsManager();

    assertThatThrownBy(() -> cf.get(cp().credentialsManager(tokenCredentialsManager)))
        .isInstanceOf(AuthenticationFailureException.class);
  }

  @Test
  void connectionShouldBeClosedWhenRefreshedTokenExpires() {
    Duration tokenDuration = Duration.ofSeconds(2);
    long expiry = currentTimeMillis() + tokenDuration.toMillis();
    String token = JwtTestUtils.token(expiry);

    Sync sync = sync();
    cf.get(cp().username("").password(token).shutdownListener(shutdownContext -> sync.down()));
    assertThat(sync).completes(tokenDuration.multipliedBy(4));
  }

  @Test
  void tokenWithHttpShouldBeRefreshedWhenItExpires() throws Exception {
    this.tokenShouldBeRefreshedWhenItExpires(null);
  }

  @Test
  void tokenWithHttpsShouldBeRefreshedWhenItExpires() throws Exception {
    this.tokenShouldBeRefreshedWhenItExpires(generateKeyPair());
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

    TokenCredentialsManager tokenCredentialsManager = createTokenCredentialsManager(sslContext);

    try (Client ignored = cf.get(cp().credentialsManager(tokenCredentialsManager))) {
      assertThat(tokenRefreshedSync).completes(tokenLifetime.multipliedBy(tokenRefreshCount + 1));
    }
  }

  private HttpServer start(HttpHandler handler) {
    return start(handler, null);
  }

  private HttpServer start(HttpHandler handler, KeyStore ks) {
    return HttpTestUtils.startServer(port, contextPath, ks, handler);
  }

  private TokenCredentialsManager createTokenCredentialsManager() {
    return this.createTokenCredentialsManager(null);
  }

  private TokenCredentialsManager createTokenCredentialsManager(SSLContext sslContext) {
    String uri =
        String.format(
            "%s://localhost:%d%s",
            sslContext == null ? "http" : "https", this.port, this.contextPath);
    HttpTokenRequester tokenRequester =
        new HttpTokenRequester(
            uri,
            "rabbitmq",
            "rabbitmq",
            "client_credentials",
            Collections.emptyMap(),
            c -> {
              if (sslContext != null) {
                c.sslContext(sslContext);
              }
            },
            null,
            new GsonTokenParser());
    // the broker works at the second level for expiration
    // we have to make sure to renew fast enough for short-lived tokens
    Function<Instant, Duration> refreshDelayStrategy =
        TokenCredentialsManager.ratioRefreshDelayStrategy(0.4f);
    return new TokenCredentialsManager(
        tokenRequester, this.scheduledExecutorService, refreshDelayStrategy);
  }
}
