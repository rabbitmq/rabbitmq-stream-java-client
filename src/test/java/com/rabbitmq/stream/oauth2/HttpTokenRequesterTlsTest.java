// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.oauth2;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyStore;
import java.time.Duration;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

public class HttpTokenRequesterTlsTest {

  private static final String TLS_AES_256 = "TLS_AES_256_GCM_SHA384";
  private static final String TLS_AES_128 = "TLS_AES_128_GCM_SHA256";
  private static final String PQC_GROUP = "X25519MLKEM768";
  private static final String CLASSICAL_GROUP = "x25519";

  HttpServer server;
  int port;
  String contextPath = "/uaa/oauth/token";
  HostnameVerifier previousDefaultHostnameVerifier;

  @BeforeEach
  void init() throws IOException {
    this.port = OAuth2TestUtils.randomNetworkPort();
    this.previousDefaultHostnameVerifier = HttpsURLConnection.getDefaultHostnameVerifier();
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop(0);
    }
    HttpsURLConnection.setDefaultHostnameVerifier(this.previousDefaultHostnameVerifier);
  }

  private SSLContext clientSslContext(KeyStore keyStore) throws Exception {
    SSLContext sslContext = SSLContext.getInstance("TLS");
    TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
    tmf.init(keyStore);
    sslContext.init(null, tmf.getTrustManagers(), null);
    return sslContext;
  }

  private HttpTokenRequester.Builder validBuilder(String uri) {
    return HttpTokenRequester.builder()
        .tokenEndpointUri(uri)
        .clientId("rabbit_client")
        .clientSecret("rabbit_secret")
        .grantType("client_credentials")
        .parser(new GsonTokenParser());
  }

  private String httpsUri() {
    return String.format("https://localhost:%d%s", port, contextPath);
  }

  private static void respondWithToken(
      com.sun.net.httpserver.HttpExchange exchange, String accessToken) throws IOException {
    byte[] data =
        OAuth2TestUtils.sampleJsonToken(accessToken, Duration.ofSeconds(60)).getBytes(UTF_8);
    exchange.getResponseHeaders().set("content-type", "application/json");
    exchange.sendResponseHeaders(200, data.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(data);
    }
  }

  @Test
  void buildRejectsNamedGroupsWithoutSslContext() {
    assertThatThrownBy(
            () ->
                validBuilder("http://localhost:" + port + contextPath)
                    .namedGroups(PQC_GROUP)
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsCipherSuitesWithoutSslContext() {
    assertThatThrownBy(
            () ->
                validBuilder("http://localhost:" + port + contextPath).ciphers(TLS_AES_256).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsHttpsEndpointUriWithoutSslContext() {
    assertThatThrownBy(() -> validBuilder(httpsUri()).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void matchingCipherSuitesSucceed() throws Exception {
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            keyStore,
            new String[] {TLS_AES_256},
            null,
            exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri())
            .sslContext(clientSslContext(keyStore))
            .ciphers(TLS_AES_256)
            .build();
    assertThat(requester.request()).isNotNull();
  }

  @Test
  void nonOverlappingCipherSuitesFailHandshake() throws Exception {
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            keyStore,
            new String[] {TLS_AES_128},
            null,
            exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri())
            .sslContext(clientSslContext(keyStore))
            .ciphers(TLS_AES_256)
            .build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void wrongCommonNameCertificateIsRejected() throws Exception {
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair("not-localhost");
    server =
        OAuth2TestUtils.startServer(
            port, contextPath, keyStore, exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri()).sslContext(clientSslContext(keyStore)).build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void wrongCommonNameCertificateIsRejectedEvenWithPermissiveGlobalHostnameVerifier()
      throws Exception {
    HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair("not-localhost");
    server =
        OAuth2TestUtils.startServer(
            port, contextPath, keyStore, exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri()).sslContext(clientSslContext(keyStore)).build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  @EnabledForJreRange(minVersion = 27)
  void matchingNamedGroupsSucceed() throws Exception {
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            keyStore,
            new String[] {TLS_AES_256},
            new String[] {PQC_GROUP},
            exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri())
            .sslContext(clientSslContext(keyStore))
            .ciphers(TLS_AES_256)
            .namedGroups(PQC_GROUP)
            .build();
    assertThat(requester.request()).isNotNull();
  }

  @Test
  @EnabledForJreRange(minVersion = 27)
  void nonOverlappingNamedGroupsFailHandshake() throws Exception {
    KeyStore keyStore = OAuth2TestUtils.generateKeyPair();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            keyStore,
            new String[] {TLS_AES_256},
            new String[] {CLASSICAL_GROUP},
            exchange -> respondWithToken(exchange, "token"));
    TokenRequester requester =
        validBuilder(httpsUri())
            .sslContext(clientSslContext(keyStore))
            .ciphers(TLS_AES_256)
            .namedGroups(PQC_GROUP)
            .build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }
}
