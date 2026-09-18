// Copyright (c) 2024-2026 Broadcom. All Rights Reserved.
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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

public class HttpTokenRequesterTest {

  HttpServer server;
  int port;
  String contextPath = "/uaa/oauth/token";

  @BeforeEach
  void init() throws IOException {
    this.port = OAuth2TestUtils.randomNetworkPort();
  }

  private HttpTokenRequester.Builder validBuilder() {
    return HttpTokenRequester.builder()
        .tokenEndpointUri(String.format("http://localhost:%d%s", port, contextPath))
        .clientId("rabbit_client")
        .clientSecret("rabbit_secret")
        .grantType("client_credentials")
        .parser(new GsonTokenParser());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void requestToken(boolean tls) throws Exception {
    String protocol;
    KeyStore keyStore;
    Consumer<HttpURLConnection> connectionConfigurator;
    if (tls) {
      protocol = "https";
      keyStore = OAuth2TestUtils.generateKeyPair();
      SSLContext sslContext = SSLContext.getInstance("TLS");
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(keyStore);
      sslContext.init(null, tmf.getTrustManagers(), null);
      connectionConfigurator =
          c -> {
            if (c instanceof HttpsURLConnection) {
              ((HttpsURLConnection) c).setSSLSocketFactory(sslContext.getSocketFactory());
            }
          };
    } else {
      protocol = "http";
      keyStore = null;
      connectionConfigurator = b -> {};
    }
    String uri = String.format("%s://localhost:%d%s", protocol, port, contextPath);
    AtomicReference<String> httpMethod = new AtomicReference<>();
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    AtomicReference<String> accept = new AtomicReference<>();
    AtomicReference<Map<String, String>> httpParameters = new AtomicReference<>();

    String accessToken = UUID.randomUUID().toString();

    Duration expiresIn = Duration.ofSeconds(60);
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            keyStore,
            exchange -> {
              Headers headers = exchange.getRequestHeaders();
              httpMethod.set(exchange.getRequestMethod());
              contentType.set(headers.getFirst("content-type"));
              authorization.set(headers.getFirst("authorization"));
              accept.set(headers.getFirst("accept"));

              String requestBody = new String(exchange.getRequestBody().readAllBytes(), UTF_8);
              Map<String, String> parameters =
                  Arrays.stream(requestBody.split("&"))
                      .map(p -> p.split("="))
                      .collect(Collectors.toMap(p -> p[0], p -> p[1]));
              httpParameters.set(parameters);

              byte[] data = OAuth2TestUtils.sampleJsonToken(accessToken, expiresIn).getBytes(UTF_8);

              Headers responseHeaders = exchange.getResponseHeaders();
              responseHeaders.set("content-type", "application/json");
              exchange.sendResponseHeaders(200, data.length);
              OutputStream responseBody = exchange.getResponseBody();
              responseBody.write(data);
              responseBody.close();
            });

    TokenRequester requester =
        HttpTokenRequester.builder()
            .tokenEndpointUri(uri)
            .clientId("rabbit_client")
            .clientSecret("rabbit_secret")
            .grantType("password")
            .parameters(Map.of("username", "rabbit_username", "password", "rabbit_password"))
            .connectionConfigurator(connectionConfigurator)
            .parser(StringToken::new)
            .build();

    String token = requester.request().value();
    assertThat(token).contains(accessToken);
    Gson gson = new Gson();
    TypeToken<Map<String, Object>> mapType = new TypeToken<>() {};
    Map<String, Object> tokenMap = gson.fromJson(token, mapType);
    assertThat(tokenMap)
        .containsEntry("access_token", accessToken)
        .containsEntry("expires_in", (double) expiresIn.toSeconds());

    assertThat(httpMethod).hasValue("POST");
    assertThat(contentType).hasValue("application/x-www-form-urlencoded;charset=UTF-8");
    assertThat(authorization).hasValue("Basic cmFiYml0X2NsaWVudDpyYWJiaXRfc2VjcmV0");
    assertThat(accept).hasValue("application/json");
    Map<String, String> parameters = httpParameters.get();
    assertThat(parameters)
        .isNotNull()
        .hasSize(3)
        .containsEntry("grant_type", "password")
        .containsEntry("username", "rabbit_username")
        .containsEntry("password", "rabbit_password");
  }

  @Test
  void buildRejectsNullTokenEndpointUri() {
    assertThatThrownBy(() -> validBuilder().tokenEndpointUri(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsSyntacticallyInvalidTokenEndpointUri() {
    assertThatThrownBy(() -> validBuilder().tokenEndpointUri("http://a b").build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsRelativeTokenEndpointUri() {
    assertThatThrownBy(() -> validBuilder().tokenEndpointUri("/uaa/oauth/token").build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsTokenEndpointUriWithNoHost() {
    assertThatThrownBy(() -> validBuilder().tokenEndpointUri("file:///etc/passwd").build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ftp://localhost/token", "file:///etc/passwd"})
  void buildRejectsUnsupportedScheme(String uri) {
    assertThatThrownBy(() -> validBuilder().tokenEndpointUri(uri).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsTokenEndpointUriWithUserInfo() {
    assertThatThrownBy(
            () ->
                validBuilder()
                    .tokenEndpointUri(
                        String.format("http://user:pwd@localhost:%d%s", port, contextPath))
                    .build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void buildRejectsBlankGrantType(String grantType) {
    assertThatThrownBy(() -> validBuilder().grantType(grantType).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsNullGrantType() {
    assertThatThrownBy(() -> validBuilder().grantType(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsNullParser() {
    assertThatThrownBy(() -> validBuilder().parser(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsNullParameters() {
    assertThatThrownBy(() -> validBuilder().parameters(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsGrantTypeParameterEntry() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("grant_type", "something");
    assertThatThrownBy(() -> validBuilder().parameters(parameters).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsNullClientId() {
    assertThatThrownBy(() -> validBuilder().clientId(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void buildRejectsNullClientSecret() {
    assertThatThrownBy(() -> validBuilder().clientSecret(null).build())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void connectionConfiguratorSeesNonZeroReadTimeout() throws Exception {
    server =
        OAuth2TestUtils.startServer(
            port, contextPath, null, exchange -> respondWithToken(exchange, "token"));
    AtomicReference<Integer> readTimeout = new AtomicReference<>();
    TokenRequester requester =
        validBuilder().connectionConfigurator(c -> readTimeout.set(c.getReadTimeout())).build();
    requester.request();
    assertThat(readTimeout.get()).isPositive();
  }

  @Test
  void connectionConfiguratorCanOverrideDefaultAuthorizationHeader() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              authorization.set(exchange.getRequestHeaders().getFirst("authorization"));
              respondWithToken(exchange, "token");
            });
    TokenRequester requester =
        validBuilder()
            .connectionConfigurator(c -> c.setRequestProperty("Authorization", "Bearer custom"))
            .build();
    requester.request();
    assertThat(authorization).hasValue("Bearer custom");
  }

  @Test
  void redirectIsNotFollowed() throws Exception {
    int secondPort = OAuth2TestUtils.randomNetworkPort();
    AtomicReference<Boolean> secondServerHit = new AtomicReference<>(false);
    HttpServer secondServer =
        OAuth2TestUtils.startServer(
            secondPort,
            contextPath,
            exchange -> {
              secondServerHit.set(true);
              respondWithToken(exchange, "token");
            });
    try {
      server =
          OAuth2TestUtils.startServer(
              port,
              contextPath,
              null,
              exchange -> {
                exchange
                    .getResponseHeaders()
                    .set(
                        "location",
                        String.format("http://localhost:%d%s", secondPort, contextPath));
                exchange.sendResponseHeaders(302, -1);
                exchange.close();
              });
      TokenRequester requester = validBuilder().build();
      assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
      assertThat(secondServerHit.get()).isFalse();
    } finally {
      secondServer.stop(0);
    }
  }

  @Test
  void oversizedResponseBodyWithCorrectContentLengthFails() throws Exception {
    byte[] oversized = new byte[2 * 1024 * 1024];
    Arrays.fill(oversized, (byte) 'a');
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              exchange.getResponseHeaders().set("content-type", "application/json");
              exchange.sendResponseHeaders(200, oversized.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(oversized);
              }
            });
    TokenRequester requester = validBuilder().build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void oversizedResponseBodyWithLyingContentLengthFails() throws Exception {
    byte[] oversized = new byte[2 * 1024 * 1024];
    Arrays.fill(oversized, (byte) 'a');
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              exchange.getResponseHeaders().set("content-type", "application/json");
              exchange.sendResponseHeaders(200, 10);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(oversized);
              }
            });
    TokenRequester requester = validBuilder().build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @ParameterizedTest
  @CsvSource({
    "application/json,true",
    "application/json;charset=UTF-8,true",
    "application/hal+json,true",
    "text/html,false",
    "application/x-json-hijack,false",
  })
  void contentTypeAcceptance(String contentType, boolean accepted) throws Exception {
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              byte[] data =
                  OAuth2TestUtils.sampleJsonToken("token", Duration.ofSeconds(60)).getBytes(UTF_8);
              exchange.getResponseHeaders().set("content-type", contentType);
              exchange.sendResponseHeaders(200, data.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
              }
            });
    TokenRequester requester = validBuilder().build();
    if (accepted) {
      assertThat(requester.request()).isNotNull();
    } else {
      assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
    }
  }

  @Test
  void absentContentTypeIsRejected() throws Exception {
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              byte[] data =
                  OAuth2TestUtils.sampleJsonToken("token", Duration.ofSeconds(60)).getBytes(UTF_8);
              exchange.sendResponseHeaders(200, data.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
              }
            });
    TokenRequester requester = validBuilder().build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void blankBodyFails(String body) throws Exception {
    byte[] data = body.getBytes(UTF_8);
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              exchange.getResponseHeaders().set("content-type", "application/json");
              exchange.sendResponseHeaders(200, data.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
              }
            });
    TokenRequester requester = validBuilder().build();
    assertThatThrownBy(requester::request).isInstanceOf(OAuth2Exception.class);
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 500})
  void nonOkStatusThrowsAndConnectionIsNotLeaked(int status) throws Exception {
    AtomicReference<Integer> requestCount = new AtomicReference<>(0);
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              int count = requestCount.getAndUpdate(c -> c + 1);
              if (count == 0) {
                byte[] error = "{\"error\":\"invalid_client\"}".getBytes(UTF_8);
                exchange.getResponseHeaders().set("content-type", "application/json");
                exchange.sendResponseHeaders(status, error.length);
                try (OutputStream os = exchange.getResponseBody()) {
                  os.write(error);
                }
              } else {
                respondWithToken(exchange, "token");
              }
            });
    TokenRequester requester = validBuilder().build();
    assertThatThrownBy(requester::request)
        .isInstanceOf(OAuth2Exception.class)
        .hasMessageContaining(String.valueOf(status));
    assertThat(requester.request()).isNotNull();
  }

  @Test
  void authorizationHeaderEncodesSpecialCharacters() throws Exception {
    AtomicReference<String> authorization = new AtomicReference<>();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              authorization.set(exchange.getRequestHeaders().getFirst("authorization"));
              respondWithToken(exchange, "token");
            });
    String clientId = "id:with:colons and space";
    String clientSecret = "sécret";
    TokenRequester requester = validBuilder().clientId(clientId).clientSecret(clientSecret).build();
    requester.request();
    String encodedCredential =
        java.net.URLEncoder.encode(clientId, UTF_8)
            + ":"
            + java.net.URLEncoder.encode(clientSecret, UTF_8);
    String expected =
        "Basic " + java.util.Base64.getEncoder().encodeToString(encodedCredential.getBytes(UTF_8));
    assertThat(authorization).hasValue(expected);
  }

  @Test
  void contentTypeHeaderCarriesCharsetAndNoCharsetHeaderIsSent() throws Exception {
    AtomicReference<String> contentType = new AtomicReference<>();
    AtomicReference<String> charsetHeader = new AtomicReference<>();
    server =
        OAuth2TestUtils.startServer(
            port,
            contextPath,
            null,
            exchange -> {
              contentType.set(exchange.getRequestHeaders().getFirst("content-type"));
              charsetHeader.set(exchange.getRequestHeaders().getFirst("charset"));
              respondWithToken(exchange, "token");
            });
    TokenRequester requester = validBuilder().build();
    requester.request();
    assertThat(contentType).hasValue("application/x-www-form-urlencoded;charset=UTF-8");
    assertThat(charsetHeader.get()).isNull();
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

  @AfterEach
  public void tearDown() {
    if (server != null) {
      server.stop(0);
    }
  }

  private static class StringToken implements Token {

    private final String value;

    private StringToken(String value) {
      this.value = value;
    }

    @Override
    public String value() {
      return this.value;
    }

    @Override
    public Instant expirationTime() {
      return Instant.EPOCH;
    }
  }
}
