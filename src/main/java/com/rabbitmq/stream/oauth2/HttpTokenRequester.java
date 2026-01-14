// Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

/**
 * Token requester using HTTP(S) to request an OAuth2 Access token.
 *
 * <p>This class supports two modes of operation:
 *
 * <ul>
 *   <li>Using {@link HttpClient} (Java 11+) for advanced customization of HTTP requests
 *   <li>Using {@link HttpURLConnection} for compatibility with environments where {@link
 *       HttpClient} is not available (e.g., Android)
 * </ul>
 *
 * <p>Use the constructor with {@link SSLContext} parameter for the {@link HttpURLConnection}-based
 * implementation, which is compatible with both the regular JDK and Android.
 *
 * @see #HttpTokenRequester(String, String, String, String, Map, SSLContext, TokenParser)
 */
public final class HttpTokenRequester implements TokenRequester {

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
  private static final int REQUEST_TIMEOUT_MS = (int) REQUEST_TIMEOUT.toMillis();
  private static final int CONNECT_TIMEOUT_MS = (int) CONNECT_TIMEOUT.toMillis();

  private final URI tokenEndpointUri;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;

  private final Map<String, String> parameters;

  // HttpClient-based implementation (null when using HttpURLConnection)
  private final HttpClient client;
  private final Consumer<HttpRequest.Builder> requestBuilderConsumer;

  // HttpURLConnection-based implementation (null when using HttpClient)
  private final SSLContext sslContext;

  private final TokenParser parser;

  /**
   * Creates a token requester using {@link HttpURLConnection}.
   *
   * <p>This constructor creates a token requester that is compatible with environments where {@link
   * HttpClient} is not available, such as Android.
   *
   * @param tokenEndpointUri the URI of the token endpoint
   * @param clientId the OAuth2 client ID
   * @param clientSecret the OAuth2 client secret
   * @param grantType the OAuth2 grant type (e.g., "client_credentials")
   * @param parameters additional parameters to include in the token request
   * @param sslContext the SSL context for HTTPS connections, or null for HTTP
   * @param parser the parser to extract the token from the response
   * @since 1.5.0
   */
  public HttpTokenRequester(
      String tokenEndpointUri,
      String clientId,
      String clientSecret,
      String grantType,
      Map<String, String> parameters,
      SSLContext sslContext,
      TokenParser parser) {
    if (tokenEndpointUri == null) {
      throw new IllegalArgumentException("tokenEndpointUri must not be null");
    }
    if (parser == null) {
      throw new IllegalArgumentException("parser must not be null");
    }
    try {
      this.tokenEndpointUri = new URI(tokenEndpointUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + tokenEndpointUri, e);
    }
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.grantType = grantType;
    this.parameters = parameters == null ? Collections.emptyMap() : Map.copyOf(parameters);
    this.parser = parser;
    this.sslContext = sslContext;
    this.client = null;
    this.requestBuilderConsumer = null;
  }

  /**
   * Creates a token requester using {@link HttpClient}.
   *
   * <p>This constructor allows advanced customization of the HTTP client and requests. Note that
   * {@link HttpClient} is only available in Java 11+ and is not available on Android.
   *
   * @param tokenEndpointUri the URI of the token endpoint
   * @param clientId the OAuth2 client ID
   * @param clientSecret the OAuth2 client secret
   * @param grantType the OAuth2 grant type (e.g., "client_credentials")
   * @param parameters additional parameters to include in the token request
   * @param clientBuilderConsumer consumer to customize the {@link HttpClient.Builder}
   * @param requestBuilderConsumer consumer to customize the {@link HttpRequest.Builder}
   * @param parser the parser to extract the token from the response
   */
  public HttpTokenRequester(
      String tokenEndpointUri,
      String clientId,
      String clientSecret,
      String grantType,
      Map<String, String> parameters,
      Consumer<HttpClient.Builder> clientBuilderConsumer,
      Consumer<HttpRequest.Builder> requestBuilderConsumer,
      TokenParser parser) {
    if (tokenEndpointUri == null) {
      throw new IllegalArgumentException("tokenEndpointUri must not be null");
    }
    if (parser == null) {
      throw new IllegalArgumentException("parser must not be null");
    }
    try {
      this.tokenEndpointUri = new URI(tokenEndpointUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid URI: " + tokenEndpointUri, e);
    }
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.grantType = grantType;
    this.parameters = parameters == null ? Collections.emptyMap() : Map.copyOf(parameters);
    this.parser = parser;
    if (requestBuilderConsumer == null) {
      this.requestBuilderConsumer =
          requestBuilder ->
              requestBuilder
                  .timeout(REQUEST_TIMEOUT)
                  .setHeader("authorization", authorization(this.clientId, this.clientSecret));
    } else {
      this.requestBuilderConsumer = requestBuilderConsumer;
    }

    HttpClient.Builder builder =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(CONNECT_TIMEOUT);
    if (clientBuilderConsumer != null) {
      clientBuilderConsumer.accept(builder);
    }
    this.client = builder.build();
    this.sslContext = null;
  }

  @Override
  public Token request() {
    if (this.client != null) {
      return requestWithHttpClient();
    } else {
      return requestWithUrlConnection();
    }
  }

  private Token requestWithHttpClient() {
    byte[] postData = buildPostData();
    HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(this.tokenEndpointUri);
    requestBuilder.header("content-type", "application/x-www-form-urlencoded");
    requestBuilder.header("charset", UTF_8.name());
    requestBuilder.header("accept", "application/json");

    requestBuilderConsumer.accept(requestBuilder);
    HttpRequest request =
        requestBuilder.POST(HttpRequest.BodyPublishers.ofByteArray(postData)).build();

    try {
      HttpResponse<String> response =
          this.client.send(request, HttpResponse.BodyHandlers.ofString(UTF_8));
      checkStatusCode(response.statusCode());
      checkContentType(response.headers().firstValue("content-type").orElse(null));
      return this.parser.parse(response.body());
    } catch (IOException e) {
      throw new OAuth2Exception("Error while retrieving OAuth 2 token", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new OAuth2Exception("Error while retrieving OAuth 2 token", e);
    }
  }

  private Token requestWithUrlConnection() {
    byte[] postData = buildPostData();
    HttpURLConnection connection = null;
    try {
      URL url = tokenEndpointUri.toURL();
      connection = (HttpURLConnection) url.openConnection();

      if (connection instanceof HttpsURLConnection && sslContext != null) {
        ((HttpsURLConnection) connection).setSSLSocketFactory(sslContext.getSocketFactory());
      }

      connection.setRequestMethod("POST");
      connection.setConnectTimeout(CONNECT_TIMEOUT_MS);
      connection.setReadTimeout(REQUEST_TIMEOUT_MS);
      connection.setDoOutput(true);
      connection.setInstanceFollowRedirects(true);

      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setRequestProperty("charset", UTF_8.name());
      connection.setRequestProperty("Accept", "application/json");
      connection.setRequestProperty("Authorization", authorization(clientId, clientSecret));
      connection.setRequestProperty("Content-Length", String.valueOf(postData.length));

      try (OutputStream os = connection.getOutputStream()) {
        os.write(postData);
      }

      int statusCode = connection.getResponseCode();
      checkStatusCode(statusCode);
      checkContentType(connection.getContentType());

      String responseBody;
      try (InputStream in = connection.getInputStream()) {
        responseBody = readResponseBody(in);
      }
      return this.parser.parse(responseBody);
    } catch (IOException e) {
      throw new OAuth2Exception("Error while retrieving OAuth 2 token", e);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private byte[] buildPostData() {
    StringBuilder params = new StringBuilder();
    appendParam(params, "grant_type", grantType);
    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      appendParam(params, entry.getKey(), entry.getValue());
    }
    return params.toString().getBytes(UTF_8);
  }

  private static String readResponseBody(InputStream in) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = in.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString(UTF_8);
  }

  private static String authorization(String username, String password) {
    return "Basic " + base64(username + ":" + password);
  }

  private static String base64(String in) {
    return Base64.getEncoder().encodeToString(in.getBytes(UTF_8));
  }

  private static void appendParam(StringBuilder builder, String name, String value) {
    if (value != null) {
      if (builder.length() > 0) {
        builder.append("&");
      }
      builder.append(urlEncode(name)).append("=").append(urlEncode(value));
    }
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, UTF_8);
  }

  private static void checkContentType(String contentType) {
    if (contentType == null || !contentType.toLowerCase().contains("json")) {
      throw new OAuth2Exception("HTTP request for token retrieval is not JSON: " + contentType);
    }
  }

  private static void checkStatusCode(int statusCode) {
    if (statusCode != 200) {
      throw new OAuth2Exception(
          "HTTP request for token retrieval did not return 200 status code: " + statusCode);
    }
  }
}
