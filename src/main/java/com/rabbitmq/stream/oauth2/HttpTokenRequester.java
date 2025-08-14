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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Token requester using HTTP(S) to request an OAuth2 Access token.
 *
 * <p>Uses {@link HttpClient} for the HTTP operations.
 */
public final class HttpTokenRequester implements TokenRequester {

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

  private final URI tokenEndpointUri;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;

  private final Map<String, String> parameters;

  private final HttpClient client;
  private final Consumer<HttpRequest.Builder> requestBuilderConsumer;

  private final TokenParser parser;

  public HttpTokenRequester(
      String tokenEndpointUri,
      String clientId,
      String clientSecret,
      String grantType,
      Map<String, String> parameters,
      Consumer<HttpClient.Builder> clientBuilderConsumer,
      Consumer<HttpRequest.Builder> requestBuilderConsumer,
      TokenParser parser) {
    try {
      this.tokenEndpointUri = new URI(tokenEndpointUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Error in URI: " + tokenEndpointUri);
    }
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.grantType = grantType;
    this.parameters = Map.copyOf(parameters);
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
    // TODO handle HTTPS configuration
  }

  @Override
  public Token request() {
    StringBuilder urlParameters = new StringBuilder();
    encode(urlParameters, "grant_type", grantType);
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      encode(urlParameters, parameter.getKey(), parameter.getValue());
    }
    byte[] postData = urlParameters.toString().getBytes(UTF_8);

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

  private static String authorization(String username, String password) {
    return "Basic " + base64(username + ":" + password);
  }

  private static String base64(String in) {
    return Base64.getEncoder().encodeToString(in.getBytes(UTF_8));
  }

  private static void encode(StringBuilder builder, String name, String value) {
    if (value != null) {
      if (builder.length() > 0) {
        builder.append("&");
      }
      builder.append(encode(name)).append("=").append(encode(value));
    }
  }

  private static String encode(String value) {
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
          "HTTP request for token retrieval did not " + "return 200 status code: " + statusCode);
    }
  }
}
