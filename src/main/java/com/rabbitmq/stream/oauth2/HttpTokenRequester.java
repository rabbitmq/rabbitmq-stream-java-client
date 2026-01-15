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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Token requester using HTTP(S) to request an OAuth2 Access token.
 *
 * <p>Uses {@link HttpURLConnection} for the HTTP operations.
 */
public final class HttpTokenRequester implements TokenRequester {

  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);

  private final URI tokenEndpointUri;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;

  private final Map<String, String> parameters;

  private final Consumer<HttpURLConnection> connectionConfigurator;
  private final Consumer<HttpURLConnection> requestConfigurator;

  private final TokenParser parser;

  public HttpTokenRequester(
      String tokenEndpointUri,
      String clientId,
      String clientSecret,
      String grantType,
      Map<String, String> parameters,
      Consumer<HttpURLConnection> connectionConfigurator,
      Consumer<HttpURLConnection> requestConfigurator,
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
    this.connectionConfigurator = connectionConfigurator;
    if (requestConfigurator == null) {
      this.requestConfigurator =
          connection -> {
            connection.setReadTimeout((int) REQUEST_TIMEOUT.toMillis());
            connection.setRequestProperty(
                "Authorization", authorization(this.clientId, this.clientSecret));
          };
    } else {
      this.requestConfigurator = requestConfigurator;
    }
  }

  @Override
  public Token request() {
    StringBuilder urlParameters = new StringBuilder();
    encode(urlParameters, "grant_type", grantType);
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      encode(urlParameters, parameter.getKey(), parameter.getValue());
    }
    byte[] postData = urlParameters.toString().getBytes(UTF_8);

    try {
      URL url = this.tokenEndpointUri.toURL();
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setConnectTimeout((int) CONNECT_TIMEOUT.toMillis());
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
      connection.setRequestProperty("Charset", UTF_8.name());
      connection.setRequestProperty("Accept", "application/json");
      connection.setDoOutput(true);

      if (this.connectionConfigurator != null) {
        this.connectionConfigurator.accept(connection);
      }
      this.requestConfigurator.accept(connection);

      try (OutputStream os = connection.getOutputStream()) {
        os.write(postData);
      }

      int responseCode = connection.getResponseCode();
      checkStatusCode(responseCode);
      checkContentType(connection.getContentType());

      try (InputStream is = connection.getInputStream();
          BufferedReader reader = new BufferedReader(new InputStreamReader(is, UTF_8))) {
        String responseBody = reader.lines().collect(Collectors.joining("\n"));
        return this.parser.parse(responseBody);
      }

    } catch (IOException e) {
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
