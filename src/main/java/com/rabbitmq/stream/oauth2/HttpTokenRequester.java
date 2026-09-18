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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Token requester using HTTP(S) to request an OAuth 2 access token.
 *
 * <p>Uses {@link HttpURLConnection} for the HTTP operations.
 *
 * <p>This class is intended for internal use by the library. Its API, including this Javadoc, is
 * not part of the public API of the library and may change without notice.
 */
public final class HttpTokenRequester implements TokenRequester {

  private static final Set<String> SUPPORTED_SCHEMES = Set.of("http", "https");
  private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(30);
  private static final int MAX_RESPONSE_SIZE = 1024 * 1024;
  private static final int MAX_ERROR_BODY_LENGTH_IN_MESSAGE = 512;

  private final URL tokenEndpointUrl;
  private final String clientId;
  private final String clientSecret;
  private final String grantType;

  private final Map<String, String> parameters;
  private final byte[] postData;

  private final Consumer<HttpURLConnection> connectionConfigurator;

  private final TokenParser parser;

  private HttpTokenRequester(
      String tokenEndpointUri,
      String clientId,
      String clientSecret,
      String grantType,
      Map<String, String> parameters,
      Consumer<HttpURLConnection> connectionConfigurator,
      TokenParser parser) {
    URI uri = validateTokenEndpointUri(tokenEndpointUri);
    try {
      this.tokenEndpointUrl = uri.toURL();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Invalid token endpoint URI: " + e.getMessage(), e);
    }
    this.clientId = nonNull(clientId, "clientId");
    this.clientSecret = nonNull(clientSecret, "clientSecret");
    this.grantType = nonBlank(grantType, "grantType");
    this.parameters = validateParameters(parameters);
    this.parser = nonNull(parser, "parser");
    this.connectionConfigurator = connectionConfigurator;
    this.postData = encodeParameters(this.grantType, this.parameters);
  }

  public static Builder builder() {
    return new Builder();
  }

  private static URI validateTokenEndpointUri(String tokenEndpointUri) {
    nonNull(tokenEndpointUri, "tokenEndpointUri");
    URI uri;
    try {
      uri = new URI(tokenEndpointUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid token endpoint URI syntax ("
              + e.getReason()
              + " at index "
              + e.getIndex()
              + ")");
    }
    if (!uri.isAbsolute() || uri.getHost() == null) {
      throw new IllegalArgumentException("Token endpoint URI must be absolute and have a host");
    }
    String scheme = uri.getScheme().toLowerCase(Locale.ROOT);
    if (!SUPPORTED_SCHEMES.contains(scheme)) {
      throw new IllegalArgumentException(
          "Token endpoint URI scheme must be http or https, got: " + scheme);
    }
    if (uri.getUserInfo() != null) {
      throw new IllegalArgumentException(
          "Token endpoint URI must not carry user information, it is ignored by HttpURLConnection");
    }
    return uri;
  }

  private static Map<String, String> validateParameters(Map<String, String> parameters) {
    nonNull(parameters, "parameters");
    if (parameters.containsKey("grant_type")) {
      throw new IllegalArgumentException(
          "parameters must not contain a 'grant_type' entry, use the grantType argument instead");
    }
    return Map.copyOf(parameters);
  }

  private static byte[] encodeParameters(String grantType, Map<String, String> parameters) {
    StringBuilder urlParameters = new StringBuilder();
    encode(urlParameters, "grant_type", grantType);
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      encode(urlParameters, parameter.getKey(), parameter.getValue());
    }
    return urlParameters.toString().getBytes(UTF_8);
  }

  @Override
  public Token request() {
    try {
      HttpURLConnection connection = (HttpURLConnection) this.tokenEndpointUrl.openConnection();
      connection.setInstanceFollowRedirects(false);
      connection.setConnectTimeout((int) CONNECT_TIMEOUT.toMillis());
      connection.setReadTimeout((int) REQUEST_TIMEOUT.toMillis());
      connection.setRequestMethod("POST");
      connection.setRequestProperty(
          "Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
      connection.setRequestProperty("Accept", "application/json");
      connection.setRequestProperty("Accept-Encoding", "identity");
      connection.setUseCaches(false);
      connection.setRequestProperty("Cache-Control", "no-store");
      connection.setRequestProperty(
          "Authorization", authorization(this.clientId, this.clientSecret));
      connection.setDoOutput(true);
      connection.setFixedLengthStreamingMode(this.postData.length);

      if (this.connectionConfigurator != null) {
        this.connectionConfigurator.accept(connection);
      }

      try (OutputStream os = connection.getOutputStream()) {
        os.write(this.postData);
      }

      int responseCode = connection.getResponseCode();
      if (responseCode != 200) {
        String errorBody = readAndClose(connection.getErrorStream());
        throw new OAuth2Exception(
            errorMessage(responseCode, connection.getContentType(), errorBody));
      }
      String contentType = connection.getContentType();
      if (!isJson(contentType)) {
        readAndClose(connection.getInputStream());
        throw new OAuth2Exception("HTTP request for token retrieval is not JSON: " + contentType);
      }

      String responseBody;
      try (InputStream is = connection.getInputStream()) {
        responseBody = new String(read(is), UTF_8);
      }
      if (responseBody.isBlank()) {
        throw new OAuth2Exception("HTTP request for token retrieval returned an empty body");
      }
      return this.parser.parse(responseBody);
    } catch (IOException e) {
      throw new OAuth2Exception("Error while retrieving OAuth 2 token", e);
    }
  }

  private static String errorMessage(int responseCode, String contentType, String errorBody) {
    String message =
        "HTTP request for token retrieval did not return 200 status code: " + responseCode;
    if (errorBody != null && !errorBody.isBlank() && isJson(contentType)) {
      String snippet =
          errorBody.length() > MAX_ERROR_BODY_LENGTH_IN_MESSAGE
              ? errorBody.substring(0, MAX_ERROR_BODY_LENGTH_IN_MESSAGE)
              : errorBody;
      message = message + ", response body: " + snippet;
    }
    return message;
  }

  private static String readAndClose(InputStream is) throws IOException {
    if (is == null) {
      return null;
    }
    try (InputStream in = is) {
      return new String(read(in), UTF_8);
    }
  }

  private static byte[] read(InputStream is) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    int total = 0;
    while ((read = is.read(buffer)) != -1) {
      total += read;
      if (total > MAX_RESPONSE_SIZE) {
        throw new OAuth2Exception(
            "HTTP response for token retrieval exceeds the maximum allowed size of "
                + MAX_RESPONSE_SIZE
                + " bytes");
      }
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }

  private static String authorization(String username, String password) {
    return "Basic " + base64(encode(username) + ":" + encode(password));
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

  private static boolean isJson(String contentType) {
    if (contentType == null) {
      return false;
    }
    String type = contentType;
    int semiColon = type.indexOf(';');
    if (semiColon >= 0) {
      type = type.substring(0, semiColon);
    }
    type = type.trim().toLowerCase(Locale.ROOT);
    int slash = type.indexOf('/');
    if (slash < 0) {
      return false;
    }
    String subtype = type.substring(slash + 1);
    return subtype.equals("json") || subtype.endsWith("+json");
  }

  private static String nonBlank(String value, String name) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException(name + " must not be null or blank");
    }
    return value;
  }

  private static <T> T nonNull(T value, String name) {
    if (value == null) {
      throw new IllegalArgumentException(name + " must not be null");
    }
    return value;
  }

  /** Builder for {@link HttpTokenRequester}. */
  public static final class Builder {

    private String tokenEndpointUri;
    private String clientId;
    private String clientSecret;
    private String grantType;
    private final Map<String, String> parameters = new HashMap<>();
    private TokenParser parser;
    private Consumer<HttpURLConnection> connectionConfigurator;

    private Builder() {}

    public Builder tokenEndpointUri(String tokenEndpointUri) {
      this.tokenEndpointUri = tokenEndpointUri;
      return this;
    }

    public Builder clientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public Builder clientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public Builder grantType(String grantType) {
      this.grantType = grantType;
      return this;
    }

    public Builder parameter(String name, String value) {
      if (value == null) {
        this.parameters.remove(name);
      } else {
        this.parameters.put(name, value);
      }
      return this;
    }

    public Builder parameters(Map<String, String> parameters) {
      nonNull(parameters, "parameters");
      this.parameters.putAll(parameters);
      return this;
    }

    public Builder parser(TokenParser parser) {
      this.parser = parser;
      return this;
    }

    /**
     * Sets a configurator applied to the connection after all library defaults (timeouts, headers,
     * {@code Authorization}) have been set. The configurator runs last and may override any of
     * them.
     *
     * @param connectionConfigurator connection configurator
     * @return this builder
     */
    public Builder connectionConfigurator(Consumer<HttpURLConnection> connectionConfigurator) {
      this.connectionConfigurator = connectionConfigurator;
      return this;
    }

    public HttpTokenRequester build() {
      return new HttpTokenRequester(
          this.tokenEndpointUri,
          this.clientId,
          this.clientSecret,
          this.grantType,
          this.parameters,
          this.connectionConfigurator,
          this.parser);
    }
  }
}
