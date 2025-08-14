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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Token parser for <a href="https://www.rfc-editor.org/rfc/rfc6749#section-5.1">JSON OAuth 2 Access
 * tokens</a>.
 *
 * <p>Uses <a href="https://github.com/google/gson">GSON</a> for the JSON parsing.
 */
public class GsonTokenParser implements TokenParser {

  private static final Gson GSON = new Gson();
  private static final TypeToken<Map<String, Object>> MAP_TYPE = new TypeToken<>() {};

  @Override
  public Token parse(String tokenAsString) {
    Map<String, Object> tokenAsMap = GSON.fromJson(tokenAsString, MAP_TYPE);
    String accessToken = (String) tokenAsMap.get("access_token");
    // in seconds, see https://www.rfc-editor.org/rfc/rfc6749#section-5.1
    Duration expiresIn = Duration.ofSeconds(((Number) tokenAsMap.get("expires_in")).longValue());
    Instant expirationTime =
        Instant.ofEpochMilli(System.currentTimeMillis() + expiresIn.toMillis());
    return new DefaultTokenInfo(accessToken, expirationTime);
  }

  private static final class DefaultTokenInfo implements Token {

    private final String value;
    private final Instant expirationTime;

    private DefaultTokenInfo(String value, Instant expirationTime) {
      this.value = value;
      this.expirationTime = expirationTime;
    }

    @Override
    public String value() {
      return this.value;
    }

    @Override
    public Instant expirationTime() {
      return this.expirationTime;
    }
  }
}
