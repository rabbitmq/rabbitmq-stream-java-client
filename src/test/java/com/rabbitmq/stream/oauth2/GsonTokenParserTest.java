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

import static com.rabbitmq.stream.oauth2.OAuth2TestUtils.sampleJsonToken;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class GsonTokenParserTest {

  TokenParser parser = new GsonTokenParser();

  @Test
  void parse() {
    String accessToken = UUID.randomUUID().toString();
    Duration expireIn = ofSeconds(60);
    String jsonToken = sampleJsonToken(accessToken, expireIn);
    Token token = parser.parse(jsonToken);
    assertThat(token.value()).isEqualTo(accessToken);
    assertThat(token.expirationTime())
        .isCloseTo(Instant.now().plus(expireIn), within(1, ChronoUnit.SECONDS));
  }

  @Test
  void parseRejectsNonJsonBody() {
    assertThatThrownBy(() -> parser.parse("not json")).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void parseRejectsJsonArray() {
    assertThatThrownBy(() -> parser.parse("[1, 2, 3]")).isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void parseRejectsMissingAccessToken() {
    assertThatThrownBy(() -> parser.parse("{\"expires_in\": 60}"))
        .isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void parseRejectsNonStringAccessToken() {
    assertThatThrownBy(() -> parser.parse("{\"access_token\": 123, \"expires_in\": 60}"))
        .isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void parseRejectsMissingExpiresIn() {
    assertThatThrownBy(() -> parser.parse("{\"access_token\": \"token\"}"))
        .isInstanceOf(OAuth2Exception.class);
  }

  @Test
  void parseRejectsNonNumberExpiresIn() {
    assertThatThrownBy(
            () -> parser.parse("{\"access_token\": \"token\", \"expires_in\": \"soon\"}"))
        .isInstanceOf(OAuth2Exception.class);
  }
}
