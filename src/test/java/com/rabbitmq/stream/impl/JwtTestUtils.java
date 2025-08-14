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
package com.rabbitmq.stream.impl;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.stream.oauth2.Token;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.NumericDate;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.keys.HmacKey;

final class JwtTestUtils {

  private static final String BASE64_KEY = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";
  private static final HmacKey KEY = new HmacKey(Base64.getDecoder().decode(BASE64_KEY));
  private static final String AUDIENCE = "rabbitmq";
  private static final Gson GSON = new Gson();
  private static final TypeToken<Map<String, Object>> MAP_TYPE = new TypeToken<>() {};

  private JwtTestUtils() {}

  static String token(long expirationTime) {
    try {
      JwtClaims claims = new JwtClaims();
      claims.setIssuer("unit_test");
      claims.setAudience(AUDIENCE);
      claims.setExpirationTime(NumericDate.fromMilliseconds(expirationTime));
      claims.setStringListClaim(
          "scope", List.of("rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"));
      claims.setStringClaim("random", RandomStringUtils.insecure().nextAscii(6));

      JsonWebSignature signature = new JsonWebSignature();

      signature.setKeyIdHeaderValue("token-key");
      signature.setAlgorithmHeaderValue(AlgorithmIdentifiers.HMAC_SHA256);
      signature.setKey(KEY);
      signature.setPayload(claims.toJson());
      return signature.getCompactSerialization();
    } catch (Exception e) {
      System.out.println("ERROR " + e.getMessage());
      throw new RuntimeException(e);
    }
  }

  static Token parseToken(String tokenAsString) {
    long expirationTime;
    try {
      JwtConsumer consumer =
          new JwtConsumerBuilder()
              .setExpectedAudience(AUDIENCE)
              // we do not validate the expiration time
              .setEvaluationTime(NumericDate.fromMilliseconds(0))
              .setVerificationKey(KEY)
              .build();
      JwtClaims claims = consumer.processToClaims(tokenAsString);
      expirationTime = claims.getExpirationTime().getValueInMillis();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new Token() {
      @Override
      public String value() {
        return tokenAsString;
      }

      @Override
      public Instant expirationTime() {
        return Instant.ofEpochMilli(expirationTime);
      }
    };
  }

  static Map<String, Object> parse(String json) {
    return GSON.fromJson(json, MAP_TYPE);
  }
}
