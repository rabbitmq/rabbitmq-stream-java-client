// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

package com.rabbitmq.stream.sasl;

import java.nio.charset.StandardCharsets;

/**
 * The <code>PLAIN</code> {@link SaslMechanism}.
 *
 * <p>This is the default mechanism used.
 *
 * @see <a href="https://tools.ietf.org/html/rfc4616">The PLAIN Simple Authentication and Security
 *     Layer (SASL) Mechanism (RFC 4616)</a>
 */
public class PlainSaslMechanism implements SaslMechanism {

  public static final SaslMechanism INSTANCE = new PlainSaslMechanism();
  private static final String UNICODE_NULL = "\u0000";

  @Override
  public String getName() {
    return "PLAIN";
  }

  @Override
  public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
    if (credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
      UsernamePasswordCredentialsProvider usernamePasswordCredentialsProvider =
          (UsernamePasswordCredentialsProvider) credentialsProvider;
      String response =
          UNICODE_NULL
              + usernamePasswordCredentialsProvider.getUsername()
              + UNICODE_NULL
              + usernamePasswordCredentialsProvider.getPassword();
      return response.getBytes(StandardCharsets.UTF_8);
    } else {
      throw new IllegalArgumentException(
          "Only username/password credentials provider is supported, not "
              + CredentialsProvider.class.getSimpleName());
    }
  }
}
