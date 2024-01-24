// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.sasl;

/** Contract to handle SASL challenges sent by a SASL server. */
public interface SaslMechanism {

  /**
   * The name of the SASL mechanism.
   *
   * @return the name of the SASL mechanism
   */
  String getName();

  /**
   * Handle a challenge from the server
   *
   * @param challenge the server challenge
   * @param credentialsProvider the credentials to use
   * @return the response to the challenge
   */
  byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider);
}
