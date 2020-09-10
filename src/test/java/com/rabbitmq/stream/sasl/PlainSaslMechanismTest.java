// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

public class PlainSaslMechanismTest {

  @Test
  void handleChallengeShouldEncodeUsernamePasswordCorrectly() {
    byte[] challengeResponse =
        PlainSaslMechanism.INSTANCE.handleChallenge(
            null, new DefaultUsernamePasswordCredentialsProvider("foo", "bar"));
    assertThat(challengeResponse).isEqualTo("\u0000foo\u0000bar".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void handleChallengeSupportsOnlyUsernamePasswordCredentialsProvider() {
    assertThatThrownBy(
            () -> PlainSaslMechanism.INSTANCE.handleChallenge(null, new CredentialsProvider() {}))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
