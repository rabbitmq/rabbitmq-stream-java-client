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
package com.rabbitmq.stream.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.impl.Client.ClientParameters;
import org.junit.jupiter.api.Test;

public class ClientParametersTest {

  @Test
  void duplicate() {
    Client.ClientParameters clientParameters1 =
        new Client.ClientParameters().host("rabbitmq").port(5556);
    clientParameters1.clientProperty("connection_name", "producer");
    ClientParameters clientParameters2 = clientParameters1.duplicate();
    assertThat(clientParameters2.host()).isEqualTo("rabbitmq");
    assertThat(clientParameters2.port()).isEqualTo(5556);

    // same as original
    assertThat(clientParameters2.clientProperties()).containsEntry("connection_name", "producer");
    // changing the copy should not change the original
    clientParameters2.clientProperty("connection_name", "consumer");
    assertThat(clientParameters1.clientProperties()).containsEntry("connection_name", "producer");
  }
}
