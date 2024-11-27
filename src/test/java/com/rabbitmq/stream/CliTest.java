// Copyright (c) 2023-2024 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Cli.ConnectionInfo;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class CliTest {

  @Test
  @Disabled
  void deserializeConnectionInfo() {
    List<ConnectionInfo> connections =
        Cli.toConnectionInfoList(LIST_STREAM_CONNECTIONS_JSON_OUTPUT);
    assertThat(connections).hasSize(3);
    ConnectionInfo c = connections.get(0);
    assertThat(c.name()).isEqualTo("127.0.0.1:49214 -> 127.0.1.1:5552");
    assertThat(c.clientProvidedName()).isEqualTo("rabbitmq-stream-consumer-0");
  }

  private static final String LIST_STREAM_CONNECTIONS_JSON_OUTPUT =
      "[\n"
          + "{\"conn_name\":\"127.0.0.1:49214 -> 127.0.1.1:5552\",\"client_properties\":[[\"connection_name\",\"longstr\",\"rabbitmq-stream-consumer-0\"],[\"copyright\",\"longstr\",\"Copyright (c) 2020-2023 Broadcom Inc. and/or its subsidiaries.\"],[\"information\",\"longstr\",\"Licensed under the MPL 2.0. See https://www.rabbitmq.com/\"],[\"platform\",\"longstr\",\"Java\"],[\"product\",\"longstr\",\"RabbitMQ Stream\"],[\"version\",\"longstr\",\"0.5.0-SNAPSHOT\"]]}\n"
          + ",{\"conn_name\":\"127.0.0.1:49212 -> 127.0.1.1:5552\",\"client_properties\":[[\"connection_name\",\"longstr\",\"rabbitmq-stream-producer-0\"],[\"copyright\",\"longstr\",\"Copyright (c) 2020-2023 Broadcom Inc. and/or its subsidiaries.\"],[\"information\",\"longstr\",\"Licensed under the MPL 2.0. See https://www.rabbitmq.com/\"],[\"platform\",\"longstr\",\"Java\"],[\"product\",\"longstr\",\"RabbitMQ Stream\"],[\"version\",\"longstr\",\"0.5.0-SNAPSHOT\"]]}\n"
          + ",{\"conn_name\":\"127.0.0.1:58118 -> 127.0.0.1:5552\",\"client_properties\":[[\"connection_name\",\"longstr\",\"rabbitmq-stream-locator-0\"],[\"copyright\",\"longstr\",\"Copyright (c) 2020-2023 Broadcom Inc. and/or its subsidiaries.\"],[\"information\",\"longstr\",\"Licensed under the MPL 2.0. See https://www.rabbitmq.com/\"],[\"platform\",\"longstr\",\"Java\"],[\"product\",\"longstr\",\"RabbitMQ Stream\"],[\"version\",\"longstr\",\"0.5.0-SNAPSHOT\"]]}\n"
          + "]";
}
