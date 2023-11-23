// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Host;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class ShutdownListenerTest {

  @Test
  void shutdownListenerShouldBeCalledOnClientClosing() {
    AtomicReference<Client.ShutdownContext.ShutdownReason> reason = new AtomicReference<>();
    Client client =
        new Client(
            new Client.ClientParameters()
                .shutdownListener(
                    shutdownContext -> reason.set(shutdownContext.getShutdownReason())));
    client.close();
    assertThat(reason.get())
        .isNotNull()
        .isEqualTo(Client.ShutdownContext.ShutdownReason.CLIENT_CLOSE);
    assertThat(client.isOpen()).isFalse();
  }

  @Test
  void shutdownListenerShouldBeCalledOnServerClosing() throws Exception {
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    AtomicReference<Client.ShutdownContext.ShutdownReason> reason = new AtomicReference<>();
    Client client =
        new Client(
            new Client.ClientParameters()
                .shutdownListener(
                    shutdownContext -> {
                      reason.set(shutdownContext.getShutdownReason());
                      shutdownLatch.countDown();
                    }));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(out);
    dataOutputStream.writeInt(4);
    dataOutputStream.writeShort(30000); // command ID
    dataOutputStream.writeShort(Constants.VERSION_1);
    client.send(out.toByteArray());

    assertThat(shutdownLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(reason.get())
        .isNotNull()
        .isEqualTo(Client.ShutdownContext.ShutdownReason.SERVER_CLOSE);
    assertThat(client.isOpen()).isFalse();
    client.close();
  }

  @Test
  @TestUtils.DisabledIfRabbitMqCtlNotSet
  void shutdownListenerShouldBeCalledWhenConnectionIsKilled() throws Exception {
    String connectionName = UUID.randomUUID().toString();
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    AtomicReference<Client.ShutdownContext.ShutdownReason> reason = new AtomicReference<>();
    Client client =
        new Client(
            new Client.ClientParameters()
                .clientProperty("connection_name", connectionName)
                .shutdownListener(
                    shutdownContext -> {
                      reason.set(shutdownContext.getShutdownReason());
                      shutdownLatch.countDown();
                    }));

    Host.killConnection(connectionName);
    assertThat(shutdownLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(reason.get()).isNotNull().isEqualTo(Client.ShutdownContext.ShutdownReason.UNKNOWN);
    assertThat(client.isOpen()).isFalse();
    client.close();
  }
}
