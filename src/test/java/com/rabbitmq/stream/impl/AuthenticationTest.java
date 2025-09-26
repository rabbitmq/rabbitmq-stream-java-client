// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Cli.addUser;
import static com.rabbitmq.stream.Cli.changePassword;
import static com.rabbitmq.stream.Cli.clearPermissions;
import static com.rabbitmq.stream.Cli.deleteUser;
import static com.rabbitmq.stream.Cli.setPermissions;
import static com.rabbitmq.stream.impl.Assertions.assertThat;
import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_3_13_0;
import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_4_1_4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.AuthenticationFailureException;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.DefaultSaslConfiguration;
import com.rabbitmq.stream.sasl.DefaultUsernamePasswordCredentialsProvider;
import com.rabbitmq.stream.sasl.JdkSaslConfiguration;
import com.rabbitmq.stream.sasl.PlainSaslMechanism;
import com.rabbitmq.stream.sasl.SaslMechanism;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class AuthenticationTest {

  TestUtils.ClientFactory cf;
  String brokerVersion;

  @Test
  void authenticateShouldPassWithValidCredentials() {
    cf.get(new Client.ClientParameters());
  }

  @Test
  void authenticateWithJdkSaslConfiguration() {
    cf.get(
        new Client.ClientParameters()
            .saslConfiguration(
                new JdkSaslConfiguration(
                    new DefaultUsernamePasswordCredentialsProvider("guest", "guest"),
                    () -> "localhost")));
  }

  @Test
  void authenticateShouldFailWhenUsingBadCredentials() {
    try {
      cf.get(new Client.ClientParameters().username("bad").password("bad"));
    } catch (AuthenticationFailureException e) {
      assertThat(e.getMessage())
          .contains(String.valueOf(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE));
    }
  }

  @Test
  void authenticateShouldFailWhenUsingUnsupportedSaslMechanism() {
    try {
      cf.get(
          new Client.ClientParameters()
              .saslConfiguration(
                  mechanisms ->
                      new SaslMechanism() {
                        @Override
                        public String getName() {
                          return "FANCY-SASL";
                        }

                        @Override
                        public byte[] handleChallenge(
                            byte[] challenge, CredentialsProvider credentialsProvider) {
                          return new byte[0];
                        }
                      }));
    } catch (StreamException e) {
      assertThat(e.getMessage())
          .contains(String.valueOf(Constants.RESPONSE_CODE_SASL_MECHANISM_NOT_SUPPORTED));
    }
  }

  @Test
  void authenticateShouldFailWhenSendingGarbageToSaslChallenge() {
    try {
      cf.get(
          new Client.ClientParameters()
              .rpcTimeout(Duration.ofSeconds(1))
              .saslConfiguration(
                  mechanisms ->
                      new SaslMechanism() {
                        @Override
                        public String getName() {
                          return PlainSaslMechanism.INSTANCE.getName();
                        }

                        @Override
                        public byte[] handleChallenge(
                            byte[] challenge, CredentialsProvider credentialsProvider) {
                          return "blabla".getBytes(StandardCharsets.UTF_8);
                        }
                      }));
    } catch (StreamException e) {
      // there can be a timeout because the connection gets closed before returning the error
      assertThat(e).hasMessageContaining(String.valueOf(Constants.RESPONSE_CODE_SASL_ERROR));
    }
  }

  @Test
  void accessToNonExistingVirtualHostShouldFail() {
    try {
      cf.get(new Client.ClientParameters().virtualHost(UUID.randomUUID().toString()));
    } catch (StreamException e) {
      assertThat(e.getMessage())
          .contains(String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE));
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_3_13_0)
  void updateSecretShouldSucceedWithNewCorrectPassword() {
    String username = "stream";
    String password = "stream";
    String newPassword = "new-password";
    try {
      addUser(username, password);
      setPermissions(username, "/", "^stream.*$");
      Client client = cf.get(new Client.ClientParameters().username("stream").password(username));
      changePassword(username, newPassword);
      // OK
      client.authenticate(credentialsProvider(username, newPassword));
      assertThat(client.isOpen()).isTrue();
      client.close();
    } finally {
      deleteUser(username);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_3_13_0)
  void updateSecretBrokerShouldCloseConnectionWithWrongPassword() {
    String u = Utils.DEFAULT_USERNAME, p = u;
    TestUtils.Sync closedSync = TestUtils.sync();
    Client client =
        cf.get(
            new Client.ClientParameters()
                .username(u)
                .password(p)
                .shutdownListener(shutdownContext -> closedSync.down()));
    // wrong password
    assertThatThrownBy(() -> client.authenticate(credentialsProvider(u, p + "foo")))
        .isInstanceOf(AuthenticationFailureException.class)
        .hasMessageContaining(String.valueOf(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE));
    if (connectionClosedOnUpdateSecretFailure()) {
      assertThat(closedSync).completes();
      assertThat(client.isOpen()).isFalse();
    }
    client.close();
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_3_13_0)
  void updateSecretBrokerShouldCloseConnectionWithUpdatedUsername() {
    String username = "stream";
    String password = "stream";
    try {
      addUser(username, password);
      setPermissions(username, "/", "^stream.*$");
      TestUtils.Sync closedSync = TestUtils.sync();
      Client client =
          cf.get(
              new Client.ClientParameters()
                  .username(username)
                  .password(password)
                  .shutdownListener(shutdownContext -> closedSync.down()));
      // cannot change username
      String u = Utils.DEFAULT_USERNAME, p = u;
      assertThatThrownBy(() -> client.authenticate(credentialsProvider(u, p)))
          .isInstanceOf(StreamException.class)
          .hasMessageContaining(
              String.valueOf(Constants.RESPONSE_CODE_SASL_CANNOT_CHANGE_USERNAME));
      if (connectionClosedOnUpdateSecretFailure()) {
        assertThat(closedSync).completes();
        assertThat(client.isOpen()).isFalse();
      }
      client.close();
    } finally {
      deleteUser(username);
    }
  }

  @Test
  @BrokerVersionAtLeast(RABBITMQ_4_1_4)
  void updateSecretBrokerShouldCloseConnectionIfUnauthorizedVhost() {
    String u = "stream";
    String p = "stream";
    try {
      addUser(u, p);
      setPermissions(u, "/", "^stream.*$");
      TestUtils.Sync closedSync = TestUtils.sync();
      Client client =
          cf.get(
              new Client.ClientParameters()
                  .username(u)
                  .password(p)
                  .shutdownListener(shutdownContext -> closedSync.down()));
      clearPermissions(u);
      assertThatThrownBy(() -> client.authenticate(credentialsProvider(u, p)))
          .isInstanceOf(StreamException.class)
          .hasMessageContaining(
              String.valueOf(Constants.RESPONSE_CODE_VIRTUAL_HOST_ACCESS_FAILURE));
      assertThat(closedSync).completes();
      assertThat(client.isOpen()).isFalse();
      client.close();
    } finally {
      deleteUser(u);
    }
  }

  @Test
  @BrokerVersionAtLeast(TestUtils.BrokerVersion.RABBITMQ_4_0_0)
  void anonymousAuthenticationShouldWork() {
    try (Client ignored =
        cf.get(
            new Client.ClientParameters().saslConfiguration(DefaultSaslConfiguration.ANONYMOUS))) {}
  }

  private static CredentialsProvider credentialsProvider(String username, String password) {
    return new DefaultUsernamePasswordCredentialsProvider(username, password);
  }

  private boolean connectionClosedOnUpdateSecretFailure() {
    return TestUtils.atLeastVersion(RABBITMQ_4_1_4.version(), brokerVersion);
  }
}
