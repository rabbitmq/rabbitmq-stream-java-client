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

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import javax.security.auth.callback.*;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

/**
 * {@link SaslConfiguration} using the JDK SASL support.
 *
 * <p>This is not the default because SASL is not available on all environments, e.g. Android. This
 * code is based on the implementation from RabbitMQ Java client.
 */
public class JdkSaslConfiguration implements SaslConfiguration {

  private static final String[] DEFAULT_PREFERRED_MECHANISMS = new String[] {"PLAIN"};

  private final Supplier<String> serverNameSupplier;
  private final List<String> mechanisms;
  private final CallbackHandler callbackHandler;

  public JdkSaslConfiguration(
      CredentialsProvider credentialsProvider, Supplier<String> serverNameSupplier) {
    this(credentialsProvider, serverNameSupplier, DEFAULT_PREFERRED_MECHANISMS);
  }

  public JdkSaslConfiguration(
      CredentialsProvider credentialsProvider,
      Supplier<String> serverNameSupplier,
      String[] mechanisms) {
    this.serverNameSupplier = serverNameSupplier;
    this.callbackHandler = new UsernamePasswordCallbackHandler(credentialsProvider);
    this.mechanisms = Arrays.asList(mechanisms);
  }

  @Override
  public SaslMechanism getSaslMechanism(List<String> serverMechanisms) {
    for (String mechanism : mechanisms) {
      if (serverMechanisms.contains(mechanism)) {
        try {
          SaslClient saslClient =
              Sasl.createSaslClient(
                  new String[] {mechanism},
                  null,
                  "RABBITMQ-STREAM",
                  serverNameSupplier.get(),
                  null,
                  callbackHandler);
          if (saslClient != null) return new JdkSaslMechanism(saslClient);
        } catch (SaslException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return null;
  }

  private class JdkSaslMechanism implements SaslMechanism {
    private final SaslClient client;

    public JdkSaslMechanism(SaslClient client) {
      this.client = client;
    }

    @Override
    public String getName() {
      return client.getMechanismName();
    }

    @Override
    public byte[] handleChallenge(byte[] challenge, CredentialsProvider credentialsProvider) {
      try {
        return client.evaluateChallenge(challenge);
      } catch (SaslException e) {
        throw new StreamSaslException(e);
      }
    }
  }

  private class UsernamePasswordCallbackHandler implements CallbackHandler {

    private final UsernamePasswordCredentialsProvider credentialsProvider;

    public UsernamePasswordCallbackHandler(CredentialsProvider credentialsProvider) {
      if (credentialsProvider == null
          || !(credentialsProvider instanceof UsernamePasswordCredentialsProvider)) {
        throw new IllegalArgumentException(
            "Only username/password credentials provider is supported, not "
                + CredentialsProvider.class.getSimpleName());
      }
      this.credentialsProvider = (UsernamePasswordCredentialsProvider) credentialsProvider;
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          nc.setName(credentialsProvider.getUsername());
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          pc.setPassword(credentialsProvider.getPassword().toCharArray());
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized Callback");
        }
      }
    }
  }
}
