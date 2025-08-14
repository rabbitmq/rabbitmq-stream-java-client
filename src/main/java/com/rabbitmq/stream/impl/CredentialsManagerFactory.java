// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.impl.StreamEnvironmentBuilder.DefaultOAuth2Configuration;
import com.rabbitmq.stream.oauth2.CredentialsManager;
import com.rabbitmq.stream.oauth2.GsonTokenParser;
import com.rabbitmq.stream.oauth2.HttpTokenRequester;
import com.rabbitmq.stream.oauth2.TokenCredentialsManager;
import java.net.http.HttpClient;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import javax.net.ssl.SSLContext;

final class CredentialsManagerFactory {

  private static final CredentialsManager.Registration CALLBACK_DELEGATING_REGISTRATION =
      new CredentialsManager.Registration() {
        @Override
        public void connect(CredentialsManager.AuthenticationCallback callback) {
          callback.authenticate(null, null);
        }

        @Override
        public void close() {}
      };

  private static final CredentialsManager CREDENTIALS_MANAGER =
      (name, updateCallback) -> CALLBACK_DELEGATING_REGISTRATION;

  static CredentialsManager get(
      DefaultOAuth2Configuration oauth2, ScheduledExecutorService scheduledExecutorService) {
    if (oauth2 != null && oauth2.enabled()) {
      Consumer<HttpClient.Builder> clientBuilderConsumer;
      if (oauth2.tlsEnabled()) {
        SSLContext sslContext = oauth2.sslContext();
        clientBuilderConsumer = b -> b.sslContext(sslContext);
      } else {
        clientBuilderConsumer = ignored -> {};
      }
      HttpTokenRequester tokenRequester =
          new HttpTokenRequester(
              oauth2.tokenEndpointUri(),
              oauth2.clientId(),
              oauth2.clientSecret(),
              oauth2.grantType(),
              oauth2.parameters(),
              clientBuilderConsumer,
              null,
              new GsonTokenParser());
      return new TokenCredentialsManager(
          tokenRequester, scheduledExecutorService, oauth2.refreshDelayStrategy());
    } else {
      return CREDENTIALS_MANAGER;
    }
  }

  static CredentialsManager.Registration get() {
    return CALLBACK_DELEGATING_REGISTRATION;
  }
}
