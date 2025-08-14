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
package com.rabbitmq.stream.oauth2;

/**
 * Contract to authenticate and possibly re-authenticate application components.
 *
 * <p>A typical "application component" is a connection.
 */
public interface CredentialsManager {

  /** No-op credentials manager. */
  CredentialsManager NO_OP = new NoOpCredentialsManager();

  /**
   * Register a component for authentication.
   *
   * @param name component name (must be unique)
   * @param updateCallback callback to update the component authentication
   * @return the registration (must be closed when no longer necessary)
   */
  Registration register(String name, AuthenticationCallback updateCallback);

  /** A component registration. */
  interface Registration extends AutoCloseable {

    /**
     * Connection request from the component.
     *
     * <p>The component calls this method when it needs to authenticate. The underlying credentials
     * manager implementation must take care of providing the component with the appropriate
     * credentials in the callback.
     *
     * @param callback client code to authenticate the component
     */
    void connect(AuthenticationCallback callback);

    /** Close the registration. */
    void close();
  }

  /**
   * Component authentication callback.
   *
   * <p>The component provides the logic and the manager implementation calls it with the
   * appropriate credentials.
   */
  interface AuthenticationCallback {

    /**
     * Authentication logic.
     *
     * @param username username
     * @param password password
     */
    void authenticate(String username, String password);
  }

  class NoOpCredentialsManager implements CredentialsManager {

    @Override
    public Registration register(String name, AuthenticationCallback updateCallback) {
      return new NoOpRegistration();
    }
  }

  class NoOpRegistration implements Registration {

    @Override
    public void connect(AuthenticationCallback callback) {}

    @Override
    public void close() {}
  }
}
