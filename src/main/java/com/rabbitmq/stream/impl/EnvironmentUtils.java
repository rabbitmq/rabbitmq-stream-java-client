// Copyright (c) 2026 Broadcom. All Rights Reserved.
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

import com.rabbitmq.stream.Environment;
import java.util.function.Function;

/**
 * Utilities on top of an {@link Environment}.
 *
 * <p><b>This is an internal API, its contract can change in any release, use it at your own
 * risk.</b>
 */
public final class EnvironmentUtils {

  private EnvironmentUtils() {}

  /**
   * Runs an operation with the {@link Client} of a locator connection of the environment.
   *
   * <p>The call initializes the locator connections of an environment with lazy initialization. The
   * operation is retried a few times when no locator is available, for example while the locator
   * connections recover. The operation must not close the client.
   *
   * <p><b>This is an internal API, its contract can change in any release, use it at your own
   * risk.</b>
   *
   * @param environment the environment, created with {@link Environment#builder()}
   * @param operation the operation
   * @param <T> the type of the result
   * @return the result of the operation
   * @throws IllegalArgumentException if the environment was not created with {@link
   *     Environment#builder()}
   * @throws IllegalStateException if the environment is closed
   */
  public static <T> T locatorOperation(Environment environment, Function<Client, T> operation) {
    if (!(environment instanceof StreamEnvironment)) {
      throw new IllegalArgumentException(
          "The environment must be created with Environment.builder(), not "
              + (environment == null ? null : environment.getClass().getName()));
    }
    StreamEnvironment streamEnvironment = (StreamEnvironment) environment;
    streamEnvironment.checkNotClosed();
    streamEnvironment.maybeInitializeLocator();
    return streamEnvironment.locatorOperation(operation);
  }
}
