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
package com.rabbitmq.stream.oauth2;

import io.netty.util.internal.PlatformDependent;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.security.AccessController;
import java.security.PrivilegedAction;
import javax.net.ssl.SSLParameters;

/**
 * Reflective access to {@code SSLParameters#setNamedGroups(String[])}, which exists only from Java
 * 20. Adapted from Netty's {@code OpenSslParametersUtil}.
 */
final class TlsUtils {

  private static final int MIN_JAVA_VERSION_FOR_NAMED_GROUPS = 20;

  private static final MethodHandle SET_NAMED_GROUPS;

  static {
    MethodHandle setNamedGroups = null;
    if (PlatformDependent.javaVersion() >= MIN_JAVA_VERSION_FOR_NAMED_GROUPS) {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      setNamedGroups =
          obtainHandle(lookup, "setNamedGroups", MethodType.methodType(void.class, String[].class));
    }
    SET_NAMED_GROUPS = setNamedGroups;
  }

  private TlsUtils() {}

  @SuppressWarnings("removal")
  private static MethodHandle obtainHandle(
      MethodHandles.Lookup lookup, String methodName, MethodType type) {
    return AccessController.doPrivileged(
        (PrivilegedAction<MethodHandle>)
            () -> {
              try {
                return lookup.findVirtual(SSLParameters.class, methodName, type);
              } catch (UnsupportedOperationException
                  | SecurityException
                  | NoSuchMethodException
                  | IllegalAccessException e) {
                return null;
              }
            });
  }

  /**
   * Sets the named groups (key exchange groups) on the given parameters.
   *
   * <p>Unlike the test-tree equivalent this is not a silent no-op on unsupported JREs: falling back
   * to classical key exchange without telling the caller would defeat the point of asking for a
   * specific (e.g. post-quantum) group.
   */
  static void setNamedGroups(SSLParameters parameters, String[] namedGroups) {
    if (SET_NAMED_GROUPS == null) {
      throw new UnsupportedOperationException(
          "TLS named groups require Java "
              + MIN_JAVA_VERSION_FOR_NAMED_GROUPS
              + " or more, running on "
              + System.getProperty("java.version"));
    }
    try {
      SET_NAMED_GROUPS.invoke(parameters, namedGroups);
    } catch (Throwable e) {
      throw new OAuth2Exception("Error while setting TLS named groups", e);
    }
  }
}
