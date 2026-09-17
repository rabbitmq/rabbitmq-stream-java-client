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

import static com.rabbitmq.stream.impl.Utils.TRUST_EVERYTHING_TRUST_MANAGER;

import com.rabbitmq.stream.Cli;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.io.FileInputStream;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.AccessController;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PrivilegedAction;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

final class TlsTestUtils {

  private TlsTestUtils() {}

  static SslContext alwaysTrustSslContext(SslContextBuilder builder) {
    try {
      return builder
          .trustManager(TRUST_EVERYTHING_TRUST_MANAGER)
          .endpointIdentificationAlgorithm(null)
          .build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }
  }

  static X509Certificate caCertificate() throws Exception {
    return loadCertificate(caCertificateFile());
  }

  static String caCertificateFile() {
    return tlsArtefactPath(
        System.getProperty("ca.certificate", "./tls-gen/basic/result/ca_certificate.pem"));
  }

  static X509Certificate clientCertificate() throws Exception {
    return loadCertificate(clientCertificateFile());
  }

  static String clientCertificateFile() {
    return tlsArtefactPath(
        System.getProperty(
            "client.certificate",
            "./tls-gen/basic/result/client_" + hostname() + "_certificate.pem"));
  }

  static PrivateKey clientKey() throws Exception {
    return loadPrivateKey(clientKeyFile());
  }

  static PrivateKey loadPrivateKey(String filename) throws Exception {
    File file = new File(filename);
    String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());

    String privateKeyPEM =
        key.replace("-----BEGIN PRIVATE KEY-----", "")
            .replaceAll(System.lineSeparator(), "")
            .replace("-----END PRIVATE KEY-----", "");

    byte[] decoded = Base64.getDecoder().decode(privateKeyPEM);

    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(decoded);
    PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
    return privateKey;
  }

  static String clientKeyFile() {
    return tlsArtefactPath(
        System.getProperty(
            "client.key", "./tls-gen/basic/result/client_" + hostname() + "_key.pem"));
  }

  static X509Certificate loadCertificate(String file) throws Exception {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      X509Certificate certificate = (X509Certificate) fact.generateCertificate(inputStream);
      return certificate;
    }
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return Cli.hostname();
    }
  }

  private static String tlsArtefactPath(String in) {
    return in.replace("$(hostname)", hostname()).replace("$(hostname -s)", hostname());
  }

  // from Netty's OpenSslParametersUtil
  private static final MethodHandle SET_NAMED_GROUPS;

  static {
    MethodHandle setNamedGroups = null;
    if (PlatformDependent.javaVersion() >= 20) {
      final MethodHandles.Lookup lookup = MethodHandles.lookup();
      setNamedGroups =
          obtainHandle(lookup, "setNamedGroups", MethodType.methodType(void.class, String[].class));
    }
    SET_NAMED_GROUPS = setNamedGroups;
  }

  @SuppressWarnings("removal")
  private static MethodHandle obtainHandle(
      final MethodHandles.Lookup lookup, final String methodName, final MethodType type) {
    return AccessController.doPrivileged(
        (PrivilegedAction<MethodHandle>)
            () -> {
              try {
                return lookup.findVirtual(SSLParameters.class, methodName, type);
              } catch (UnsupportedOperationException
                  | SecurityException
                  | NoSuchMethodException
                  | IllegalAccessException e) {
                // Just ignore it.
                return null;
              }
            });
  }

  static void setNamesGroups(SSLParameters parameters, String[] names) {
    if (SET_NAMED_GROUPS == null) {
      return;
    }
    try {
      SET_NAMED_GROUPS.invoke(parameters, names);
    } catch (Throwable ignore) {
      // Ignore
    }
  }
}
