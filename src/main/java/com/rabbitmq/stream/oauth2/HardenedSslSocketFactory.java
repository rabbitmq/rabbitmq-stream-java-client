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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

/**
 * Delegating {@link SSLSocketFactory} that pins the endpoint identification algorithm on every
 * socket it hands back, and optionally restricts cipher suites and named groups (key exchange
 * groups).
 *
 * <p>{@link javax.net.ssl.HttpsURLConnection} exposes no way to reach an {@link SSLParameters} on a
 * per-connection basis, which is why this goes through the socket factory instead.
 */
final class HardenedSslSocketFactory extends SSLSocketFactory {

  private final SSLSocketFactory delegate;
  private final String[] ciphers;
  private final String[] namedGroups;

  HardenedSslSocketFactory(SSLSocketFactory delegate, String[] ciphers, String[] namedGroups) {
    this.delegate = delegate;
    this.ciphers = ciphers;
    this.namedGroups = namedGroups;
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return this.delegate.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return this.delegate.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket() throws IOException {
    return configure(this.delegate.createSocket());
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return configure(this.delegate.createSocket(host, port));
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localHost, int localPort)
      throws IOException {
    return configure(this.delegate.createSocket(host, port, localHost, localPort));
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return configure(this.delegate.createSocket(host, port));
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return configure(this.delegate.createSocket(address, port, localAddress, localPort));
  }

  @Override
  public Socket createSocket(Socket s, String host, int port, boolean autoClose)
      throws IOException {
    return configure(this.delegate.createSocket(s, host, port, autoClose));
  }

  private Socket configure(Socket socket) {
    SSLSocket sslSocket = (SSLSocket) socket;
    SSLParameters parameters = sslSocket.getSSLParameters();
    parameters.setEndpointIdentificationAlgorithm("HTTPS");
    if (this.ciphers != null) {
      parameters.setCipherSuites(this.ciphers);
    }
    if (this.namedGroups != null) {
      TlsUtils.setNamedGroups(parameters, this.namedGroups);
    }
    sslSocket.setSSLParameters(parameters);
    return sslSocket;
  }
}
