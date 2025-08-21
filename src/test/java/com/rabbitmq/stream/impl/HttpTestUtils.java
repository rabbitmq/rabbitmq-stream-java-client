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

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.rabbitmq.stream.oauth2.OAuth2TestUtils;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.function.LongSupplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public final class HttpTestUtils {

  private static final char[] KEY_STORE_PASSWORD = "password".toCharArray();

  private HttpTestUtils() {}

  public static HttpServer startServer(int port, String path, HttpHandler handler) {
    return startServer(port, path, null, handler);
  }

  public static HttpServer startServer(
      int port, String path, KeyStore keyStore, HttpHandler handler) {
    HttpServer server;
    try {
      if (keyStore != null) {
        KeyManagerFactory keyManagerFactory =
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, KEY_STORE_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
        server = HttpsServer.create(new InetSocketAddress(port), 0);
        ((HttpsServer) server).setHttpsConfigurator(new HttpsConfigurator(sslContext));
      } else {
        server = HttpServer.create(new InetSocketAddress(port), 0);
      }
      server.createContext(path, handler);
      server.start();
      return server;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static KeyStore generateKeyPair() {
    try {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, KEY_STORE_PASSWORD);

      KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
      ECGenParameterSpec spec = new ECGenParameterSpec("secp521r1");
      kpg.initialize(spec);

      KeyPair kp = kpg.generateKeyPair();

      JcaX509v3CertificateBuilder certificateBuilder =
          new JcaX509v3CertificateBuilder(
              new X500NameBuilder().addRDN(BCStyle.CN, "localhost").build(),
              BigInteger.valueOf(new SecureRandom().nextInt()),
              Date.from(Instant.now().minus(10, ChronoUnit.DAYS)),
              Date.from(Instant.now().plus(10, ChronoUnit.DAYS)),
              new X500NameBuilder().addRDN(BCStyle.CN, "localhost").build(),
              kp.getPublic());

      X509CertificateHolder certificateHolder =
          certificateBuilder.build(
              new JcaContentSignerBuilder("SHA512withECDSA").build(kp.getPrivate()));

      X509Certificate certificate =
          new JcaX509CertificateConverter().getCertificate(certificateHolder);

      keyStore.setKeyEntry(
          "default", kp.getPrivate(), KEY_STORE_PASSWORD, new Certificate[] {certificate});
      return keyStore;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static HttpHandler oAuth2TokenHttpHandler(LongSupplier expirationTimeSupplier) {
    return oAuth2TokenHttpHandler(expirationTimeSupplier, () -> {});
  }

  static HttpHandler oAuth2TokenHttpHandler(
      LongSupplier expirationTimeSupplier, Runnable requestCallback) {
    return exchange -> {
      long expirationTime = expirationTimeSupplier.getAsLong();
      String jwtToken = JwtTestUtils.token(expirationTime);
      Duration expiresIn = Duration.ofMillis(expirationTime - currentTimeMillis());
      String oauthToken = OAuth2TestUtils.sampleJsonToken(jwtToken, expiresIn);
      byte[] data = oauthToken.getBytes(UTF_8);
      Headers responseHeaders = exchange.getResponseHeaders();
      responseHeaders.set("content-type", "application/json");
      exchange.sendResponseHeaders(200, data.length);
      OutputStream responseBody = exchange.getResponseBody();
      responseBody.write(data);
      responseBody.close();
      requestCallback.run();
    };
  }
}
