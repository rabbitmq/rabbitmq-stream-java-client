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

import static org.junit.jupiter.api.Assertions.fail;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.function.Supplier;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.bouncycastle.asn1.x500.X500NameBuilder;
import org.bouncycastle.asn1.x500.style.BCStyle;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

public final class OAuth2TestUtils {

  private static final char[] KEY_STORE_PASSWORD = "password".toCharArray();

  private OAuth2TestUtils() {}

  public static String sampleJsonToken(String accessToken, Duration expiresIn) {
    String json =
        "{\n"
            + "  \"access_token\" : \"{accessToken}\",\n"
            + "  \"token_type\" : \"bearer\",\n"
            + "  \"expires_in\" : {expiresIn},\n"
            + "  \"scope\" : \"clients.read emails.write scim.userids password.write idps.write notifications.write oauth.login scim.write critical_notifications.write\",\n"
            + "  \"jti\" : \"18c1b1dfdda04382a8bcc14d077b71dd\"\n"
            + "}";
    return json.replace("{accessToken}", accessToken)
        .replace("{expiresIn}", expiresIn.toSeconds() + "");
  }

  public static int randomNetworkPort() throws IOException {
    ServerSocket socket = new ServerSocket();
    socket.bind(null);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  public static Duration waitAtMost(
      Duration timeout,
      Duration waitTime,
      CallableBooleanSupplier condition,
      Supplier<String> message)
      throws Exception {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    Duration waitedTime = Duration.ZERO;
    Exception exception = null;
    while (waitedTime.compareTo(timeout) <= 0) {
      Thread.sleep(waitTime.toMillis());
      waitedTime = waitedTime.plus(waitTime);
      try {
        if (condition.getAsBoolean()) {
          return waitedTime;
        }
        exception = null;
      } catch (Exception e) {
        exception = e;
      }
    }
    String msg;
    if (message == null) {
      msg = "Waited " + timeout.getSeconds() + " second(s), condition never got true";
    } else {
      msg = "Waited " + timeout.getSeconds() + " second(s), " + message.get();
    }
    if (exception == null) {
      fail(msg);
    } else {
      fail(msg, exception);
    }
    return waitedTime;
  }

  public static Duration waitAtMost(
      Duration timeout, Duration waitTime, CallableBooleanSupplier condition) throws Exception {
    return waitAtMost(timeout, waitTime, condition, null);
  }

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
              new JcaContentSignerBuilder("SHA256withECDSA").build(kp.getPrivate()));

      X509Certificate certificate =
          new JcaX509CertificateConverter().getCertificate(certificateHolder);

      keyStore.setKeyEntry(
          "localhost", kp.getPrivate(), KEY_STORE_PASSWORD, new X509Certificate[] {certificate});

      return keyStore;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static <A, B> Pair<A, B> pair(A v1, B v2) {
    return new Pair<>(v1, v2);
  }

  public interface CallableBooleanSupplier {
    boolean getAsBoolean() throws Exception;
  }

  public static class Pair<A, B> {

    private final A v1;
    private final B v2;

    private Pair(A v1, B v2) {
      this.v1 = v1;
      this.v2 = v2;
    }

    public A v1() {
      return this.v1;
    }

    public B v2() {
      return this.v2;
    }
  }
}
