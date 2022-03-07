// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.Utils.TRUST_EVERYTHING_TRUST_MANAGER;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.ChannelCustomizer;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLParameters;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@DisabledIfTlsNotEnabled
@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class TlsTest {

  static boolean isJava13() {
    String javaVersion = System.getProperty("java.version");
    return javaVersion != null && javaVersion.startsWith("13.");
  }

  @BeforeEach
  public void init() {
    if (isJava13()) {
      // for Java 13.0.7, see https://github.com/bcgit/bc-java/issues/941
      System.setProperty("keystore.pkcs12.keyProtectionAlgorithm", "PBEWithHmacSHA256AndAES_256");
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (isJava13()) {
      System.setProperty("keystore.pkcs12.keyProtectionAlgorithm", "");
    }
  }

  String stream;

  TestUtils.ClientFactory cf;
  int credit = 10;

  static SslContext alwaysTrustSslContext() {
    try {
      return SslContextBuilder.forClient().trustManager(TRUST_EVERYTHING_TRUST_MANAGER).build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }
  }

  static X509Certificate caCertificate() throws Exception {
    return loadCertificate(caCertificateFile());
  }

  static String caCertificateFile() {
    return System.getProperty("ca.certificate", "/tmp/tls-gen/basic/result/ca_certificate.pem");
  }

  static X509Certificate clientCertificate() throws Exception {
    return loadCertificate(clientCertificateFile());
  }

  static String clientCertificateFile() {
    return System.getProperty(
        "client.certificate",
        "/tmp/tls-gen/basic/result/client_" + hostname() + "_certificate.pem");
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
    return System.getProperty(
        "client.key", "/tmp/tls-gen/basic/result/client_" + hostname() + "_key.pem");
  }

  static X509Certificate loadCertificate(String file) throws Exception {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      X509Certificate certificate = (X509Certificate) fact.generateCertificate(inputStream);
      return certificate;
    }
  }

  @Test
  void publishAndConsumeWithUnverifiedConnection() throws Exception {
    int publishCount = 1_000_000;

    CountDownLatch consumedLatch = new CountDownLatch(publishCount);
    Client.ChunkListener chunkListener =
        (client, correlationId, offset, messageCount, dataSize) -> {
          if (consumedLatch.getCount() != 0) {
            client.credit(correlationId, 1);
          }
        };

    Client.MessageListener messageListener =
        (corr, offset, chunkTimestamp, data) -> consumedLatch.countDown();

    Client client =
        cf.get(
            new Client.ClientParameters()
                .sslContext(alwaysTrustSslContext())
                .chunkListener(chunkListener)
                .messageListener(messageListener));

    client.subscribe(b(1), stream, OffsetSpecification.first(), credit);

    CountDownLatch confirmedLatch = new CountDownLatch(publishCount);
    new Thread(
            () -> {
              Client publisher =
                  cf.get(
                      new Client.ClientParameters()
                          .sslContext(alwaysTrustSslContext())
                          .publishConfirmListener(
                              (publisherId, correlationId) -> confirmedLatch.countDown()));
              int messageId = 0;
              publisher.declarePublisher(b(1), null, stream);
              while (messageId < publishCount) {
                messageId++;
                publisher.publish(
                    b(1),
                    Collections.singletonList(
                        publisher
                            .messageBuilder()
                            .addData(("message" + messageId).getBytes(StandardCharsets.UTF_8))
                            .build()));
              }
            })
        .start();

    assertThat(confirmedLatch.await(15, SECONDS)).isTrue();
    assertThat(consumedLatch.await(15, SECONDS)).isTrue();
    client.unsubscribe(b(1));
  }

  @Test
  void unverifiedConnection() {
    cf.get(new ClientParameters().sslContext(alwaysTrustSslContext()));
  }

  @Test
  void unverifiedConnectionWithSni() {
    ChannelCustomizer channelCustomizer =
        ch -> {
          SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
          if (sslHandler != null) {
            SSLParameters sslParameters = sslHandler.engine().getSSLParameters();
            sslParameters.setServerNames(Collections.singletonList(new SNIHostName("localhost")));
            sslHandler.engine().setSSLParameters(sslParameters);
          }
        };
    cf.get(
        new ClientParameters()
            .sslContext(alwaysTrustSslContext())
            .channelCustomizer(channelCustomizer));
  }

  @Test
  void verifiedConnectionWithCorrectServerCertificate() throws Exception {
    SslContext context = SslContextBuilder.forClient().trustManager(caCertificate()).build();
    cf.get(new ClientParameters().sslContext(context));
  }

  @Test
  void verifiedConnectionWithWrongServerCertificate() throws Exception {
    SslContext context = SslContextBuilder.forClient().trustManager(clientCertificate()).build();
    assertThatThrownBy(() -> cf.get(new ClientParameters().sslContext(context)))
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void verifiedConnectionWithCorrectClientPrivateKey() throws Exception {
    SslContext context =
        SslContextBuilder.forClient()
            .trustManager(caCertificate())
            .keyManager(clientKey(), clientCertificate())
            .build();

    cf.get(new ClientParameters().sslContext(context));
  }

  @Test
  void hostnameVerificationShouldFailWhenSettingHostToLoopbackInterface() throws Exception {
    SslContext context = SslContextBuilder.forClient().trustManager(caCertificate()).build();
    assertThatThrownBy(() -> cf.get(new ClientParameters().sslContext(context).host("127.0.0.1")))
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void shouldConnectWhenSettingHostToLoopbackInterfaceAndDisablingHostnameVerification()
      throws Exception {
    SslContext context = SslContextBuilder.forClient().trustManager(caCertificate()).build();
    cf.get(
        new ClientParameters()
            .sslContext(context)
            .host("127.0.0.1")
            .tlsHostnameVerification(false));
  }

  @Test
  void metadataShouldReturnTlsPortForTlsConnection() {
    assertThat(cf.get().metadata(stream).get(stream).getLeader().getPort())
        .isEqualTo(Client.DEFAULT_PORT);
    assertThat(
            cf.get(new ClientParameters().sslContext(alwaysTrustSslContext()))
                .metadata(stream)
                .get(stream)
                .getLeader()
                .getPort())
        .isEqualTo(Client.DEFAULT_TLS_PORT);
  }

  @Test
  void environmentPublisherConsumer() throws Exception {
    try (Environment env =
        Environment.builder()
            .uri("rabbitmq-stream+tls://localhost")
            .addressResolver(addr -> new Address("localhost", Client.DEFAULT_TLS_PORT))
            .tls()
            .sslContext(SslContextBuilder.forClient().trustManager(caCertificate()).build())
            .environmentBuilder()
            .build()) {

      int messageCount = 10_000;

      CountDownLatch latchConfirm = new CountDownLatch(messageCount);
      Producer producer = env.producerBuilder().stream(this.stream).build();
      ConfirmationHandler confirmationHandler = confirmationStatus -> latchConfirm.countDown();
      IntStream.range(0, messageCount)
          .forEach(
              i ->
                  producer.send(
                      producer
                          .messageBuilder()
                          .addData("".getBytes(StandardCharsets.UTF_8))
                          .build(),
                      confirmationHandler));
      assertThat(latchAssert(latchConfirm)).completes();

      CountDownLatch latchConsume = new CountDownLatch(messageCount);
      env.consumerBuilder().stream(this.stream)
          .offset(OffsetSpecification.first())
          .messageHandler((context, message) -> latchConsume.countDown())
          .build();
      assertThat(latchAssert(latchConsume)).completes();
    }
  }

  @Test
  void clientShouldContainServerAdvertisedTlsPort() {
    Client client = cf.get(new ClientParameters().sslContext(alwaysTrustSslContext()));
    assertThat(client.serverAdvertisedPort()).isEqualTo(Client.DEFAULT_TLS_PORT);
  }

  private static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      try {
        return Host.hostname();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }
}
