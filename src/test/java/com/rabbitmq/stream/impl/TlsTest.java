// Copyright (c) 2021-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.BrokerVersion.RABBITMQ_3_13_0;
import static com.rabbitmq.stream.impl.TestUtils.ExceptionConditions.responseCode;
import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.Utils.TRUST_EVERYTHING_TRUST_MANAGER;
import static java.lang.String.format;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfAuthMechanismSslNotEnabled;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import com.rabbitmq.stream.sasl.DefaultSaslConfiguration;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.File;
import java.io.FileInputStream;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.EnumSource;

@DisabledIfTlsNotEnabled
@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@ParameterizedClass
@EnumSource(names = {"JDK", "OPENSSL"})
public class TlsTest {

  @Parameter SslProvider sslProvider;

  String stream;

  TestUtils.ClientFactory cf;
  int credit = 10;

  SslContext alwaysTrustSslContext() {
    try {
      return builder().trustManager(TRUST_EVERYTHING_TRUST_MANAGER).build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }
  }

  static X509Certificate caCertificate() throws Exception {
    return loadCertificate(caCertificateFile());
  }

  static String caCertificateFile() {
    return tlsArtefactPath(
        System.getProperty("ca.certificate", "/tmp/tls-gen/basic/result/ca_certificate.pem"));
  }

  static X509Certificate clientCertificate() throws Exception {
    return loadCertificate(clientCertificateFile());
  }

  static String clientCertificateFile() {
    return tlsArtefactPath(
        System.getProperty(
            "client.certificate",
            "/tmp/tls-gen/basic/result/client_" + hostname() + "_certificate.pem"));
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
            "client.key", "/tmp/tls-gen/basic/result/client_" + hostname() + "_key.pem"));
  }

  static X509Certificate loadCertificate(String file) throws Exception {
    try (FileInputStream inputStream = new FileInputStream(file)) {
      CertificateFactory fact = CertificateFactory.getInstance("X.509");
      X509Certificate certificate = (X509Certificate) fact.generateCertificate(inputStream);
      return certificate;
    }
  }

  @Test
  void publishAndConsumeWithUnverifiedConnection() {
    int publishCount = 1_000_000;

    CountDownLatch consumedLatch = new CountDownLatch(publishCount);
    Client.ChunkListener chunkListener =
        (client, correlationId, offset, messageCount, dataSize) -> {
          if (consumedLatch.getCount() != 0) {
            client.credit(correlationId, 1);
          }
          return null;
        };

    Client.MessageListener messageListener =
        (corr, offset, chunkTimestamp, committedOffset, chunkContext, data) ->
            consumedLatch.countDown();

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

    latchAssert(confirmedLatch).completes(ofSeconds(20));
    latchAssert(consumedLatch).completes(ofSeconds(20));
    client.unsubscribe(b(1));
  }

  @Test
  void unverifiedConnection() {
    cf.get(new ClientParameters().sslContext(alwaysTrustSslContext()));
  }

  @Test
  void verifiedConnectionWithCorrectServerCertificate() throws Exception {
    // in server certificate SAN
    String hostname = "localhost";
    SslContext context = builder().trustManager(caCertificate()).build();
    cf.get(new ClientParameters().host(hostname).sslContext(context));
  }

  @Test
  void verifiedConnectionWithCorrectServerCertificateWithSni() throws Exception {
    // not in server certificate SAN, but setting SNI makes it work
    String hostname = "127.0.0.1";
    SslContext context =
        builder().trustManager(caCertificate()).serverName(new SNIHostName("localhost")).build();
    cf.get(new ClientParameters().host(hostname).sslContext(context));
  }

  @Test
  void verifiedConnectionWithCorrectServerCertificateFailsIfHostnameNotInSan() throws Exception {
    // not in server certificate SAN
    String hostname = "127.0.0.1";
    SslContext context = builder().trustManager(caCertificate()).build();
    assertThatThrownBy(() -> cf.get(new ClientParameters().host(hostname).sslContext(context)))
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void verifiedConnectionWithWrongServerCertificate() throws Exception {
    SslContext context = builder().trustManager(clientCertificate()).build();
    assertThatThrownBy(() -> cf.get(new ClientParameters().sslContext(context)))
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void verifiedConnectionWithCorrectClientPrivateKey() throws Exception {
    SslContext context =
        builder()
            .trustManager(caCertificate())
            .keyManager(clientKey(), clientCertificate())
            .build();

    cf.get(new ClientParameters().sslContext(context));
  }

  @Test
  @DisabledIfAuthMechanismSslNotEnabled
  @BrokerVersionAtLeast(RABBITMQ_3_13_0)
  void saslExternalShouldSucceedWithUserForClientCertificate() throws Exception {
    X509Certificate clientCertificate = clientCertificate();
    SslContext context =
        builder().trustManager(caCertificate()).keyManager(clientKey(), clientCertificate).build();

    String username = clientCertificate.getSubjectX500Principal().getName();
    Cli.rabbitmqctlIgnoreError(format("delete_user %s", username));
    Cli.rabbitmqctl(format("add_user %s foo", username));
    try {
      Cli.rabbitmqctl(format("set_permissions %s '.*' '.*' '.*'", username));

      cf.get(
          new ClientParameters()
              .username(UUID.randomUUID().toString())
              .sslContext(context)
              .saslConfiguration(DefaultSaslConfiguration.EXTERNAL));
    } finally {
      Cli.rabbitmqctl(format("delete_user %s", username));
    }
  }

  @Test
  @DisabledIfAuthMechanismSslNotEnabled
  @BrokerVersionAtLeast(RABBITMQ_3_13_0)
  void saslExternalShouldFailIfNoUserForClientCertificate() throws Exception {
    X509Certificate clientCertificate = clientCertificate();
    SslContext context =
        builder().trustManager(caCertificate()).keyManager(clientKey(), clientCertificate).build();

    String username = clientCertificate.getSubjectX500Principal().getName();
    Cli.rabbitmqctlIgnoreError(format("delete_user %s", username));
    assertThatThrownBy(
            () ->
                cf.get(
                    new ClientParameters()
                        .username(UUID.randomUUID().toString())
                        .sslContext(context)
                        .saslConfiguration(DefaultSaslConfiguration.EXTERNAL)))
        .isInstanceOf(StreamException.class)
        .has(responseCode(Constants.RESPONSE_CODE_AUTHENTICATION_FAILURE));
  }

  @Test
  void hostnameVerificationShouldFailWhenSettingHostToLoopbackInterface() throws Exception {
    SslContext context = builder().trustManager(caCertificate()).build();
    assertThatThrownBy(() -> cf.get(new ClientParameters().sslContext(context).host("127.0.0.1")))
        .isInstanceOf(StreamException.class)
        .hasCauseInstanceOf(SSLHandshakeException.class);
  }

  @Test
  void shouldConnectWhenSettingHostToLoopbackInterfaceAndDisablingHostnameVerification()
      throws Exception {
    SslContext context =
        builder().endpointIdentificationAlgorithm(null).trustManager(caCertificate()).build();
    cf.get(new ClientParameters().sslContext(context).host("127.0.0.1"));
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
            .sslContext(builder().trustManager(caCertificate()).build())
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
      return Cli.hostname();
    }
  }

  private static String tlsArtefactPath(String in) {
    return in.replace("$(hostname)", hostname()).replace("$(hostname -s)", hostname());
  }

  private SslContextBuilder builder() {
    return SslContextBuilder.forClient().sslProvider(sslProvider);
  }
}
