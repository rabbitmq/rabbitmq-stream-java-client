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

import static com.rabbitmq.stream.impl.TlsTestUtils.caCertificate;
import static com.rabbitmq.stream.impl.TlsTestUtils.clientCertificate;
import static com.rabbitmq.stream.impl.TlsTestUtils.clientKey;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfTlsNotEnabled;
import com.rabbitmq.stream.impl.TestUtils.ErlangVersionAtLeast;
import io.netty.channel.Channel;
import io.netty.handler.ssl.OpenSslContextOption;
import io.netty.handler.ssl.OpenSslSession;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import org.bouncycastle.jsse.BCSSLEngine;
import org.bouncycastle.jsse.BCSSLParameters;
import org.bouncycastle.jsse.provider.BouncyCastleJsseProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;

@DisabledIfTlsNotEnabled
@StreamTestInfrastructure
class AdvancedTlsTest {

  TestUtils.ClientFactory cf;
  String group = "X25519MLKEM768";
  String cipher = "TLS_AES_256_GCM_SHA384";
  String protocol = "TLSv1.3";

  @Test
  @ErlangVersionAtLeast(28)
  void openSslPqcGroupNegotiation() throws Exception {
    SslContextBuilder builder =
        builder()
            .sslProvider(SslProvider.OPENSSL)
            .trustManager(caCertificate())
            .keyManager(clientKey(), clientCertificate());

    builder.option(OpenSslContextOption.GROUPS, new String[] {group});

    SslContext context = builder.build();
    AtomicReference<Channel> channel = new AtomicReference<>();
    cf.get(new ClientParameters().sslContext(context).channelCustomizer(channel::set));

    assertThat(channel).doesNotHaveNullValue();
    Channel ch = channel.get();

    SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
    assertThat(sslHandler).isNotNull();
    SSLSession session = sslHandler.engine().getSession();
    assertThat(session.getCipherSuite()).isEqualTo(cipher);
    assertThat(session.getProtocol()).isEqualTo(protocol);
    assertThat(session).isInstanceOf(OpenSslSession.class);
    OpenSslSession openSslSession = (OpenSslSession) session;
    assertThat(openSslSession.getNamedGroup())
        .as("Negotiated TLS key exchange group")
        .isEqualTo(group);
  }

  @Test
  @ErlangVersionAtLeast(28)
  @EnabledForJreRange(minVersion = 27)
  void jssePqcGroupNegotiation() throws Exception {
    SslContextBuilder builder =
        builder()
            .sslProvider(SslProvider.JDK)
            .trustManager(caCertificate())
            .keyManager(clientKey(), clientCertificate());

    SslContext context = builder.build();
    AtomicReference<Channel> channel = new AtomicReference<>();
    cf.get(
        new ClientParameters()
            .sslContext(context)
            .channelCustomizer(
                ch -> {
                  channel.set(ch);
                  SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
                  if (sslHandler != null) {
                    SSLParameters sslParams = sslHandler.engine().getSSLParameters();
                    // to compile on Java < 20
                    TlsTestUtils.setNamesGroups(sslParams, new String[] {group});
                    sslParams.setCipherSuites(new String[] {cipher});
                    sslHandler.engine().setSSLParameters(sslParams);
                  }
                }));

    assertThat(channel).doesNotHaveNullValue();
    Channel ch = channel.get();

    SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
    assertThat(sslHandler).isNotNull();
    SSLSession session = sslHandler.engine().getSession();
    assertThat(session.getCipherSuite()).isEqualTo(cipher);
    assertThat(session.getProtocol()).isEqualTo(protocol);
    // there is no way to check the key exchange algorithm used
  }

  @Test
  @ErlangVersionAtLeast(28)
  void bouncyCastlePqcGroupNegotiation() throws Exception {
    java.security.Provider bcJsseProvider = new BouncyCastleJsseProvider();
    java.security.Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

    SslContextBuilder builder =
        builder()
            .sslProvider(SslProvider.JDK)
            .sslContextProvider(bcJsseProvider)
            .trustManager(caCertificate())
            .keyManager(clientKey(), clientCertificate());

    SslContext context = builder.build();
    AtomicReference<Channel> channel = new AtomicReference<>();
    cf.get(
        new ClientParameters()
            .sslContext(context)
            .channelCustomizer(
                ch -> {
                  SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
                  if (sslHandler != null && sslHandler.engine() instanceof BCSSLEngine) {
                    channel.set(ch);
                    // use BC API, to set named groups with Java < 20
                    BCSSLEngine engine = (BCSSLEngine) sslHandler.engine();
                    BCSSLParameters sslParams = engine.getParameters();
                    sslParams.setNamedGroups(new String[] {group});
                    engine.setParameters(sslParams);
                  }
                }));

    assertThat(channel).doesNotHaveNullValue();
    Channel ch = channel.get();

    SslHandler sslHandler = ch.pipeline().get(SslHandler.class);
    assertThat(sslHandler).isNotNull();
    SSLSession session = sslHandler.engine().getSession();
    assertThat(session.getCipherSuite()).isEqualTo(cipher);
    assertThat(session.getProtocol()).isEqualTo(protocol);
    assertThat(session.getClass().getName()).containsIgnoringCase("bouncycastle");
  }

  private SslContextBuilder builder() {
    return SslContextBuilder.forClient();
  }
}
