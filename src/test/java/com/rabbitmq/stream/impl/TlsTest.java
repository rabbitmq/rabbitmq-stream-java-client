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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import javax.net.ssl.SSLException;
import javax.net.ssl.X509TrustManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class TlsTest {
  
  TestUtils.ClientFactory cf;
  int credit = 10;

  @Test
  void publishAndConsume() throws Exception {
    String s = "tls-stream";
    int publishCount = 10000;

    CountDownLatch consumedLatch = new CountDownLatch(publishCount);
    Client.ChunkListener chunkListener =
        (client, correlationId, offset, messageCount, dataSize) -> {
          if (consumedLatch.getCount() != 0) {
            client.credit(correlationId, 1);
          }
        };

    Client.MessageListener messageListener = (corr, offset, data) -> consumedLatch.countDown();

    Client client =
        cf.get(
            new Client.ClientParameters()
                .sslContext(sslContext())
                .chunkListener(chunkListener)
                .messageListener(messageListener));

    boolean created = client.create(s).isOk();

    client.subscribe(b(1), s, OffsetSpecification.first(), credit);

    if (created) {
      CountDownLatch confirmedLatch = new CountDownLatch(publishCount);
      new Thread(
              () -> {
                Client publisher =
                    cf.get(
                        new Client.ClientParameters()
                            .sslContext(sslContext())
                            .publishConfirmListener(
                                (publisherId, correlationId) -> confirmedLatch.countDown()));
                int messageId = 0;
                publisher.declarePublisher(b(1), null, s);
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
    }
    assertThat(consumedLatch.await(15, SECONDS)).isTrue();
    client.unsubscribe(b(1));
  }

  @Test
  void tls() {
    ClientParameters parameters = new ClientParameters().sslContext(sslContext());
    Client client = new Client(parameters);
    client.close();
  }

  static SslContext sslContext() {
    try {
      return SslContextBuilder.forClient().trustManager(new AlwaysTrustTrustManager()).build();
    } catch (SSLException e) {
      throw new RuntimeException(e);
    }
  }

  private static class AlwaysTrustTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  }
}
