// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.OffsetSpecification;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@TestUtils.DisabledIfRabbitMqCtlNotSet
public class AuthorisationTest {

  private static final String VH = "test_stream";
  private static final String USERNAME = "stream";
  private static final String PASSWORD = "stream";
  TestUtils.ClientFactory cf;

  @BeforeAll
  static void init() throws Exception {
    Host.rabbitmqctl("add_vhost " + VH);
    Host.rabbitmqctl("add_user " + USERNAME + " " + PASSWORD);
    Host.rabbitmqctl(
        "set_permissions --vhost "
            + VH
            + " "
            + USERNAME
            + " '^stream.*$' '^stream.*$' '^stream.*$'");
    Host.rabbitmqctl("set_permissions --vhost " + VH + " guest '.*' '.*' '.*'");
  }

  @AfterAll
  static void tearDown() throws Exception {
    Host.rabbitmqctl("delete_user stream");
    Host.rabbitmqctl("delete_vhost test_stream");
  }

  static boolean await(CountDownLatch latch) {
    try {
      return latch.await(10, SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void createStreamWithAuthorisedNameShouldSucceed() {
    Client deletionClient = configurationClient();
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "stream-authorized" + i;
              Client.Response response = client.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = deletionClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void createStreamWithUnauthorisedNameShouldFail() {
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              Client.Response response = client.create("not-authorized" + i);
              assertThat(response.isOk()).isFalse();
              assertThat(response.getResponseCode())
                  .isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);
            });
  }

  @Test
  void deleteStreamWithAuthorisedNameShouldSucceed() {
    Client creationClient = configurationClient();
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "stream-authorized" + i;
              Client.Response response = creationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = client.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void deleteStreamWithUnauthorisedNameShouldFail() {
    Client creationClient = configurationClient();
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "not-authorized" + i;
              Client.Response response = creationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = client.delete(stream);
              assertThat(response.isOk()).isFalse();
              assertThat(response.getResponseCode())
                  .isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);

              response = creationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void subscribeToAuthorisedStreamShouldSucceed() {
    Client configurationClient = configurationClient();
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "stream-authorized" + i;
              Client.Response response = configurationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = client.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void subscribeToUnauthorisedStreamShouldFail() {
    Client configurationClient = configurationClient();
    Client client = client();
    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "not-authorized" + i;
              Client.Response response = configurationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              response = client.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
              assertThat(response.isOk()).isFalse();
              assertThat(response.getResponseCode())
                  .isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void publishToAuthorisedStreamShouldSucceed() {
    Client configurationClient = configurationClient();

    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "stream-authorized" + i;
              Client.Response response = configurationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              int messageCount = 1000;
              CountDownLatch publishConfirmLatch = new CountDownLatch(messageCount);
              AtomicInteger publishErrorCount = new AtomicInteger(0);
              Client client =
                  client(
                      new Client.ClientParameters()
                          .publishConfirmListener(
                              (publisherId, publishingId) -> publishConfirmLatch.countDown())
                          .publishErrorListener(
                              (publisherId, publishingId, errorCode) ->
                                  publishErrorCount.incrementAndGet()));

              IntStream.range(0, messageCount)
                  .forEach(
                      j ->
                          client.publish(
                              stream,
                              (byte) 1,
                              Collections.singletonList(
                                  client
                                      .messageBuilder()
                                      .addData("hello".getBytes(StandardCharsets.UTF_8))
                                      .build())));

              assertThat(await(publishConfirmLatch)).isTrue();
              assertThat(publishErrorCount.get()).isZero();

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void publishToUnauthorisedStreamShouldFail() {
    Client configurationClient = configurationClient();

    IntStream.range(0, 30)
        .forEach(
            i -> {
              String stream = "not-authorized" + i;
              Client.Response response = configurationClient.create(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

              int messageCount = 1000;
              CountDownLatch publishErrorLatch = new CountDownLatch(messageCount);
              AtomicInteger publishConfirmCount = new AtomicInteger(0);
              Client client =
                  client(
                      new Client.ClientParameters()
                          .publishConfirmListener(
                              (publisherId, publishingId) -> publishConfirmCount.incrementAndGet())
                          .publishErrorListener(
                              (publisherId, publishingId, errorCode) ->
                                  publishErrorLatch.countDown()));

              IntStream.range(0, messageCount)
                  .forEach(
                      j ->
                          client.publish(
                              stream,
                              (byte) 1,
                              Collections.singletonList(
                                  client
                                      .messageBuilder()
                                      .addData(("hello".getBytes(StandardCharsets.UTF_8)))
                                      .build())));

              assertThat(await(publishErrorLatch)).isTrue();
              assertThat(publishConfirmCount.get()).isZero();

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
            });
  }

  @Test
  void commitQueryOffsetShouldSucceedOnAuthorisedStreamShouldFailOnUnauthorisedStream()
      throws Exception {
    Client configurationClient = configurationClient();
    String s = "commit-not-always-authorized";
    try {
      assertThat(configurationClient.create(s).isOk()).isTrue();

      configurationClient.commitOffset("configuration", s, 10);

      Duration timeToCheckOffsetCommit =
          waitAtMost(5, () -> configurationClient.queryOffset("configuration", s) == 10);

      Client client = client();

      client.commitOffset("default-client", s, 10);

      // commit offset is fire-and-forget, let's wait a bit to make sure nothing is written
      Thread.sleep(timeToCheckOffsetCommit.toMillis() * 2);
      assertThat(configurationClient.queryOffset("default-client", s)).isNotEqualTo(10);

      // querying is not even authorised of for the default client, it should return 0
      assertThat(client.queryOffset("configuration", s)).isZero();

    } finally {
      assertThat(configurationClient.delete(s).isOk()).isTrue();
    }
  }

  Client configurationClient() {
    return cf.get(new Client.ClientParameters().virtualHost(VH));
  }

  Client client() {
    return client(new Client.ClientParameters());
  }

  Client client(Client.ClientParameters parameters) {
    return cf.get(parameters.virtualHost(VH).username(USERNAME).password(PASSWORD));
  }
}
