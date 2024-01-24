// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.Constants.*;
import static com.rabbitmq.stream.Host.*;
import static com.rabbitmq.stream.OffsetSpecification.first;
import static com.rabbitmq.stream.impl.TestUtils.*;
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.ok;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.DefaultUsernamePasswordCredentialsProvider;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
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
    addVhost(VH);
    addUser(USERNAME, PASSWORD);
    setPermissions(USERNAME, VH, "^stream.*$");
    setPermissions("guest", VH, ".*");
  }

  @AfterAll
  static void tearDown() throws Exception {
    deleteUser(USERNAME);
    deleteVhost(VH);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = deletionClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = client.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = client.delete(stream);
              assertThat(response.isOk()).isFalse();
              assertThat(response.getResponseCode())
                  .isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);

              response = creationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = client.subscribe(b(1), stream, first(), 10);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

              response = client.subscribe(b(1), stream, first(), 10);
              assertThat(response.isOk()).isFalse();
              assertThat(response.getResponseCode())
                  .isEqualTo(Constants.RESPONSE_CODE_ACCESS_REFUSED);

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

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

              assertThat(client.declarePublisher(b(1), null, stream).isOk()).isTrue();
              client.declarePublisher(b(1), null, stream);
              IntStream.range(0, messageCount)
                  .forEach(
                      j ->
                          client.publish(
                              b(1),
                              Collections.singletonList(
                                  client
                                      .messageBuilder()
                                      .addData("hello".getBytes(StandardCharsets.UTF_8))
                                      .build())));

              assertThat(await(publishConfirmLatch)).isTrue();
              assertThat(publishErrorCount.get()).isZero();

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
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
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);

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

              assertThat(client.declarePublisher(b(1), null, stream).isOk()).isFalse();

              client.declarePublisher(b(1), null, stream);
              IntStream.range(0, messageCount)
                  .forEach(
                      j ->
                          client.publish(
                              b(1),
                              Collections.singletonList(
                                  client
                                      .messageBuilder()
                                      .addData(("hello".getBytes(StandardCharsets.UTF_8)))
                                      .build())));

              assertThat(await(publishErrorLatch)).isTrue();
              assertThat(publishConfirmCount.get()).isZero();

              response = configurationClient.delete(stream);
              assertThat(response.isOk()).isTrue();
              assertThat(response.getResponseCode()).isEqualTo(RESPONSE_CODE_OK);
            });
  }

  @Test
  void storeQueryOffsetShouldSucceedOnAuthorisedStreamShouldFailOnUnauthorisedStream()
      throws Exception {
    Client configurationClient = configurationClient();
    String s = "store-not-always-authorized";
    try {
      assertThat(configurationClient.create(s).isOk()).isTrue();

      configurationClient.storeOffset("configuration", s, 10);

      Duration timeToCheckOffsetTracking =
          waitAtMost(
              5, () -> configurationClient.queryOffset("configuration", s).getOffset() == 10);

      Client client = client();

      client.storeOffset("default-client", s, 10);

      // store offset is fire-and-forget, let's wait a bit to make sure nothing is written
      Thread.sleep(timeToCheckOffsetTracking.toMillis() * 2);
      assertThat(configurationClient.queryOffset("default-client", s)).isNotEqualTo(10);

      // querying is not even authorised for the default client, it should return 0
      assertThat(client.queryOffset("configuration", s).getOffset()).isZero();

    } finally {
      assertThat(configurationClient.delete(s).isOk()).isTrue();
    }
  }

  @Test
  @TestUtils.BrokerVersionAtLeast(TestUtils.BrokerVersion.RABBITMQ_3_13_0)
  void shouldReceiveMetadataUpdateAfterUpdateSecret(TestInfo info) throws Exception {
    try {
      String newPassword = "new-password";
      String prefix = "passthrough-";
      String pubSub = TestUtils.streamName(info);
      String authorizedPubSub = prefix + TestUtils.streamName(info);
      String pub = TestUtils.streamName(info);
      String authorizedPub = prefix + TestUtils.streamName(info);
      String sub = TestUtils.streamName(info);
      String authorizedSub = prefix + TestUtils.streamName(info);
      setPermissions(USERNAME, VH, ".*");
      Set<String> metadataUpdates = ConcurrentHashMap.newKeySet();
      ConcurrentMap<Byte, Short> publishConfirms = new ConcurrentHashMap<>();
      ConcurrentMap<Byte, Short> creditNotifications = new ConcurrentHashMap<>();
      Set<Byte> receivedMessages = ConcurrentHashMap.newKeySet();
      Client client =
          cf.get(
              parameters()
                  .virtualHost(VH)
                  .username(USERNAME)
                  .password(USERNAME)
                  .publishConfirmListener(
                      (publisherId, publishingId) ->
                          publishConfirms.put(publisherId, RESPONSE_CODE_OK))
                  .publishErrorListener(
                      (publisherId, publishingId, errorCode) ->
                          publishConfirms.put(publisherId, errorCode))
                  .creditNotification(
                      (subscriptionId, responseCode) ->
                          creditNotifications.put(subscriptionId, responseCode))
                  .messageListener(
                      (subscriptionId,
                          offset,
                          chunkTimestamp,
                          committedChunkId,
                          chunkContext,
                          message) -> receivedMessages.add(subscriptionId))
                  .metadataListener((stream, code) -> metadataUpdates.add(stream)));
      assertThat(client.create(pubSub)).is(ok());
      assertThat(client.create(authorizedPubSub)).is(ok());
      assertThat(client.create(pub)).is(ok());
      assertThat(client.create(authorizedPub)).is(ok());
      assertThat(client.create(sub)).is(ok());
      assertThat(client.create(authorizedSub)).is(ok());

      Map<String, Byte> publishers = new HashMap<>();
      publishers.put(pubSub, b(0));
      publishers.put(authorizedPubSub, b(1));
      publishers.put(pub, b(2));
      publishers.put(authorizedPub, b(3));
      publishers.forEach((s, id) -> assertThat(client.declarePublisher(id, null, s)).is(ok()));
      Map<String, Byte> subscriptions = new HashMap<>();
      subscriptions.put(pubSub, b(0));
      subscriptions.put(authorizedPubSub, b(1));
      subscriptions.put(sub, b(2));
      subscriptions.put(authorizedSub, b(3));
      subscriptions.forEach((s, id) -> assertThat(client.subscribe(id, s, first(), 1)).is(ok()));

      Function<String, Byte> toPub = publishers::get;
      Function<String, Byte> toSub = subscriptions::get;

      // change password and permissions and re-authenticate
      changePassword(USERNAME, newPassword);
      setPermissions(USERNAME, VH, "^passthrough.*$");
      client.authenticate(credentialsProvider(USERNAME, newPassword));

      waitAtMost(() -> metadataUpdates.containsAll(asList(pubSub, pub, sub)));

      List<Message> message = Collections.singletonList(client.messageBuilder().build());

      // publishers for unauthorized streams should be gone
      asList(toPub.apply(pubSub), toPub.apply(pub))
          .forEach(
              wrap(
                  pubId -> {
                    assertThat(publishConfirms).doesNotContainKey(pubId);
                    client.publish(pubId, message);
                    waitAtMost(() -> publishConfirms.containsKey(pubId));
                    assertThat(publishConfirms)
                        .containsEntry(pubId, RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST);
                  }));

      // subscriptions for unauthorized streams should be gone
      asList(toSub.apply(pubSub), toSub.apply(sub))
          .forEach(
              wrap(
                  subId -> {
                    assertThat(creditNotifications).doesNotContainKey(subId);
                    client.credit(subId, 1);
                    waitAtMost(() -> creditNotifications.containsKey(subId));
                    assertThat(creditNotifications)
                        .containsEntry(subId, RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST);
                  }));

      // subscriptions for authorized streams still work
      asList(toSub.apply(authorizedPubSub), toSub.apply(authorizedSub))
          .forEach(subId -> client.credit(subId, 1));

      assertThat(receivedMessages).isEmpty();
      // publishers for authorized streams should still work
      asList(toPub.apply(authorizedPubSub), toPub.apply(authorizedPub))
          .forEach(
              wrap(
                  pubId -> {
                    client.publish(pubId, message);
                    waitAtMost(() -> publishConfirms.containsKey(pubId));
                    assertThat(publishConfirms).containsEntry(pubId, RESPONSE_CODE_OK);
                  }));

      waitAtMost(() -> receivedMessages.contains(b(1)));

      // send message to authorized subscription stream
      assertThat(client.declarePublisher(b(5), null, authorizedSub)).is(ok());
      client.publish(b(5), message);
      waitAtMost(() -> receivedMessages.contains(toSub.apply(authorizedSub)));

      // last checks to make sure nothing unexpected arrived late
      assertThat(metadataUpdates).hasSize(3);
      assertThat(creditNotifications).containsOnlyKeys(b(0), b(2));
      assertThat(publishConfirms)
          .hasSize(4 + 1)
          .containsEntry(toPub.apply(pubSub), RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST)
          .containsEntry(toPub.apply(authorizedPubSub), RESPONSE_CODE_OK)
          .containsEntry(toPub.apply(pub), RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST)
          .containsEntry(toPub.apply(authorizedPub), RESPONSE_CODE_OK);
      assertThat(receivedMessages).hasSize(2);

      client.close();
    } finally {
      changePassword(USERNAME, PASSWORD);
      setPermissions(USERNAME, VH, "^stream.*$");
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

  private static Client.ClientParameters parameters() {
    return new Client.ClientParameters();
  }

  private static CredentialsProvider credentialsProvider(String username, String password) {
    return new DefaultUsernamePasswordCredentialsProvider(username, password);
  }
}
