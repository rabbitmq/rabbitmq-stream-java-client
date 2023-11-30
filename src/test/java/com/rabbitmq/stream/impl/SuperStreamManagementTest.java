// Copyright (c) 2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc.
// and/or its subsidiaries.
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
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.*;
import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
@TestUtils.DisabledIfRabbitMqCtlNotSet
public class SuperStreamManagementTest {

  TestUtils.ClientFactory cf;
  static final int partitionCount = 3;
  String s;
  List<String> partitions;
  List<String> bindingKeys;

  @BeforeEach
  void init(TestInfo info) {
    s = streamName(info);
    partitions = partitions(s);
    bindingKeys = bindingKeys();
  }

  @Test
  @TestUtils.BrokerVersionAtLeast(TestUtils.BrokerVersion.RABBITMQ_3_13_0)
  void createDelete() {
    Client c = cf.get();
    Client.Response response = c.createSuperStream(s, partitions, bindingKeys, null);
    assertThat(response).is(ok());
    assertThat(c.metadata(partitions))
        .hasSameSizeAs(partitions)
        .allSatisfy((s, streamMetadata) -> assertThat(streamMetadata.isResponseOk()).isTrue());
    assertThat(c.partitions(s)).isEqualTo(partitions);
    bindingKeys.forEach(bk -> assertThat(c.route(bk, s)).hasSize(1).contains(s + "-" + bk));

    response = c.createSuperStream(s, partitions, bindingKeys, null);
    assertThat(response).is(ko()).is(responseCode(RESPONSE_CODE_STREAM_ALREADY_EXISTS));

    response = c.deleteSuperStream(s);
    assertThat(response).is(ok());
    assertThat(c.metadata(partitions))
        .hasSameSizeAs(partitions)
        .allSatisfy(
            (s, streamMetadata) ->
                assertThat(streamMetadata.getResponseCode())
                    .isEqualTo(RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
    assertThat(c.partitions(s)).isEmpty();
    bindingKeys.forEach(bk -> assertThat(c.route(bk, s)).isEmpty());

    response = c.deleteSuperStream(s);
    assertThat(response).is(responseCode(RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
  }

  @Test
  @TestUtils.BrokerVersionAtLeast(TestUtils.BrokerVersion.RABBITMQ_3_13_0)
  void clientWithSubscriptionShouldReceiveNotificationOnDeletion() throws Exception {
    Client c = cf.get();
    Client.Response response = c.createSuperStream(s, partitions, bindingKeys, null);
    assertThat(response).is(ok());
    Map<String, Short> notifications = new ConcurrentHashMap<>(partitions.size());
    AtomicInteger notificationCount = new AtomicInteger();
    partitions.forEach(
        p -> {
          Client client =
              cf.get(
                  new Client.ClientParameters()
                      .metadataListener(
                          (stream, code) -> {
                            notifications.put(stream, code);
                            notificationCount.incrementAndGet();
                          }));
          Client.Response r = client.subscribe((byte) 0, p, OffsetSpecification.first(), 1);
          assertThat(r).is(ok());
        });

    response = c.deleteSuperStream(s);
    assertThat(response).is(ok());
    waitAtMost(() -> notificationCount.get() == partitionCount);
    assertThat(notifications).hasSize(partitionCount).containsOnlyKeys(partitions);
    assertThat(notifications.values()).containsOnly(RESPONSE_CODE_STREAM_NOT_AVAILABLE);
  }

  @Test
  @TestUtils.BrokerVersionAtLeast(TestUtils.BrokerVersion.RABBITMQ_3_13_0)
  void authorisation() throws Exception {
    String user = "stream";
    // binding keys do not matter for authorisation
    bindingKeys = asList("1", "2", "3");
    try {
      addUser(user, user);
      setPermissions(user, asList("stream|partition.*$", "partition.*$", "stream.*$"));
      Client c = cf.get(new Client.ClientParameters().username(user).password(user));
      Client.Response response = c.createSuperStream("not-allowed", partitions, bindingKeys, null);
      assertThat(response).is(ko()).is(responseCode(RESPONSE_CODE_ACCESS_REFUSED));

      s = name("stream");
      response = c.createSuperStream(s, asList("1", "2", "3"), bindingKeys, null);
      assertThat(response).is(ko()).is(responseCode(RESPONSE_CODE_ACCESS_REFUSED));

      partitions = range(0, partitionCount).mapToObj(i -> s + "-" + i).collect(toList());
      // we can create the queues, but can't bind them, as it requires write permission
      response = c.createSuperStream(s, partitions, bindingKeys, null);
      assertThat(response).is(ko()).is(responseCode(RESPONSE_CODE_ACCESS_REFUSED));

      String partitionName = name("partition");
      partitions =
          range(0, partitionCount).mapToObj(i -> partitionName + "-" + i).collect(toList());
      response = c.createSuperStream(s, partitions, bindingKeys, null);
      assertThat(response).is(ok());

      assertThat(c.metadata(partitions))
          .hasSameSizeAs(partitions)
          .allSatisfy((s, streamMetadata) -> assertThat(streamMetadata.isResponseOk()).isTrue());
      assertThat(c.partitions(s)).isEqualTo(partitions);
      for (int i = 0; i < bindingKeys.size(); i++) {
        String bk = bindingKeys.get(i);
        assertThat(c.route(bk, s)).hasSize(1).contains(partitions.get(i));
      }

      response = c.deleteSuperStream(s);
      assertThat(response).is(ok());
    } finally {
      deleteUser(user);
    }
  }

  private static List<String> bindingKeys() {
    return bindingKeys(partitionCount);
  }

  private static List<String> bindingKeys(int partitions) {
    return range(0, partitions).mapToObj(String::valueOf).collect(toList());
  }

  private static List<String> partitions(String superStream) {
    return partitions(superStream, partitionCount);
  }

  private static List<String> partitions(String superStream, int partitions) {
    return range(0, partitions).mapToObj(i -> superStream + "-" + i).collect(toList());
  }

  private static String name(String value) {
    String uuid = UUID.randomUUID().toString();
    return String.format(
        "%s-%s%s",
        SuperStreamManagementTest.class.getSimpleName(), value, uuid.substring(uuid.length() / 2));
  }
}
