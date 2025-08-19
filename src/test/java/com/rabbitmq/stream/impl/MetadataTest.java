// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Cli;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@StreamTestInfrastructure
public class MetadataTest {

  TestUtils.ClientFactory cf;

  String stream;

  @ValueSource(ints = {1, 2, 3, 4, 5})
  @ParameterizedTest
  void metadataExistingStreams(int streamCount, TestInfo info) {
    Client streamClient = cf.get();
    String[] streams =
        IntStream.range(0, streamCount)
            .mapToObj(
                i -> {
                  String t = streamName(info);
                  streamClient.create(t);
                  return t;
                })
            .toArray(String[]::new);

    Client client = cf.get();
    Map<String, Client.StreamMetadata> metadata = client.metadata(streams);
    assertThat(metadata).hasSize(streamCount).containsKeys(streams);
    asList(streams)
        .forEach(
            t -> {
              Client.StreamMetadata streamMetadata = metadata.get(t);
              assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
              assertThat(streamMetadata.getStream()).isEqualTo(t);
              checkHost(streamMetadata.getLeader());
              assertThat(streamMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
              assertThat(streamMetadata.getReplicas()).isEmpty();
            });
    asList(streams).forEach(t -> streamClient.delete(t));
  }

  @Test
  void metadataOneNonExistingStream() {
    Client client = cf.get();
    String nonExistingStream = UUID.randomUUID().toString();
    Map<String, Client.StreamMetadata> metadata = client.metadata(nonExistingStream);
    assertThat(metadata).hasSize(1).containsKey(nonExistingStream);
    Client.StreamMetadata streamMetadata = metadata.get(nonExistingStream);
    assertThat(streamMetadata.getResponseCode())
        .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    assertThat(streamMetadata.getStream()).isEqualTo(nonExistingStream);
    assertThat(streamMetadata.getLeader()).isNull();
    assertThat(streamMetadata.getReplicas()).isEmpty();
  }

  @ParameterizedTest
  @CsvSource({
    "1,1", "2,1", "5,1", "1,2", "2,2", "5,2", "1,3", "2,3", "5,3",
  })
  void metadataExistingNonExistingStreams(int existingCount, int nonExistingCount, TestInfo info) {
    Client streamClient = cf.get();
    List<String> existingStreams =
        IntStream.range(0, existingCount)
            .mapToObj(
                i -> {
                  String t = streamName(info);
                  streamClient.create(t);
                  return t;
                })
            .collect(Collectors.toList());

    List<String> nonExistingStreams =
        IntStream.range(0, nonExistingCount)
            .mapToObj(i -> UUID.randomUUID().toString())
            .collect(Collectors.toList());

    List<String> allStreams = new ArrayList<>(existingCount + nonExistingCount);
    allStreams.addAll(existingStreams);
    allStreams.addAll(nonExistingStreams);
    Collections.shuffle(allStreams);

    String[] streams = allStreams.toArray(new String[] {});

    Client client = cf.get();
    Map<String, Client.StreamMetadata> metadata = client.metadata(streams);
    assertThat(metadata).hasSize(streams.length).containsKeys(streams);
    metadata
        .keySet()
        .forEach(
            t -> {
              Client.StreamMetadata streamMetadata = metadata.get(t);
              assertThat(streamMetadata.getStream()).isEqualTo(t);
              if (existingStreams.contains(t)) {
                assertThat(streamMetadata.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
                checkHost(streamMetadata.getLeader());
                assertThat(streamMetadata.getLeader().getPort()).isEqualTo(Client.DEFAULT_PORT);
                assertThat(streamMetadata.getReplicas()).isEmpty();
              } else {
                assertThat(streamMetadata.getResponseCode())
                    .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
                assertThat(streamMetadata.getStream()).isEqualTo(t);
                assertThat(streamMetadata.getLeader()).isNull();
                assertThat(streamMetadata.getReplicas()).isEmpty();
              }
            });
    existingStreams.forEach(t -> streamClient.delete(t));
  }

  static void checkHost(Broker broker) {
    if (!Cli.isOnDocker()) {
      assertThat(broker.getHost()).isEqualTo(hostname());
    }
  }

  static String hostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      return Cli.hostname();
    }
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11_7)
  void shouldFilterOutNodesInMaintenance() throws Exception {
    Client client = cf.get();
    BooleanSupplier hasLeader = () -> client.metadata(stream).get(stream).getLeader() != null;
    waitAtMost(() -> hasLeader.getAsBoolean());
    Cli.rabbitmqctl("eval 'rabbit_maintenance:drain().'");
    waitAtMost(() -> !hasLeader.getAsBoolean());
    Cli.rabbitmqctl("eval 'rabbit_maintenance:revive().'");
    waitAtMost(() -> hasLeader.getAsBoolean());
  }
}
