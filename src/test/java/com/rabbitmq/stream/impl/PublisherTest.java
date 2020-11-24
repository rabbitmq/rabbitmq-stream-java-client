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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class PublisherTest {

  String stream;
  TestUtils.ClientFactory cf;

  @ParameterizedTest
  @ValueSource(strings = {"publisher-reference", ""})
  void declarePublisher(String publisherReference) throws Exception {
    int messageCount = 10_000;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    CountDownLatch consumerLatch = new CountDownLatch(messageCount);
    Client c =
        cf.get(
            new ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown())
                .chunkListener(
                    (client, subscriptionId, offset, messageCount1, dataSize) ->
                        client.credit(subscriptionId, 1))
                .messageListener((subscriptionId, offset, message) -> consumerLatch.countDown()));

    Response response = c.declarePublisher((byte) 1, publisherReference, stream);
    assertThat(response.isOk()).isTrue();
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                c.publish(
                    stream,
                    (byte) 1,
                    Collections.singletonList(c.messageBuilder().addData("".getBytes()).build())));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
    response = c.deletePublisher((byte) 1);
    assertThat(response.isOk()).isTrue();

    response = c.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();
    assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();
  }

  static Stream<Arguments> deduplication() {
    return Stream.of(
        // publisher reference, message count, duplicated messages, expected consumed messages
        Arguments.arguments("ref1", 10_000, 1000, 10_000),
        Arguments.arguments(null, 10_000, 1000, 11_000),
        Arguments.arguments("", 10_000, 1000, 11_000));
  }

  @ParameterizedTest
  @MethodSource
  void deduplication(
      String publisherReference, int messageCount, int duplicatedCount, int expectedConsumed)
      throws Exception {
    CountDownLatch publishLatch = new CountDownLatch(messageCount + duplicatedCount);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    AtomicInteger consumeCount = new AtomicInteger();
    Client c =
        cf.get(
            new ClientParameters()
                .publishConfirmListener((pubId, publishingId) -> publishLatch.countDown())
                .chunkListener(
                    (client, subscriptionId, offset, messageCount1, dataSize) ->
                        client.credit(subscriptionId, 1))
                .messageListener(
                    (subscriptionId, offset, message) -> {
                      consumeCount.incrementAndGet();
                      consumeLatch.countDown();
                    }));

    Response response = c.declarePublisher((byte) 1, publisherReference, stream);
    assertThat(response.isOk()).isTrue();

    AtomicLong publishingSequence = new AtomicLong(0);
    LongSupplier publishingSequenceSupplier = () -> publishingSequence.incrementAndGet();

    IntStream.range(0, messageCount)
        .forEach(
            i ->
                c.publish(
                    stream,
                    (byte) 1,
                    Collections.singletonList(c.messageBuilder().addData("".getBytes()).build()),
                    publishingSequenceSupplier));

    publishingSequence.addAndGet(-duplicatedCount);
    IntStream.range(0, duplicatedCount)
        .forEach(
            i ->
                c.publish(
                    stream,
                    (byte) 1,
                    Collections.singletonList(c.messageBuilder().addData("".getBytes()).build()),
                    publishingSequenceSupplier));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
    response = c.deletePublisher((byte) 1);
    assertThat(response.isOk()).isTrue();

    response = c.subscribe((byte) 1, stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(1000L);
    assertThat(consumeCount.get()).isEqualTo(expectedConsumed);
  }

  // FIXME test publishers with same ID/reference
  // FIXME test on stream that does not exist
  // FIXME test on stream without the appropriate permissions (in AuthorisationTest)
  // FIXME test delete publisher that does not exist

}
