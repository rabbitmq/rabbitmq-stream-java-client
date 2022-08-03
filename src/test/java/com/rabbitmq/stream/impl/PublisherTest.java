// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class PublisherTest {

  String stream;
  TestUtils.ClientFactory cf;

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"publisher-reference"})
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
                .messageListener(
                    (subscriptionId, offset, chunkTimestamp, committedOffset, message) ->
                        consumerLatch.countDown()));

    Response response = c.declarePublisher(b(1), publisherReference, stream);
    assertThat(response.isOk()).isTrue();
    c.declarePublisher(b(1), null, stream);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                c.publish(
                    b(1),
                    Collections.singletonList(c.messageBuilder().addData("".getBytes()).build())));

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
    response = c.deletePublisher(b(1));
    assertThat(response.isOk()).isTrue();

    response = c.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();
    assertThat(consumerLatch.await(10, TimeUnit.SECONDS)).isTrue();
  }

  static Stream<Arguments> declarePublisherUniqueness() {
    return Stream.of(
        Arguments.of(b(1), "ref-1", "s1", b(2), "ref-2", "s2", true), // ok
        Arguments.of(b(1), "ref-1", "s1", b(2), "ref-1", "s1", false), // same ref + stream
        Arguments.of(b(1), "ref-1", "s1", b(1), "ref-1", "s2", false), // same producer ID
        Arguments.of(b(1), "ref-1", "s1", b(1), "ref-2", "s2", false), // same producer ID
        Arguments.of(b(1), null, "s1", b(2), null, "s2", true), // no ref
        Arguments.of(b(1), "ref-1", "s1", b(2), null, "s1", true), // same stream, ref + no ref
        Arguments.of(b(1), null, "s1", b(1), null, "s1", false) // same producer ID, no ref
        );
  }

  @ParameterizedTest
  @MethodSource
  void declarePublisherUniqueness(
      byte pubId1,
      String ref1,
      String s1,
      byte pubId2,
      String ref2,
      String s2,
      boolean isOk,
      TestInfo info) {
    Client c = cf.get();
    String prefix = UUID.randomUUID().toString();
    s1 = info.getTestMethod().get().getName() + "-" + prefix + "-" + s1;
    s2 = info.getTestMethod().get().getName() + "-" + prefix + "-" + s2;
    try {
      assertThat(c.create(s1).isOk()).isTrue();
      if (!s1.equals(s2)) {
        assertThat(c.create(s2).isOk()).isTrue();
      }
      assertThat(c.declarePublisher(pubId1, ref1, s1).isOk()).isTrue();
      assertThat(c.declarePublisher(pubId2, ref2, s2).isOk()).isEqualTo(isOk);
    } finally {
      assertThat(c.delete(s1).isOk()).isTrue();
      if (!s1.equals(s2)) {
        assertThat(c.delete(s2).isOk()).isTrue();
      }
    }
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"ref-1"})
  void declarePublisherOnStreamThatDoesNotExistShouldReturnError(String reference) {
    String s = UUID.randomUUID().toString();
    Response response = cf.get().declarePublisher(b(1), reference, s);
    assertThat(response.isOk()).isFalse();
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
  }

  @Test
  void deleteNonExistingPublisherShouldReturnError() {
    Response response = cf.get().deletePublisher(b(42));
    assertThat(response.isOk()).isFalse();
    assertThat(response.getResponseCode())
        .isEqualTo(Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST);
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
                    (subscriptionId, offset, chunkTimestamp, committedOffset, message) -> {
                      consumeCount.incrementAndGet();
                      consumeLatch.countDown();
                    }));

    Response response = c.declarePublisher(b(1), publisherReference, stream);
    assertThat(response.isOk()).isTrue();

    AtomicLong publishingSequence = new AtomicLong(0);
    ToLongFunction<Object> publishingSequenceFunction = o -> publishingSequence.incrementAndGet();

    c.declarePublisher(b(1), null, stream);
    IntConsumer publishing =
        i ->
            c.publish(
                b(1),
                Collections.singletonList(c.messageBuilder().addData("".getBytes()).build()),
                publishingSequenceFunction);
    IntStream.range(0, messageCount).forEach(publishing);

    publishingSequence.addAndGet(-duplicatedCount);
    IntStream.range(0, duplicatedCount).forEach(publishing);

    assertThat(publishLatch.await(10, TimeUnit.SECONDS)).isTrue();
    response = c.deletePublisher(b(1));
    assertThat(response.isOk()).isTrue();

    response = c.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(response.isOk()).isTrue();
    assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(1000L);
    assertThat(consumeCount.get()).isEqualTo(expectedConsumed);
  }

  @Test
  void publishToNonExistingPublisherTriggersPublishErrorListener() throws Exception {
    int messageCount = 1000;
    AtomicInteger confirms = new AtomicInteger(0);
    Set<Short> responseCodes = ConcurrentHashMap.newKeySet(1);
    Set<Long> publishingIdErrors = ConcurrentHashMap.newKeySet(messageCount);
    CountDownLatch latch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> confirms.incrementAndGet())
                .publishErrorListener(
                    (publisherId, publishingId, responseCode) -> {
                      publishingIdErrors.add(publishingId);
                      responseCodes.add(responseCode);
                      latch.countDown();
                    }));

    Set<Long> publishingIds = ConcurrentHashMap.newKeySet(messageCount);

    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publishingIds.addAll(
                    client.publish(
                        b(1),
                        Collections.singletonList(
                            client.messageBuilder().addData(("" + i).getBytes()).build()))));

    assertThat(latch.await(10, SECONDS)).isTrue();
    assertThat(confirms.get()).isZero();
    assertThat(responseCodes).hasSize(1).contains(Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST);
    assertThat(publishingIdErrors)
        .hasSameSizeAs(publishingIds)
        .hasSameElementsAs(publishingIdErrors);
  }

  @Test
  void queryPublisherSequence() throws Exception {
    String publisherReference = UUID.randomUUID().toString();
    int messageCount = 10_000;
    int duplicatedCount = messageCount / 10;
    AtomicReference<CountDownLatch> publishLatch = new AtomicReference<>();
    Client c =
        cf.get(
            new ClientParameters()
                .publishConfirmListener((pubId, publishingId) -> publishLatch.get().countDown()));

    Response response = c.declarePublisher(b(1), publisherReference, stream);
    assertThat(response.isOk()).isTrue();

    AtomicLong publishingSequence = new AtomicLong(0);
    ToLongFunction<Object> publishingSequenceFunction = o -> publishingSequence.incrementAndGet();

    assertThat(c.queryPublisherSequence(publisherReference, stream)).isEqualTo(0);

    publishLatch.set(new CountDownLatch(messageCount));
    IntConsumer publishing =
        i ->
            c.publish(
                b(1),
                Collections.singletonList(c.messageBuilder().addData("".getBytes()).build()),
                publishingSequenceFunction);
    IntStream.range(0, messageCount).forEach(publishing);

    assertThat(publishLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(c.queryPublisherSequence(publisherReference, stream))
        .isEqualTo(publishingSequence.get());

    long previousSequenceValue = publishingSequence.get();
    publishLatch.set(new CountDownLatch(duplicatedCount));
    publishingSequence.addAndGet(-duplicatedCount);
    IntStream.range(0, duplicatedCount).forEach(publishing);
    assertThat(publishLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(c.queryPublisherSequence(publisherReference, stream))
        .isEqualTo(previousSequenceValue);

    publishLatch.set(new CountDownLatch(messageCount));
    IntStream.range(0, messageCount).forEach(publishing);
    assertThat(publishLatch.get().await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(c.queryPublisherSequence(publisherReference, stream))
        .isEqualTo(publishingSequence.get());
  }

  @Test
  void queryPublisherSequenceForNonExistingPublisherShouldReturnZero() {
    Client c = cf.get();
    assertThat(c.queryPublisherSequence(UUID.randomUUID().toString(), stream)).isZero();
  }

  @Test
  void queryPublisherSequenceForNonExistingStreamShouldReturnZero() {
    Client c = cf.get();
    assertThat(c.queryPublisherSequence("foo", UUID.randomUUID().toString())).isZero();
  }
}
