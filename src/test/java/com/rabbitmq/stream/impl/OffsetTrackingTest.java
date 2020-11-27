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

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.forEach;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamParametersBuilder;
import io.vavr.Tuple;
import io.vavr.Tuple3;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class OffsetTrackingTest {

  String stream;
  TestUtils.ClientFactory cf;

  @ParameterizedTest
  @CsvSource({
    "ref,ref,true,10,10,committed offset should be read",
    "ref,ref,true,-1,0,query offset should return 0 if not tracked offset for the reference",
    "ref,ref,false,-1,0,query offset should return 0 if stream does not exist",
    "ref,foo,false,-1,0,query offset should return 0 if stream does not exist",
  })
  void commitAndQuery(
      String commitReference,
      String queryReference,
      boolean streamExists,
      long committedOffset,
      long expectedOffset,
      String message)
      throws Exception {
    int messageCount = 500;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    Client client = cf.get();

    String s = streamExists ? this.stream : UUID.randomUUID().toString();
    if (committedOffset >= 0) {
      client.commitOffset(commitReference, s, committedOffset);
    }
    Thread.sleep(100L); // commit offset is fire-and-forget
    long offset = client.queryOffset(queryReference, s);
    assertThat(offset).as(message).isEqualTo(expectedOffset);
  }

  static Stream<BiConsumer<String, Client>> consumeAndCommit() {
    return Stream.of(
        (s, c) -> c.create(s),
        (s, c) -> c.create(s, new StreamParametersBuilder().maxSegmentSizeKb(100).build()));
  }

  @ParameterizedTest
  @MethodSource
  void consumeAndCommit(BiConsumer<String, Client> streamCreator) throws Exception {
    String s = UUID.randomUUID().toString();

    int batchSize = 100;
    int batchCount = 1_000;
    int messageCount = batchSize * batchCount;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);

    Client publisher =
        cf.get(
            new ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));

    ExecutorService executorService = Executors.newCachedThreadPool();
    try {
      streamCreator.accept(s, publisher);

      byte[] body = new byte[100];
      AtomicInteger messageIdSequence = new AtomicInteger();

      AtomicLong lastMessageId = new AtomicLong();
      publisher.declarePublisher(b(0), null, s);
      IntStream.range(0, batchCount)
          .forEach(
              batchIndex ->
                  publisher.publish(
                      b(0),
                      IntStream.range(0, batchSize)
                          .map(i -> messageIdSequence.incrementAndGet())
                          .mapToObj(
                              messageId -> {
                                lastMessageId.set(messageId);
                                return publisher
                                    .messageBuilder()
                                    .addData(body)
                                    .properties()
                                    .messageId(messageId)
                                    .messageBuilder()
                                    .build();
                              })
                          .collect(Collectors.toList())));

      boolean done = publishLatch.await(2, TimeUnit.SECONDS);
      assertThat(done).isTrue();

      Stream<Tuple3<Integer, Integer, String>> testConfigurations =
          Stream.of(
              // { commitEvery, consumeCountFirst, reference }
              Tuple.of(100, messageCount / 10, "ref-1"),
              Tuple.of(50, messageCount / 20, "ref-2"),
              Tuple.of(10, messageCount / 100, "ref-3"));

      Function<Tuple3<Integer, Integer, String>, Callable<Void>> testConfigurationToTask =
          testConfiguration ->
              () -> {
                int commitEvery = testConfiguration._1;
                int consumeCountFirst = testConfiguration._2;
                String reference = testConfiguration._3;

                AtomicInteger consumeCount = new AtomicInteger();
                AtomicLong lastCommittedOffset = new AtomicLong();
                AtomicLong lastConsumedMessageId = new AtomicLong();
                AtomicReference<Client> consumerReference = new AtomicReference<>();
                Set<Long> messageIdsSet = ConcurrentHashMap.newKeySet(messageCount);
                Collection<Long> messageIdsCollection = new ConcurrentLinkedQueue<>();
                CountDownLatch consumeLatch = new CountDownLatch(1);
                MessageListener messageListener =
                    (subscriptionId, offset, message) -> {
                      if (consumeCount.get() <= consumeCountFirst) {
                        consumeCount.incrementAndGet();
                        long messageId = message.getProperties().getMessageIdAsLong();
                        messageIdsSet.add(messageId);
                        messageIdsCollection.add(messageId);
                        lastConsumedMessageId.set(messageId);
                        if (consumeCount.get() % commitEvery == 0) {
                          consumerReference.get().commitOffset(reference, s, offset);
                          lastCommittedOffset.set(offset);
                        }
                      } else {
                        consumeLatch.countDown();
                      }
                    };
                Client consumer =
                    cf.get(
                        new ClientParameters()
                            .chunkListener(
                                (client, subscriptionId, offset, messageCount1, dataSize) ->
                                    client.credit(subscriptionId, 1))
                            .messageListener(messageListener));
                consumerReference.set(consumer);

                consumer.subscribe(b(0), s, OffsetSpecification.offset(0), 1);

                assertThat(consumeLatch.await(10, TimeUnit.SECONDS)).isTrue();
                Response response = consumer.unsubscribe(b(0));
                assertThat(response.isOk()).isTrue();

                assertThat(lastCommittedOffset.get()).isPositive();

                waitAtMost(
                    5,
                    () ->
                        lastCommittedOffset.get()
                            == consumerReference.get().queryOffset(reference, s),
                    () ->
                        "expecting last committed offset to be "
                            + lastCommittedOffset
                            + ", but got "
                            + consumerReference.get().queryOffset(reference, s));

                consumer.close();

                CountDownLatch consumeLatchSecondWave = new CountDownLatch(1);
                AtomicLong firstOffset = new AtomicLong(-1);

                messageListener =
                    (subscriptionId, offset, message) -> {
                      firstOffset.compareAndSet(-1, offset);
                      long messageId = message.getProperties().getMessageIdAsLong();
                      if (lastConsumedMessageId.get() < messageId) {

                        messageIdsSet.add(messageId);
                        messageIdsCollection.add(messageId);
                        consumeCount.incrementAndGet();

                        if (message.getProperties().getMessageIdAsLong() == lastMessageId.get()) {
                          consumeLatchSecondWave.countDown();
                        }
                      }
                    };

                consumer =
                    cf.get(
                        new ClientParameters()
                            .chunkListener(
                                (client, subscriptionId, offset, messageCount1, dataSize) ->
                                    client.credit(subscriptionId, 1))
                            .messageListener(messageListener));

                long offsetToStartFrom = consumer.queryOffset(reference, s) + 1;
                consumer.subscribe(b(0), s, OffsetSpecification.offset(offsetToStartFrom), 1);

                assertThat(consumeLatchSecondWave.await(10, TimeUnit.SECONDS)).isTrue();
                // there can be a non-message entry that is skipped and makes
                // the first received message offset higher
                assertThat(firstOffset.get()).isGreaterThanOrEqualTo(offsetToStartFrom);

                response = consumer.unsubscribe(b(0));
                assertThat(response.isOk()).isTrue();

                assertThat(consumeCount.get())
                    .as("check received all messages")
                    .isEqualTo(messageCount);
                assertThat(messageIdsCollection)
                    .as("check there are no duplicates")
                    .hasSameSizeAs(messageIdsSet);

                return null;
              };
      List<Future<Void>> futures =
          testConfigurations
              .map(testConfigurationToTask)
              .map(task -> executorService.submit(task))
              .collect(Collectors.toList());

      forEach(
          futures,
          (i, task) -> {
            assertThatNoException()
                .as("task " + i + " failed")
                .isThrownBy(() -> task.get(10, TimeUnit.SECONDS));
          });

    } finally {
      publisher.delete(s);
      executorService.shutdownNow();
    }
  }
}
