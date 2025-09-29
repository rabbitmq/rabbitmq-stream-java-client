// Copyright (c) 2025 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.latchAssert;
import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static com.rabbitmq.stream.impl.TestUtils.waitUntilStable;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.EnvironmentBuilder;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.SubscriptionListener;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@StreamTestInfrastructure
public class DedupIdempotencyTest {

  String stream;
  Environment environment;
  EventLoopGroup eventLoopGroup;

  @BeforeEach
  void init() {
    EnvironmentBuilder environmentBuilder =
        Environment.builder().netty().eventLoopGroup(eventLoopGroup).environmentBuilder();
    this.environment = environmentBuilder.build();
  }

  @AfterEach
  void tearDown() {
    this.environment.close();
  }

  @Test
  void publishBatchWithDedup() {
    Producer producer =
        environment.producerBuilder().name("app1").confirmTimeout(Duration.ZERO).stream(stream)
            .build();

    // message ID, must be a strictly increasing sequence
    AtomicLong sequence = new AtomicLong();
    int batchSize = 50;
    // batch of messages, publishing ID and message ID set
    // publishing ID is mandatory, the message ID can be set
    // anywhere in the message (e.g. in application properties)
    List<Message> batch =
        IntStream.range(0, batchSize)
            .mapToObj(
                ignored -> {
                  long id = sequence.getAndIncrement();
                  return producer
                      .messageBuilder()
                      .publishingId(id)
                      .properties()
                      .messageId(id)
                      .messageBuilder()
                      .build();
                })
            .collect(toList());

    boolean batchSentAndConfirmed = false;
    int attempts = 0;
    // publishing the batch of messages
    // we can retry as many times as we want,
    // the broker takes care of deduplicating messages
    // it is not suboptimal to retry the whole batch: failures are rare
    while (!batchSentAndConfirmed && attempts < 5) {
      attempts++;
      try {
        batchSentAndConfirmed = publishBatch(producer, batch);
      } catch (Exception e) {
        // keep retrying...
      }
    }
    assertThat(batchSentAndConfirmed).isTrue();

    assertThat(streamMessageCount(stream, environment)).isEqualTo(batchSize);

    // just checking dedup
    batchSentAndConfirmed = publishBatch(producer, batch);
    assertThat(batchSentAndConfirmed).isTrue();
    assertThat(streamMessageCount(stream, environment)).isEqualTo(batchSize);
  }

  @Test
  void externalOffsetTrackingIdempotentProcessing() throws Exception {
    AtomicLong sequence = new AtomicLong();
    Producer producer = environment.producerBuilder().stream(stream).build();
    int messageCount = 50;
    // a batch of messages to publish
    // we don't use dedup to demonstrate the consumer idempotency
    // (we'll publish the batch several times)
    List<Message> messages =
        IntStream.range(0, messageCount)
            .mapToObj(
                ignored ->
                    producer
                        .messageBuilder()
                        .properties()
                        .messageId(sequence.getAndIncrement())
                        .messageBuilder()
                        .build())
            .collect(Collectors.toList());
    Runnable publish =
        () -> {
          CountDownLatch latch = new CountDownLatch(messageCount);
          messages.forEach(msg -> producer.send(msg, s -> latch.countDown()));
          assertThat(latchAssert(latch)).completes();
        };
    publish.run();

    // data structures for storage
    // we assume they can be updated atomically, like tables in a relational database
    // this tracks processed messages
    Set<Long> processed = ConcurrentHashMap.newKeySet(messageCount);
    // this simulates the actual business processing
    List<Long> database = Collections.synchronizedList(new ArrayList<>(messageCount));
    // this is for offset tracking
    AtomicLong lastProcessedOffset = new AtomicLong(-1);

    // tracks the number of received messages
    AtomicInteger messageReceivedCount = new AtomicInteger();

    MessageHandler handler =
        (ctx, msg) -> {
          long id = msg.getProperties().getMessageIdAsLong();
          // this is where a database transaction should start
          // checking whether the message has been processed
          if (processed.add(id)) {
            // not processed yet, processing
            database.add(id);
          }
          lastProcessedOffset.set(ctx.offset());
          // this is where the database transaction should commit
          // increments the counter, just for the test assertions
          messageReceivedCount.incrementAndGet();
        };

    // for test assertions
    AtomicLong subscriptionStart = new AtomicLong();
    // for the external offset tracking:
    SubscriptionListener subscriptionListener =
        ctx -> {
          // we hit the data structure that tracks the last processed
          // to restart where we left off
          long offset = lastProcessedOffset.get() == -1 ? 0 : lastProcessedOffset.get() + 1;
          ctx.offsetSpecification(OffsetSpecification.offset(offset));
          subscriptionStart.set(offset);
        };

    Supplier<Consumer> consumerSupplier =
        () ->
            environment.consumerBuilder().stream(stream)
                .messageHandler(handler)
                .subscriptionListener(subscriptionListener)
                .build();

    // we start the consumer, there is already a batch of messages in the stream
    Consumer consumer = consumerSupplier.get();

    // make sure we started at the beginning of the stream
    assertThat(subscriptionStart).hasValue(0);

    // we got all the messages
    waitAtMost(() -> messageReceivedCount.get() == messageCount);
    assertThat(processed).hasSize(messageCount);
    assertThat(database).hasSize(messageCount);

    // we publish the batch again, this simulates duplicates
    publish.run();

    // the counter counts all the messages
    waitAtMost(() -> messageReceivedCount.get() == 2 * messageCount);
    // no duplicated processing
    assertThat(processed).hasSize(messageCount);
    assertThat(database).hasSize(messageCount);

    consumer.close();

    // we restart the consumer
    consumer = consumerSupplier.get();
    // make sure we started where we left off
    assertThat(subscriptionStart).hasPositiveValue().hasValue(lastProcessedOffset.get() + 1);

    // position before we publish the batch a 3rd time
    long previousProcessedOffset = lastProcessedOffset.get();

    // publish the batch again, simulating duplicates
    publish.run();

    // the counter counted the 3 batches
    waitAtMost(() -> messageReceivedCount.get() == 3 * messageCount);
    // no duplicated processing though
    assertThat(processed).hasSize(messageCount);
    assertThat(database).hasSize(messageCount);
    // the offset position moved forward
    assertThat(lastProcessedOffset).hasValueGreaterThan(previousProcessedOffset);

    consumer.close();
  }

  private static boolean publishBatch(Producer producer, List<Message> batch) {
    CountDownLatch batchLatch = new CountDownLatch(batch.size());
    ConfirmationHandler confirmationHandler = status -> batchLatch.countDown();

    for (Message message : batch) {
      producer.send(message, confirmationHandler);
    }

    try {
      return batchLatch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      return false;
    }
  }

  private static int streamMessageCount(String stream, Environment env) {
    AtomicInteger messageReceived = new AtomicInteger();
    Consumer consumer =
        env.consumerBuilder().stream(stream)
            .offset(OffsetSpecification.first())
            .messageHandler(
                (ctx, msg) -> {
                  messageReceived.incrementAndGet();
                })
            .build();

    waitUntilStable(messageReceived::longValue);
    consumer.close();
    return messageReceived.get();
  }
}
