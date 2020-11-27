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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.TestUtils.CallableConsumer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class RetentionClientTest {

  static int messageCount = 1000;
  static int payloadSize = 1000;

  TestUtils.ClientFactory cf;

  static RetentionTestConfig[] retention() {
    return new RetentionTestConfig[] {
      new RetentionTestConfig(
          "with size as bytes",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            client.create(
                stream,
                new Client.StreamParametersBuilder()
                    .maxLengthBytes(messageCount * payloadSize / 10)
                    .maxSegmentSizeBytes(messageCount * payloadSize / 20)
                    .build());
          },
          firstMessageId -> firstMessageId > 0),
      new RetentionTestConfig(
          "with size helper to specify bytes",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            client.create(
                stream,
                new Client.StreamParametersBuilder()
                    .maxLengthBytes(ByteCapacity.B(messageCount * payloadSize / 10))
                    .maxSegmentSizeBytes(ByteCapacity.B(messageCount * payloadSize / 20))
                    .build());
          },
          firstMessageId -> firstMessageId > 0),
      new RetentionTestConfig(
          "with max age",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            client.create(
                stream,
                new Client.StreamParametersBuilder()
                    .maxAge(Duration.ofSeconds(2))
                    .maxSegmentSizeBytes(ByteCapacity.B(messageCount * payloadSize / 20))
                    .build());
          },
          firstMessageId -> firstMessageId > 0,
          Duration.ofSeconds(3)),
      new RetentionTestConfig(
          "no retention",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            client.create(stream, Collections.emptyMap());
          },
          firstMessageId -> firstMessageId == 0),
      new RetentionTestConfig(
          "with AMQP client, no retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection();
                Channel ch = c.createChannel()) {
              ch.queueDeclare(
                  stream, true, false, false, Collections.singletonMap("x-queue-type", "stream"));
            }
          },
          firstMessageId -> firstMessageId == 0),
      new RetentionTestConfig(
          "with AMQP client, with size-based retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection();
                Channel ch = c.createChannel()) {
              Map<String, Object> arguments = new HashMap<>();
              arguments.put("x-queue-type", "stream");
              arguments.put("x-max-length-bytes", messageCount * payloadSize / 10);
              arguments.put("x-max-segment-size", messageCount * payloadSize / 20);
              ch.queueDeclare(stream, true, false, false, arguments);
            }
          },
          firstMessageId -> firstMessageId > 0),
      new RetentionTestConfig(
          "with AMQP client, with age-based retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection();
                Channel ch = c.createChannel()) {
              Map<String, Object> arguments = new HashMap<>();
              arguments.put("x-queue-type", "stream");
              arguments.put("x-max-age", "2s");
              arguments.put("x-max-segment-size", messageCount * payloadSize / 20);
              ch.queueDeclare(stream, true, false, false, arguments);
            }
          },
          firstMessageId -> firstMessageId > 0,
          Duration.ofSeconds(3)),
    };
  }

  @ParameterizedTest
  @MethodSource
  void retention(RetentionTestConfig configuration) throws Exception {
    String testStream = UUID.randomUUID().toString();
    CountDownLatch publishingLatch = new CountDownLatch(messageCount);
    CountDownLatch publishingLatchSecondWave = new CountDownLatch(messageCount * 2);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener(
                    (publisherId, publishingId) -> {
                      publishingLatch.countDown();
                      publishingLatchSecondWave.countDown();
                    }));

    try {
      configuration.streamCreator.accept(new Object[] {publisher, testStream});
      AtomicLong publishSequence = new AtomicLong(0);
      byte[] payload = new byte[payloadSize];
      publisher.declarePublisher(b(1), null, testStream);
      Runnable publish =
          () ->
              publisher.publish(
                  b(1),
                  Collections.singletonList(
                      publisher
                          .messageBuilder()
                          .properties()
                          .messageId(publishSequence.getAndIncrement())
                          .messageBuilder()
                          .addData(payload)
                          .build()));
      IntStream.range(0, messageCount).forEach(i -> publish.run());
      assertThat(publishingLatch.await(10, SECONDS)).isTrue();

      configuration.waiting();

      // publishing again, to make sure new segments trigger retention strategy
      IntStream.range(0, messageCount).forEach(i -> publish.run());
      assertThat(publishingLatchSecondWave.await(10, SECONDS)).isTrue();

      CountDownLatch consumingLatch = new CountDownLatch(1);
      AtomicLong firstMessageId = new AtomicLong(-1);
      Client consumer =
          cf.get(
              new Client.ClientParameters()
                  .chunkListener(
                      (client1, subscriptionId, offset, messageCount1, dataSize) ->
                          client1.credit(subscriptionId, 1))
                  .messageListener(
                      (subscriptionId, offset, message) -> {
                        long messageId = message.getProperties().getMessageIdAsLong();
                        firstMessageId.compareAndSet(-1, messageId);
                        if (messageId == publishSequence.get() - 1) {
                          consumingLatch.countDown();
                        }
                      }));

      consumer.subscribe(b(1), testStream, OffsetSpecification.first(), 10);
      assertThat(consumingLatch.await(10, SECONDS)).isTrue();
      consumer.unsubscribe(b(1));
      assertThat(configuration.firstMessageIdAssertion.test(firstMessageId.get())).isTrue();
    } finally {
      publisher.delete(testStream);
    }
  }

  private static class RetentionTestConfig {
    final String description;
    final CallableConsumer<Object> streamCreator;
    final Predicate<Long> firstMessageIdAssertion;
    final Duration waitTime;

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion) {
      this(description, streamCreator, firstMessageIdAssertion, Duration.ZERO);
    }

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion,
        Duration waitTime) {
      this.description = description;
      this.streamCreator = streamCreator;
      this.firstMessageIdAssertion = firstMessageIdAssertion;
      this.waitTime = waitTime;
    }

    void waiting() throws InterruptedException {
      if (this.waitTime.toMillis() > 0) {
        Thread.sleep(this.waitTime.toMillis());
      }
    }

    @Override
    public String toString() {
      return this.description;
    }
  }
}
