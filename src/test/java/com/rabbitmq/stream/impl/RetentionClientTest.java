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

import static com.rabbitmq.stream.impl.TestUtils.b;
import static com.rabbitmq.stream.impl.TestUtils.streamName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Host;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.TestUtils.CallableConsumer;
import com.rabbitmq.stream.impl.TestUtils.RunnableWithException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.function.Predicate;
import org.junit.jupiter.api.TestInfo;
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
          firstMessageId -> firstMessageId > 0,
          firstMessageId -> "First message ID should be positive but is " + firstMessageId),
      new RetentionTestConfig(
          "retention defined in policy should take precedence",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            long maxLengthBytes = messageCount * payloadSize / 10;
            long maxSegmentSizeBytes = messageCount * payloadSize / 20;
            // big retention in policy
            client.create(
                stream,
                new Client.StreamParametersBuilder()
                    .maxLengthBytes(ByteCapacity.GB(20))
                    .maxSegmentSizeBytes(maxSegmentSizeBytes)
                    .build());
            // small retention in policy
            String policyCommand =
                String.format(
                    "set_policy stream-retention-test \"%s\" "
                        + "'{\"max-length-bytes\":%d,\"stream-max-segment-size-bytes\":%d }' "
                        + "--priority 1 --apply-to queues",
                    stream, maxLengthBytes, maxSegmentSizeBytes);
            Host.rabbitmqctl(policyCommand);
          },
          firstMessageId -> firstMessageId > 0,
          firstMessageId -> "First message ID should be positive but is " + firstMessageId,
          () -> Host.rabbitmqctl("clear_policy stream-retention-test")),
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
          firstMessageId -> firstMessageId > 0,
          firstMessageId -> "First message ID should be positive but is " + firstMessageId),
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
          firstMessageId -> "First message ID should be positive but is " + firstMessageId,
          Duration.ofSeconds(3)),
      new RetentionTestConfig(
          "no retention",
          context -> {
            Client client = (Client) ((Object[]) context)[0];
            String stream = (String) ((Object[]) context)[1];
            client.create(stream, Collections.emptyMap());
          },
          firstMessageId -> firstMessageId == 0,
          firstMessageId -> "First message ID should be 0 but is " + firstMessageId),
      new RetentionTestConfig(
          "with AMQP client, no retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection()) {
              Channel ch = c.createChannel();
              ch.queueDeclare(
                  stream, true, false, false, Collections.singletonMap("x-queue-type", "stream"));
            }
          },
          firstMessageId -> firstMessageId == 0,
          firstMessageId -> "First message ID should be 0 but is " + firstMessageId),
      new RetentionTestConfig(
          "with AMQP client, with size-based retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection()) {
              Channel ch = c.createChannel();
              Map<String, Object> arguments = new HashMap<>();
              arguments.put("x-queue-type", "stream");
              arguments.put("x-max-length-bytes", messageCount * payloadSize / 10);
              arguments.put("x-stream-max-segment-size-bytes", messageCount * payloadSize / 20);
              ch.queueDeclare(stream, true, false, false, arguments);
            }
          },
          firstMessageId -> firstMessageId > 0,
          firstMessageId -> "First message ID should be positive but is " + firstMessageId),
      new RetentionTestConfig(
          "with AMQP client, with age-based retention",
          context -> {
            String stream = (String) ((Object[]) context)[1];
            ConnectionFactory cf = new ConnectionFactory();
            try (Connection c = cf.newConnection()) {
              Channel ch = c.createChannel();
              Map<String, Object> arguments = new HashMap<>();
              arguments.put("x-queue-type", "stream");
              arguments.put("x-max-age", "2s");
              arguments.put("x-stream-max-segment-size-bytes", messageCount * payloadSize / 20);
              ch.queueDeclare(stream, true, false, false, arguments);
            }
          },
          firstMessageId -> firstMessageId > 0,
          firstMessageId -> "First message ID should be positive but is " + firstMessageId,
          Duration.ofSeconds(3)),
    };
  }

  @TestUtils.DisabledIfRabbitMqCtlNotSet
  @ParameterizedTest
  @MethodSource
  void retention(RetentionTestConfig configuration, TestInfo info) throws Exception {
    // this test is flaky in some CI environments, so we have to retry it
    int attemptCount = 0;
    int maxAttempts = 3;
    while (attemptCount <= maxAttempts) {
      attemptCount++;
      String testStream = streamName(info);

      Client client = cf.get();
      try {
        configuration.streamCreator.accept(new Object[] {client, testStream});

        AtomicLong publishSequence = new AtomicLong(0);
        Runnable publish =
            () -> {
              byte[] payload = new byte[payloadSize];
              TestUtils.publishAndWaitForConfirms(
                  cf,
                  messageBuilder ->
                      messageBuilder
                          .properties()
                          .messageId(publishSequence.getAndIncrement())
                          .messageBuilder()
                          .addData(payload)
                          .build(),
                  messageCount,
                  testStream,
                  Duration.ofSeconds(20));
            };

        publish.run();
        configuration.waiting();
        // publishing again, to make sure new segments trigger retention strategy
        publish.run();

        CountDownLatch consumingLatch = new CountDownLatch(1);
        AtomicLong firstMessageId = new AtomicLong(-1);
        AtomicLong lastMessageId = new AtomicLong(-1);
        Client consumer =
            cf.get(
                new Client.ClientParameters()
                    .chunkListener(TestUtils.credit())
                    .messageListener(
                        (subscriptionId,
                            offset,
                            chunkTimestamp,
                            committedOffset,
                            chunkContext,
                            message) -> {
                          long messageId = message.getProperties().getMessageIdAsLong();
                          firstMessageId.compareAndSet(-1, messageId);
                          lastMessageId.set(messageId);
                          if (messageId == publishSequence.get() - 1) {
                            consumingLatch.countDown();
                          }
                        }));

        consumer.subscribe(b(1), testStream, OffsetSpecification.first(), 10);
        assertThat(consumingLatch.await(10, SECONDS))
            .as(
                () ->
                    String.format(
                        "Failure '%s', first message ID %d, last message ID %d, publish sequence %d",
                        configuration.description,
                        firstMessageId.get(),
                        lastMessageId.get(),
                        publishSequence.get()))
            .isTrue();
        consumer.unsubscribe(b(1));
        assertThat(configuration.firstMessageIdAssertion.test(firstMessageId.get()))
            .as(configuration.assertionDescription.apply(firstMessageId.get()))
            .isTrue();
        attemptCount = Integer.MAX_VALUE;
      } catch (AssertionError e) {
        // if too many attempts, fail the test, otherwise, try again
        if (attemptCount > maxAttempts) {
          throw e;
        }
      } finally {
        client.delete(testStream);
        configuration.clean();
      }
    }
  }

  private static class RetentionTestConfig {
    final String description;
    final CallableConsumer<Object> streamCreator;
    final Predicate<Long> firstMessageIdAssertion;
    final LongFunction<String> assertionDescription;
    final Duration waitTime;
    final RunnableWithException cleaning;

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion,
        LongFunction<String> assertionDescription) {
      this(
          description,
          streamCreator,
          firstMessageIdAssertion,
          assertionDescription,
          Duration.ZERO,
          null);
    }

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion,
        LongFunction<String> assertionDescription,
        Duration waitTime) {
      this(
          description,
          streamCreator,
          firstMessageIdAssertion,
          assertionDescription,
          waitTime,
          null);
    }

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion,
        LongFunction<String> assertionDescription,
        RunnableWithException cleaning) {
      this(
          description,
          streamCreator,
          firstMessageIdAssertion,
          assertionDescription,
          Duration.ZERO,
          cleaning);
    }

    RetentionTestConfig(
        String description,
        CallableConsumer<Object> streamCreator,
        Predicate<Long> firstMessageIdAssertion,
        LongFunction<String> assertionDescription,
        Duration waitTime,
        RunnableWithException cleaning) {
      this.description = description;
      this.streamCreator = streamCreator;
      this.firstMessageIdAssertion = firstMessageIdAssertion;
      this.assertionDescription = assertionDescription;
      this.waitTime = waitTime;
      this.cleaning = cleaning == null ? () -> {} : cleaning;
    }

    void waiting() throws InterruptedException {
      if (this.waitTime.toMillis() > 0) {
        Thread.sleep(this.waitTime.toMillis());
      }
    }

    void clean() throws Exception {
      this.cleaning.run();
    }

    @Override
    public String toString() {
      return this.description;
    }
  }
}
