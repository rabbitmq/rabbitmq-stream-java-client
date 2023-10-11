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

import static com.rabbitmq.stream.impl.TestUtils.*;
import static com.rabbitmq.stream.impl.TestUtils.ResponseConditions.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.QpidProtonCodec;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.codec.SwiftMqCodec;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.Client.StreamParametersBuilder;
import com.rabbitmq.stream.impl.Client.StreamStatsResponse;
import com.rabbitmq.stream.impl.ServerFrameHandler.FrameHandlerInfo;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersion;
import com.rabbitmq.stream.impl.TestUtils.BrokerVersionAtLeast;
import com.rabbitmq.stream.impl.TestUtils.DisabledIfFilteringNotSupported;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class ClientTest {

  static final Charset UTF8 = StandardCharsets.UTF_8;

  int credit = 10;

  String stream;
  TestUtils.ClientFactory cf;

  static boolean await(CountDownLatch latch, Duration timeout) {
    try {
      return latch.await(timeout.getSeconds(), SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void connectionErrorShouldReturnStreamExceptionForStackTrace() {
    assertThatThrownBy(
            () ->
                cf.get(
                    new ClientParameters()
                        .host(UUID.randomUUID().toString())
                        .bootstrapCustomizer(
                            b -> b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1_000))))
        .isInstanceOf(StreamException.class)
        .cause()
        .isInstanceOfAny(ConnectTimeoutException.class, UnknownHostException.class);
  }

  @Test
  void creditToUnknownSubscriptionShouldTriggerCreditNotification() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger creditNotificationCount = new AtomicInteger(0);
    AtomicInteger caughtSubscriptionId = new AtomicInteger(0);
    AtomicReference<Short> caughtResponseCode = new AtomicReference<>();
    Client client =
        cf.get(
            new Client.ClientParameters()
                .creditNotification(
                    (subscriptionId, responseCode) -> {
                      creditNotificationCount.incrementAndGet();
                      caughtSubscriptionId.set(subscriptionId);
                      caughtResponseCode.set(responseCode);
                      latch.countDown();
                    }));
    Client.Response response = client.subscribe(b(1), stream, OffsetSpecification.first(), 20);
    assertThat(response.isOk()).isTrue();
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);

    client.credit(b(1), 1);
    client.credit(b(42), credit);

    assertThat(latch.await(10, SECONDS)).isTrue();
    assertThat(creditNotificationCount.get()).isEqualTo(1);
    assertThat(caughtSubscriptionId.get()).isEqualTo(42);
    assertThat(caughtResponseCode.get())
        .isEqualTo(Constants.RESPONSE_CODE_SUBSCRIPTION_ID_DOES_NOT_EXIST);
  }

  @Test
  void publishConfirm() throws Exception {
    int publishCount = 1000;

    Set<Long> confirmedIds = ConcurrentHashMap.newKeySet(publishCount);
    CountDownLatch latchConfirm = new CountDownLatch(1);
    Client.PublishConfirmListener publishConfirmListener =
        (publisherId, correlationId) -> {
          confirmedIds.add(correlationId);
          if (confirmedIds.size() == publishCount) {
            latchConfirm.countDown();
          }
        };

    Client client =
        cf.get(new Client.ClientParameters().publishConfirmListener(publishConfirmListener));

    client.declarePublisher(b(1), null, stream);
    Set<Long> correlationIds = new HashSet<>(publishCount);
    for (int i = 1; i <= publishCount; i++) {
      List<Long> sequenceId =
          client.publish(
              b(1),
              Collections.singletonList(
                  client
                      .messageBuilder()
                      .addData(("message" + i).getBytes(StandardCharsets.UTF_8))
                      .build()));
      correlationIds.add(sequenceId.get(0));
    }

    assertThat(latchConfirm.await(60, SECONDS)).isTrue();
    correlationIds.removeAll(confirmedIds);
    assertThat(correlationIds).isEmpty();
  }

  @Test
  void publishConfirmWithSeveralPublisherIds() throws Exception {
    Map<Byte, Integer> outboundMessagesSpec =
        new HashMap<Byte, Integer>() {
          {
            put(b(0), 1_000);
            put(b(1), 100);
            put(b(2), 50_000);
            put(b(3), 10_000);
            put(b(4), 500);
          }
        };

    Map<Byte, Set<Long>> confirmed = new ConcurrentHashMap<>();
    Map<Byte, Set<Long>> publishingSequences = new ConcurrentHashMap<>();
    AtomicInteger totalMessageCount = new AtomicInteger(0);
    outboundMessagesSpec
        .keySet()
        .forEach(
            publisherId -> {
              confirmed.put(publisherId, ConcurrentHashMap.newKeySet());
              publishingSequences.put(publisherId, ConcurrentHashMap.newKeySet());
              totalMessageCount.addAndGet(outboundMessagesSpec.get(publisherId));
            });

    CountDownLatch confirmLatch = new CountDownLatch(totalMessageCount.get());

    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener(
                    (publisherId, publishingId) -> {
                      confirmed.get(publisherId).add(publishingId);
                      confirmLatch.countDown();
                    }));

    outboundMessagesSpec.entrySet().stream()
        .map(
            entry -> {
              byte publisherId = entry.getKey();
              client.declarePublisher(publisherId, null, stream);
              Random random = new Random();
              return new Thread(
                  () -> {
                    int messageCount = entry.getValue();
                    while (messageCount > 0) {
                      int messagesToPublish =
                          messageCount < 10 ? messageCount : random.nextInt(messageCount);
                      List<Long> pSequences =
                          client.publish(
                              publisherId,
                              IntStream.range(0, messagesToPublish)
                                  .mapToObj(
                                      i ->
                                          client
                                              .messageBuilder()
                                              .addData(("hello " + i).getBytes(UTF8))
                                              .build())
                                  .collect(Collectors.toList()));
                      publishingSequences.get(publisherId).addAll(pSequences);
                      messageCount -= messagesToPublish;
                    }
                  });
            })
        .collect(Collectors.toList())
        .forEach(Thread::start);

    assertThat(confirmLatch.await(10, SECONDS)).isTrue();
    outboundMessagesSpec.forEach(
        (key, value) -> {
          byte publisherId = key;
          int expectedMessageCount = value;
          Set<Long> sequences = publishingSequences.get(publisherId);
          Set<Long> confirmedPublishingIds = confirmed.get(publisherId);
          // Not using collection assertions because they dump the whole content
          // This is unreadable for large collections
          assertThat(sequences.size()).isEqualTo(expectedMessageCount);
          assertThat(confirmedPublishingIds.size()).isEqualTo(expectedMessageCount);

          confirmedPublishingIds.forEach(
              publishingId -> assertThat(sequences.contains(publishingId)).isTrue());
        });
  }

  void publishConsumeComplexMessage(
      Client publisher, Codec codec, Function<Integer, Message> messageFactory) {
    int publishCount = 1000;
    publisher.declarePublisher(b(1), null, stream);
    for (int i = 1; i <= publishCount; i++) {
      publisher.publish(b(1), Collections.singletonList(messageFactory.apply(i)));
    }

    CountDownLatch latch = new CountDownLatch(publishCount);
    Set<Message> messages = ConcurrentHashMap.newKeySet(publishCount);
    Client.MessageListener messageListener =
        (correlationId, offset, chunkTimestamp, committedChunkId, chunkContext, message) -> {
          messages.add(message);
          latch.countDown();
        };
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .codec(codec)
                .messageListener(messageListener)
                .chunkListener(TestUtils.credit()));
    consumer.subscribe(b(1), stream, OffsetSpecification.first(), credit);
    assertThat(await(latch, Duration.ofSeconds(10))).isTrue();
    assertThat(messages).hasSize(publishCount);
    messages.stream()
        .forEach(
            message -> {
              assertThat(message.getApplicationProperties()).hasSize(1);
              Integer id = (Integer) message.getApplicationProperties().get("id");
              assertThat(message.getBodyAsBinary())
                  .isEqualTo(("message" + id).getBytes(StandardCharsets.UTF_8));
            });
    assertThat(consumer.unsubscribe(b(1)).isOk()).isTrue();
  }

  @Test
  void publishConsumeComplexMessagesWithMessageImplementationAndSwiftMqCodec() {
    Codec codec = new SwiftMqCodec();
    Client publisher = cf.get(new Client.ClientParameters().codec(codec));
    publishConsumeComplexMessage(
        publisher,
        codec,
        i -> {
          byte[] body = ("message" + i).getBytes(StandardCharsets.UTF_8);
          Map<String, Object> applicationProperties = Collections.singletonMap("id", i);
          Message message =
              new Message() {
                @Override
                public boolean hasPublishingId() {
                  return false;
                }

                @Override
                public long getPublishingId() {
                  return 0;
                }

                @Override
                public byte[] getBodyAsBinary() {
                  return body;
                }

                @Override
                public Object getBody() {
                  return null;
                }

                @Override
                public Properties getProperties() {
                  return null;
                }

                @Override
                public Map<String, Object> getApplicationProperties() {
                  return applicationProperties;
                }

                @Override
                public Map<String, Object> getMessageAnnotations() {
                  return null;
                }
              };
          return message;
        });
  }

  @Test
  void publishConsumeComplexMessageWithMessageBuilderAndSwiftMqCodec() {
    Codec codec = new SwiftMqCodec();
    Client publisher = cf.get(new Client.ClientParameters().codec(codec));
    publishConsumeComplexMessage(
        publisher,
        codec,
        i ->
            publisher
                .messageBuilder()
                .applicationProperties()
                .entry("id", i)
                .messageBuilder()
                .addData(("message" + i).getBytes(StandardCharsets.UTF_8))
                .build());
  }

  @Test
  void publishConsumeComplexMessageWithMessageBuilderAndQpidProtonCodec() {
    Codec codec = new QpidProtonCodec();
    Client publisher = cf.get(new Client.ClientParameters().codec(codec));
    publishConsumeComplexMessage(
        publisher,
        codec,
        i ->
            publisher
                .messageBuilder()
                .applicationProperties()
                .entry("id", i)
                .messageBuilder()
                .addData(("message" + i).getBytes(StandardCharsets.UTF_8))
                .build());
  }

  @Test
  void publishConsumeWithSimpleCodec() throws Exception {
    int messageCount = 1000;
    Codec codec = new SimpleCodec();
    Client publisher = cf.get(new Client.ClientParameters().codec(codec));
    publisher.declarePublisher(b(1), null, stream);
    IntStream.range(0, 1000)
        .forEach(
            i ->
                publisher.publish(
                    b(1),
                    Collections.singletonList(
                        publisher
                            .messageBuilder()
                            .addData(String.valueOf(i).getBytes(UTF8))
                            .build())));

    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Set<String> messageBodies = ConcurrentHashMap.newKeySet(messageCount);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .codec(codec)
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {
                      messageBodies.add(new String(message.getBodyAsBinary()));
                      consumeLatch.countDown();
                    }));

    consumer.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(consumeLatch.await(10, SECONDS)).isTrue();
    IntStream.range(0, messageCount)
        .forEach(i -> assertThat(messageBodies).contains(String.valueOf(i)));
  }

  @Test
  void batchPublishing() throws Exception {
    int batchCount = 500;
    int batchSize = 10;
    int payloadSize = 20;
    int messageCount = batchCount * batchSize;
    CountDownLatch publishLatch = new CountDownLatch(messageCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishLatch.countDown()));
    AtomicInteger publishingSequence = new AtomicInteger(0);
    publisher.declarePublisher(b(1), null, stream);
    IntStream.range(0, batchCount)
        .forEach(
            batchIndex -> {
              publisher.publish(
                  b(1),
                  IntStream.range(0, batchSize)
                      .mapToObj(
                          i -> {
                            int sequence = publishingSequence.getAndIncrement();
                            ByteBuffer b = ByteBuffer.allocate(payloadSize);
                            b.putInt(sequence);
                            return b.array();
                          })
                      .map(body -> publisher.messageBuilder().addData(body).build())
                      .collect(Collectors.toList()));
            });

    assertThat(publishLatch.await(10, SECONDS)).isTrue();

    Set<Integer> sizes = ConcurrentHashMap.newKeySet(1);
    Set<Integer> sequences = ConcurrentHashMap.newKeySet(messageCount);
    CountDownLatch consumeLatch = new CountDownLatch(messageCount);
    Client consumer =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {
                      ByteBuffer bb = ByteBuffer.wrap(message.getBodyAsBinary());
                      sizes.add(message.getBodyAsBinary().length);
                      sequences.add(bb.getInt());
                      consumeLatch.countDown();
                    }));

    consumer.subscribe(b(1), stream, OffsetSpecification.first(), 10);
    assertThat(consumeLatch.await(10, SECONDS)).isTrue();
    assertThat(sizes).hasSize(1).containsOnly(payloadSize);
    assertThat(sequences).hasSize(messageCount);
    IntStream.range(0, messageCount).forEach(value -> assertThat(sequences).contains(value));
  }

  @Test
  void consume() throws Exception {
    int publishCount = 100000;
    byte correlationId = 42;
    TestUtils.publishAndWaitForConfirms(cf, publishCount, stream);

    CountDownLatch latch = new CountDownLatch(publishCount);

    AtomicInteger receivedCorrelationId = new AtomicInteger();
    Client.ChunkListener chunkListener =
        (client, corr, offset, messageCountInChunk, dataSize) -> {
          receivedCorrelationId.set(corr);
          client.credit(correlationId, 1);
          return null;
        };

    AtomicLong chunkTimestamp = new AtomicLong();
    Client.MessageListener messageListener =
        (corr, offset, chkTimestamp, committedChunkId, chunkContext, message) -> {
          chunkTimestamp.set(chkTimestamp);
          latch.countDown();
        };

    Client client =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(chunkListener)
                .messageListener(messageListener));
    Response response =
        client.subscribe(correlationId, stream, OffsetSpecification.first(), credit);
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
    assertThat(response.isOk()).isTrue();

    assertThat(latch.await(60, SECONDS)).isTrue();
    assertThat(receivedCorrelationId).hasValue(correlationId);
    assertThat(chunkTimestamp.get()).isNotZero();
    client.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void publishAndConsume(boolean directBuffer) throws Exception {
    ByteBufAllocator allocator = new UnpooledByteBufAllocator(directBuffer);
    int publishCount = 1_000_000;

    CountDownLatch consumedLatch = new CountDownLatch(publishCount);
    Client.ChunkListener chunkListener =
        (client, correlationId, offset, messageCount, dataSize) -> {
          if (consumedLatch.getCount() != 0) {
            client.credit(correlationId, 1);
          }
          return null;
        };

    Client.MessageListener messageListener =
        (corr, offset, chunkTimestamp, committedOffset, chunkContext, data) ->
            consumedLatch.countDown();

    Client client =
        cf.get(
            new Client.ClientParameters()
                .byteBufAllocator(allocator)
                .chunkListener(chunkListener)
                .messageListener(messageListener));
    client.subscribe(b(1), stream, OffsetSpecification.first(), credit);

    CountDownLatch confirmedLatch = new CountDownLatch(publishCount);
    Set<Long> sent = ConcurrentHashMap.newKeySet(publishCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .byteBufAllocator(allocator)
                .publishConfirmListener(
                    (publisherId, correlationId) -> {
                      sent.remove(correlationId);
                      confirmedLatch.countDown();
                    }));
    publisher.declarePublisher(b(1), null, stream);
    LongConsumer publish =
        messageId -> {
          sent.add(messageId);
          publisher.publish(
              b(1),
              Collections.singletonList(
                  publisher
                      .messageBuilder()
                      .addData(("message" + messageId).getBytes(StandardCharsets.UTF_8))
                      .build()),
              msg -> messageId);
        };
    new Thread(
            () -> {
              int messageId = 0;
              while (messageId < publishCount) {
                messageId++;
                publish.accept(messageId);
              }
            })
        .start();

    int attempt = 0;
    while (attempt < 3) {
      boolean allConfirmed = confirmedLatch.await(15, SECONDS);
      if (allConfirmed) {
        break;
      } else {
        attempt++;
        for (Long messageIdNotConfirmed : sent) {
          publish.accept(messageIdNotConfirmed);
        }
      }
    }
    assertThat(consumedLatch.await(15, SECONDS)).isTrue();
    client.unsubscribe(b(1));
  }

  @Test
  void deleteNonExistingStreamShouldReturnError() {
    String nonExistingStream = UUID.randomUUID().toString();
    Client.Response response = cf.get().delete(nonExistingStream);
    assertThat(response).is(ko()).has(responseCode(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST));
  }

  @Test
  void deleteNonStreamQueueShouldReturnError() throws Exception {
    String nonStreamQueue = UUID.randomUUID().toString();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    try (Connection amqpConnection = connectionFactory.newConnection()) {
      Channel c = amqpConnection.createChannel();
      c.queueDeclare(nonStreamQueue, false, true, false, null);
      Client.Response response = cf.get().delete(nonStreamQueue);
      assertThat(response.isOk()).isFalse();
      assertThat(response.getResponseCode())
          .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "amq.somename"})
  void createWithInvalidNameShouldReturnError(String name) {
    Client.Response response = cf.get().create(name);
    assertThat(response.isOk()).isFalse();
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_PRECONDITION_FAILED);
  }

  @Test
  void createAlreadyExistingStreamShouldReturnError() {
    Client.Response response = cf.get().create(stream);
    assertThat(response.isOk()).isFalse();
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_STREAM_ALREADY_EXISTS);
  }

  @Test
  void createStreamWithDifferentParametersShouldThrowException(TestInfo info) {
    String s = streamName(info);
    Client client = cf.get();
    try {
      StreamParametersBuilder streamParametersBuilder =
          new StreamParametersBuilder().maxAge(Duration.ofDays(1));
      Response response = client.create(s, streamParametersBuilder.build());
      assertThat(response.isOk()).isTrue();
      response = client.create(s, streamParametersBuilder.maxAge(Duration.ofDays(4)).build());
      assertThat(response.isOk()).isFalse();
      assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_PRECONDITION_FAILED);
    } finally {
      assertThat(client.delete(s).isOk()).isTrue();
    }
  }

  @Test
  void declarePublisherToNonStreamQueueTriggersError() throws Exception {
    String nonStreamQueue = UUID.randomUUID().toString();
    ConnectionFactory connectionFactory = new ConnectionFactory();
    try (Connection amqpConnection = connectionFactory.newConnection()) {
      Channel c = amqpConnection.createChannel();
      c.queueDeclare(nonStreamQueue, false, true, false, null);

      Client client = cf.get();
      Response response = client.declarePublisher(b(1), null, nonStreamQueue);
      assertThat(response.isOk()).isFalse();
      assertThat(response.getResponseCode())
          .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    }
  }

  @Test
  void declareAmqpStreamQueueAndUseItAsStream(TestInfo info) throws Exception {
    int messageCount = 10000;
    String q = streamName(info);
    CountDownLatch publishedLatch = new CountDownLatch(messageCount);
    CountDownLatch consumedLatch = new CountDownLatch(messageCount);
    Client client =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener((publisherId, publishingId) -> publishedLatch.countDown())
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> consumedLatch.countDown()));
    ConnectionFactory connectionFactory = new ConnectionFactory();
    try (Connection amqpConnection = connectionFactory.newConnection()) {
      Channel c = amqpConnection.createChannel();
      c.queueDeclare(q, true, false, false, Collections.singletonMap("x-queue-type", "stream"));

      client.declarePublisher(b(1), null, q);
      IntStream.range(0, messageCount)
          .forEach(
              i ->
                  client.publish(
                      b(1),
                      Collections.singletonList(
                          client
                              .messageBuilder()
                              .addData("hello".getBytes(StandardCharsets.UTF_8))
                              .build())));
      assertThat(publishedLatch.await(10, SECONDS)).isTrue();

      client.subscribe(b(1), q, OffsetSpecification.first(), 10);
      assertThat(consumedLatch.await(10, SECONDS)).isTrue();
    } finally {
      client.delete(q);
    }
  }

  @Test
  void publishThenDeleteStreamShouldTriggerPublishErrorListenerWhenPublisherAgain(TestInfo info)
      throws Exception {
    String s = streamName(info);
    Client configurer = cf.get();
    Client.Response response = configurer.create(s);
    assertThat(response.isOk()).isTrue();
    int messageCount = 10;
    AtomicInteger confirms = new AtomicInteger(0);
    Set<Short> responseCodes = ConcurrentHashMap.newKeySet(1);
    Set<Long> publishingIdErrors = ConcurrentHashMap.newKeySet(messageCount);
    CountDownLatch confirmLatch = new CountDownLatch(messageCount);
    CountDownLatch publishErrorLatch = new CountDownLatch(messageCount);
    Client publisher =
        cf.get(
            new Client.ClientParameters()
                .publishConfirmListener(
                    (publisherId, publishingId) -> {
                      confirms.incrementAndGet();
                      confirmLatch.countDown();
                    })
                .publishErrorListener(
                    (publisherId, publishingId, responseCode) -> {
                      publishingIdErrors.add(publishingId);
                      responseCodes.add(responseCode);
                      publishErrorLatch.countDown();
                    }));

    publisher.declarePublisher(b(1), null, s);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publisher.publish(
                    b(1),
                    Collections.singletonList(
                        publisher
                            .messageBuilder()
                            .addData(("first wave" + i).getBytes())
                            .build())));

    assertThat(confirmLatch.await(10, SECONDS)).isTrue();
    assertThat(confirms.get()).isEqualTo(messageCount);

    response = configurer.delete(s);
    assertThat(response.isOk()).isTrue();

    // let the event some time to propagate
    Thread.sleep(1000);

    Set<Long> publishingIds = ConcurrentHashMap.newKeySet(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i ->
                publishingIds.addAll(
                    publisher.publish(
                        b(1),
                        Collections.singletonList(
                            publisher
                                .messageBuilder()
                                .addData(("second wave" + i).getBytes())
                                .build()))));

    assertThat(publishErrorLatch.await(10, SECONDS)).isTrue();
    assertThat(confirms.get()).isEqualTo(messageCount);
    assertThat(responseCodes).hasSize(1).contains(Constants.RESPONSE_CODE_PUBLISHER_DOES_NOT_EXIST);
    assertThat(publishingIdErrors)
        .hasSameSizeAs(publishingIds)
        .hasSameElementsAs(publishingIdErrors);
  }

  @Test
  void serverShouldSendCloseWhenSendingGarbage() throws Exception {
    Client client = cf.get();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(out);
    dataOutputStream.writeInt(4);
    dataOutputStream.writeShort(30000); // command ID
    dataOutputStream.writeShort(Constants.VERSION_1);
    client.send(out.toByteArray());
    waitAtMost(10, () -> client.isOpen() == false);
  }

  @Test
  void clientShouldContainServerAdvertisedHostAndPort() {
    Client client = cf.get();
    assertThat(client.serverAdvertisedHost()).isNotNull();
    assertThat(client.serverAdvertisedPort()).isEqualTo(Client.DEFAULT_PORT);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "some-publisher-reference"})
  void closingPublisherWhilePublishingShouldNotCloseConnection(String publisherReference) {
    AtomicReference<CountDownLatch> confirmLatch =
        new AtomicReference<>(new CountDownLatch(500_000));
    CountDownLatch closedLatch = new CountDownLatch(1);
    Semaphore semaphore = new Semaphore(10000);
    Client client =
        cf.get(
            new ClientParameters()
                .shutdownListener(shutdownContext -> closedLatch.countDown())
                .publishConfirmListener(
                    (publisherId, publishingId) -> {
                      semaphore.release();
                      confirmLatch.get().countDown();
                    }));

    Response response = client.declarePublisher(TestUtils.b(0), publisherReference, stream);
    assertThat(response.isOk()).isTrue();
    List<Thread> threads =
        IntStream.range(0, 10)
            .mapToObj(
                i -> {
                  Thread thread =
                      new Thread(
                          () -> {
                            while (!Thread.currentThread().isInterrupted()) {
                              List<Message> messages =
                                  IntStream.range(0, 100)
                                      .mapToObj(
                                          j ->
                                              client
                                                  .messageBuilder()
                                                  .addData("hello".getBytes(StandardCharsets.UTF_8))
                                                  .build())
                                      .collect(Collectors.toList());
                              semaphore.acquireUninterruptibly(100);
                              client.publish(b(0), messages);
                            }
                          });
                  thread.start();
                  return thread;
                })
            .collect(Collectors.toList());
    try {
      assertThat(latchAssert(confirmLatch)).completes();
      response = client.deletePublisher(b(0));
      assertThat(response.isOk()).isTrue();
      assertThat(latchAssert(closedLatch)).doesNotComplete(1);
    } finally {
      threads.forEach(Thread::interrupt);
    }
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void exchangeCommandVersions() {
    Client client = cf.get();
    List<FrameHandlerInfo> infos = client.exchangeCommandVersions();
    assertThat(infos.stream().filter(info -> info.getKey() == Constants.COMMAND_DECLARE_PUBLISHER))
        .isNotEmpty();
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void deliverVersion2LastCommittedOffsetShouldBeSet() throws Exception {
    int publishCount = 20_000;
    byte correlationId = 42;
    TestUtils.publishAndWaitForConfirms(cf, publishCount, stream);

    CountDownLatch latch = new CountDownLatch(publishCount);

    AtomicLong committedOffset = new AtomicLong();
    Client.MessageListener messageListener =
        (corr, offset, chkTimestamp, committedOfft, chunkContext, message) -> {
          committedOffset.set(committedOfft);
          latch.countDown();
        };

    Client client =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(TestUtils.credit())
                .messageListener(messageListener));

    client.exchangeCommandVersions();

    Response response =
        client.subscribe(correlationId, stream, OffsetSpecification.first(), credit);
    assertThat(response.getResponseCode()).isEqualTo(Constants.RESPONSE_CODE_OK);
    assertThat(response.isOk()).isTrue();

    assertThat(latch.await(10, SECONDS)).isTrue();
    assertThat(committedOffset.get()).isPositive();
    client.close();
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void streamStatsShouldReturnFirstOffsetAndCommittedOffset() throws Exception {
    int publishCount = 20_000;
    CountDownLatch latch = new CountDownLatch(publishCount);

    AtomicLong committedOffset = new AtomicLong();
    Client.MessageListener messageListener =
        (corr, offset, chkTimestamp, committedOfft, chunkContext, message) -> {
          committedOffset.set(committedOfft);
          latch.countDown();
        };
    Client client =
        cf.get(
            new Client.ClientParameters()
                .chunkListener(TestUtils.credit())
                .messageListener(messageListener));
    StreamStatsResponse response = client.streamStats(stream);
    assertThat(response.getInfo()).containsEntry("first_chunk_id", -1L);
    assertThat(response.getInfo()).containsEntry("committed_chunk_id", -1L);
    TestUtils.publishAndWaitForConfirms(cf, publishCount, stream);
    response = client.streamStats(stream);
    assertThat(response.getInfo()).containsEntry("first_chunk_id", 0L);
    assertThat(response.getInfo().get("committed_chunk_id")).isNotEqualTo(-1L);

    client.exchangeCommandVersions();

    Response subscribeResponse =
        client.subscribe(b(42), stream, OffsetSpecification.first(), credit);
    assertThat(subscribeResponse.isOk()).isTrue();

    assertThat(latch.await(10, SECONDS)).isTrue();
    assertThat(committedOffset.get()).isPositive();
    assertThat(committedOffset).hasValue(response.getInfo().get("committed_chunk_id"));
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void streamStatsShouldReturnErrorWhenStreamDoesNotExist() {
    assertThat(cf.get().streamStats("does not exist").getResponseCode())
        .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
  }

  @Test
  @BrokerVersionAtLeast(BrokerVersion.RABBITMQ_3_11)
  void streamStatsFirstOffsetShouldChangeAfterRetentionKickedIn(TestInfo info) throws Exception {
    // this test is flaky in some CI environments, so we have to retry it
    repeatIfFailure(
        () -> {
          int messageCount = 1000;
          int payloadSize = 1000;
          String s = TestUtils.streamName(info);
          Client client = cf.get();
          try {
            assertThat(
                    client
                        .create(
                            s,
                            new Client.StreamParametersBuilder()
                                .maxLengthBytes(messageCount * payloadSize / 10)
                                .maxSegmentSizeBytes(messageCount * payloadSize / 20)
                                .build())
                        .isOk())
                .isTrue();

            StreamStatsResponse response = client.streamStats(s);
            assertThat(response.getInfo()).containsEntry("first_chunk_id", -1L);
            assertThat(response.getInfo()).containsEntry("committed_chunk_id", -1L);

            byte[] payload = new byte[payloadSize];
            Function<MessageBuilder, Message> messageCreation = mb -> mb.addData(payload).build();

            TestUtils.publishAndWaitForConfirms(cf, messageCreation, messageCount, s);
            // publishing again, to make sure new segments trigger retention strategy
            TestUtils.publishAndWaitForConfirms(cf, messageCreation, messageCount, s);
            response = client.streamStats(s);
            assertThat(response.getInfo().get("first_chunk_id")).isPositive();
            assertThat(response.getInfo().get("committed_chunk_id")).isPositive();

          } finally {
            assertThat(client.delete(s).isOk()).isTrue();
          }
        });
  }

  @Test
  @DisabledIfFilteringNotSupported
  void publishConsumeWithFilterValueShouldSendSubSetOfStream() throws Exception {
    int messageCount = 10_000;
    AtomicReference<CountDownLatch> publishLatch =
        new AtomicReference<>(new CountDownLatch(messageCount));
    Client publisher =
        cf.get(
            new ClientParameters()
                .publishConfirmListener(
                    (publisherId, publishingId) -> publishLatch.get().countDown()));
    AtomicLong sequence = new AtomicLong(0);
    ToLongFunction<Object> sequenceFunction = msg -> sequence.getAndIncrement();
    Response response = publisher.declarePublisher(b(0), null, stream);
    assertThat(response).is(ok());

    // first wave of messages with several filter values
    List<String> filterValues = new ArrayList<>(Arrays.asList("apple", "banana", "pear"));
    Map<String, AtomicInteger> filterValueCount = new HashMap<>();
    Random random = new Random();
    class Entity {
      private final Codec.EncodedMessage encodedMessage;
      private final String filterValue;

      Entity(Codec.EncodedMessage encodedMessage, String filterValue) {
        this.encodedMessage = encodedMessage;
        this.filterValue = filterValue;
      }
    }

    Client.OutboundEntityWriteCallback writeCallback =
        new Client.OutboundEntityWriteCallback() {
          @Override
          public int write(ByteBuf bb, Object obj, long publishingId) {
            Entity msg = (Entity) obj;
            bb.writeShort(msg.filterValue.length());
            bb.writeBytes(msg.filterValue.getBytes(StandardCharsets.UTF_8));
            Codec.EncodedMessage messageToPublish = msg.encodedMessage;
            bb.writeInt(messageToPublish.getSize());
            bb.writeBytes(messageToPublish.getData(), 0, messageToPublish.getSize());
            return 1;
          }

          @Override
          public int fragmentLength(Object obj) {
            Entity msg = (Entity) obj;
            return 8 + 2 + msg.filterValue.length() + 4 + msg.encodedMessage.getSize();
          }
        };
    int batchSize = 100;
    List<Object> messages = new ArrayList<>(batchSize);
    Runnable write =
        () ->
            publisher.publishInternal(
                Constants.VERSION_2, b(0), messages, writeCallback, sequenceFunction);
    Runnable insert =
        () -> {
          byte[] data = "hello".getBytes(StandardCharsets.UTF_8);
          int publishedMessageCount = 0;
          while (publishedMessageCount < messageCount) {
            String filterValue = filterValues.get(random.nextInt(filterValues.size()));
            filterValueCount
                .computeIfAbsent(filterValue, k -> new AtomicInteger())
                .incrementAndGet();
            Message message =
                publisher
                    .codec()
                    .messageBuilder()
                    .addData(data)
                    .properties()
                    .groupId(filterValue)
                    .messageBuilder()
                    .build();
            Entity entity = new Entity(publisher.codec().encode(message), filterValue);
            messages.add(entity);
            publishedMessageCount++;

            if (messages.size() == batchSize) {
              write.run();
              messages.clear();
            }
          }
          if (!messages.isEmpty()) {
            write.run();
          }
        };

    insert.run();
    assertThat(latchAssert(publishLatch)).completes();

    // second wave of messages, with only one, new filter value
    String newFilterValue = "orange";
    filterValues.clear();
    filterValues.add(newFilterValue);
    publishLatch.set(new CountDownLatch(messageCount));
    insert.run();
    assertThat(latchAssert(publishLatch)).completes();

    AtomicInteger consumedMessageCount = new AtomicInteger(0);
    AtomicInteger filteredConsumedMessageCount = new AtomicInteger(0);
    Client consumer =
        cf.get(
            new ClientParameters()
                .chunkListener(TestUtils.credit())
                .messageListener(
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {
                      consumedMessageCount.incrementAndGet();
                      String filterValue = message.getProperties().getGroupId();
                      if (newFilterValue.equals(filterValue)) {
                        filteredConsumedMessageCount.incrementAndGet();
                      }
                    }));

    // consume only messages with filter value from second wave
    consumer.subscribe(
        b(0),
        stream,
        OffsetSpecification.first(),
        1,
        Collections.singletonMap("filter.1", newFilterValue));

    int expectedCount = filterValueCount.get(newFilterValue).get();
    waitAtMost(() -> filteredConsumedMessageCount.get() == expectedCount);
    // we should get messages only from the "second" part of the stream
    assertThat(consumedMessageCount).hasValueLessThan(messageCount * 2);
  }
}
