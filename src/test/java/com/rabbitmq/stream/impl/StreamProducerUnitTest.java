// Copyright (c) 2021-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static java.util.Collections.emptyList;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.ObservationCollector;
import com.rabbitmq.stream.Resource;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.DefaultCompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.OutboundEntityWriteCallback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class StreamProducerUnitTest {

  @Mock StreamEnvironment env;
  @Mock Client client;
  @Mock Channel channel;
  @Mock ChannelFuture channelFuture;

  Queue<ByteBuf> buffers = new ConcurrentLinkedQueue<>();

  ScheduledExecutorService executorService;
  Clock clock = new Clock();

  AutoCloseable mocks;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    executorService = Executors.newScheduledThreadPool(2);
    ByteBufAllocator allocator = new UnpooledByteBufAllocator(false);
    when(channel.alloc()).thenReturn(allocator);
    when(channel.writeAndFlush(Mockito.any())).thenReturn(channelFuture);
    when(client.allocateNoCheck(any(ByteBufAllocator.class), anyInt()))
        .thenAnswer(
            (Answer<ByteBuf>)
                invocation -> {
                  ByteBufAllocator alloc = invocation.getArgument(0);
                  int capacity = invocation.getArgument(1);
                  ByteBuf buffer = alloc.buffer(capacity);
                  buffers.add(buffer);
                  return buffer;
                });
    when(client.maxFrameSize()).thenReturn(Integer.MAX_VALUE);
    when(client.publishInternal(
            anyShort(),
            anyByte(),
            anyList(),
            any(OutboundEntityWriteCallback.class),
            any(ToLongFunction.class)))
        .thenAnswer(
            invocation ->
                client.publishInternal(
                    Constants.VERSION_1,
                    channel,
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3),
                    invocation.getArgument(4)));

    when(client.publishInternal(
            anyShort(),
            any(Channel.class),
            anyByte(),
            anyList(),
            any(OutboundEntityWriteCallback.class),
            any(ToLongFunction.class)))
        .thenCallRealMethod();
    when(env.scheduledExecutorService()).thenReturn(executorService);
    when(env.locatorOperation(any())).thenCallRealMethod();
    when(env.clock()).thenReturn(clock);
    when(env.codec()).thenReturn(new SimpleCodec());
    when(env.observationCollector()).thenAnswer(invocation -> ObservationCollector.NO_OP);
    DefaultCompressionCodecFactory ccf = new DefaultCompressionCodecFactory();
    when(env.compressionCodecFactory()).thenReturn(ccf);
    doAnswer(
            (Answer<Runnable>)
                invocationOnMock -> {
                  StreamProducer p = invocationOnMock.getArgument(0);
                  p.assign((byte) 0, client);
                  return () -> {};
                })
        .when(env)
        .registerProducer(any(StreamProducer.class), nullable(String.class), anyString());
  }

  @AfterEach
  void tearDown() throws Exception {
    buffers.forEach(ByteBuf::release);
    if (executorService != null) {
      executorService.shutdownNow();
    }
    mocks.close();
  }

  @ParameterizedTest
  @CsvSource({
    "500,1000,1",
    "0,1000,1",
    "500,1000,7",
    "0,1000,7",
  })
  void confirmTimeoutTaskShouldFailMessagesAfterTimeout(
      long confirmTimeoutMs, long waitTimeMs, int subEntrySize) throws Exception {
    Duration confirmTimeout = Duration.ofMillis(confirmTimeoutMs);
    Duration waitTime = Duration.ofMillis(waitTimeMs);
    clock.refresh();
    int messageCount = 500;
    int confirmedPart = messageCount / 10;
    int expectedConfirmed = confirmedPart - confirmedPart % subEntrySize;
    AtomicInteger confirmedCount = new AtomicInteger();
    AtomicInteger erroredCount = new AtomicInteger();
    Set<Short> responseCodes = ConcurrentHashMap.newKeySet();
    CountDownLatch confirmLatch = new CountDownLatch(confirmedPart);
    ConfirmationHandler confirmationHandler =
        status -> {
          if (status.isConfirmed()) {
            confirmedCount.incrementAndGet();
            confirmLatch.countDown();
          } else {
            erroredCount.incrementAndGet();
            responseCodes.add(status.getCode());
          }
        };
    clock.refresh();
    StreamProducer producer =
        new StreamProducer(
            null,
            "stream",
            subEntrySize,
            10,
            true,
            Compression.NONE,
            Duration.ofMillis(100),
            messageCount * 10,
            confirmTimeout,
            Duration.ofSeconds(10),
            true,
            null,
            emptyList(),
            env);

    range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(), confirmationHandler));

    waitAtMost(() -> producer.unconfirmedCount() >= messageCount / subEntrySize);
    range(0, confirmedPart).forEach(producer::confirm);
    if (subEntrySize == 1) {
      assertThat(confirmedCount.get()).isEqualTo(expectedConfirmed);
    } else {
      assertThat(confirmedCount.get()).isCloseTo(confirmedCount.get(), Offset.offset(subEntrySize));
    }
    assertThat(erroredCount.get()).isZero();
    int confirmedPreviously = confirmedCount.get();

    executorService.scheduleAtFixedRate(() -> clock.refresh(), 100, 100, TimeUnit.MILLISECONDS);

    Thread.sleep(waitTime.toMillis());
    assertThat(confirmedCount.get()).isEqualTo(confirmedPreviously);
    if (confirmTimeout.isZero()) {
      assertThat(erroredCount.get()).isZero();
      assertThat(responseCodes).isEmpty();
    } else {
      waitAtMost(
          waitTime.multipliedBy(2),
          () -> erroredCount.get() == (messageCount - confirmedPreviously));
      assertThat(responseCodes).hasSize(1).contains(Constants.CODE_PUBLISH_CONFIRM_TIMEOUT);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void enqueueTimeoutMessageShouldBeFailedWhenEnqueueTimeoutIsReached(int subEntrySize) {
    Duration enqueueTimeout = Duration.ofMillis(10);
    StreamProducer producer =
        new StreamProducer(
            null,
            "stream",
            subEntrySize,
            10,
            true,
            Compression.NONE,
            Duration.ZERO,
            2,
            Duration.ofMinutes(1),
            enqueueTimeout,
            true,
            null,
            emptyList(),
            env);

    AtomicBoolean confirmCalled = new AtomicBoolean(false);
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(),
        status -> confirmCalled.set(true));
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(),
        status -> confirmCalled.set(true));

    AtomicBoolean failedConfirmCalled = new AtomicBoolean(false);
    producer.send(
        producer.messageBuilder().addData("".getBytes()).build(),
        status -> {
          assertThat(status.isConfirmed()).isFalse();
          assertThat(status.getCode()).isEqualTo(Constants.CODE_MESSAGE_ENQUEUEING_FAILED);
          failedConfirmCalled.set(true);
        });

    assertThat(confirmCalled).isFalse();
    assertThat(failedConfirmCalled).isTrue();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 7})
  void enqueueTimeoutSendingShouldBlockWhenEnqueueTimeoutIsZero(int subEntrySize) throws Exception {
    Duration enqueueTimeout = Duration.ZERO;
    StreamProducer producer =
        new StreamProducer(
            null,
            "stream",
            subEntrySize,
            10,
            true,
            Compression.NONE,
            Duration.ZERO,
            2,
            Duration.ofMinutes(1),
            enqueueTimeout,
            true,
            null,
            emptyList(),
            env);

    AtomicBoolean confirmCalled = new AtomicBoolean(false);
    CountDownLatch sendLatch = new CountDownLatch(1);
    CountDownLatch interruptedLatch = new CountDownLatch(1);
    Thread sendingThread =
        new Thread(
            () -> {
              try {
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    status -> confirmCalled.set(true));
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    status -> confirmCalled.set(true));
                sendLatch.countDown();
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(),
                    status -> confirmCalled.set(true));
              } catch (StreamException e) {
                if (e.getCause() instanceof InterruptedException) {
                  interruptedLatch.countDown();
                }
              }
            });
    sendingThread.start();
    assertThat(sendLatch.await(5, TimeUnit.SECONDS)).isTrue();
    sendingThread.interrupt();

    assertThat(interruptedLatch.await(5, TimeUnit.SECONDS)).isTrue();
    assertThat(confirmCalled).isFalse();
  }

  @Test
  void producerFlippedUnavailableDuringConstructionShouldNotBeOpened() {
    doAnswer(
            (Answer<Runnable>)
                invocationOnMock -> {
                  StreamProducer p = invocationOnMock.getArgument(0);
                  p.assign((byte) 0, client);
                  // a disruption right after the registration, before the constructor returns
                  p.unavailable();
                  return () -> {};
                })
        .when(env)
        .registerProducer(any(StreamProducer.class), nullable(String.class), anyString());

    StreamProducer producer =
        new StreamProducer(
            null,
            "stream",
            1,
            10,
            true,
            Compression.NONE,
            Duration.ofMillis(100),
            100,
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
            true,
            null,
            emptyList(),
            env);

    assertThat(producer.state()).isEqualTo(Resource.State.RECOVERING);
    producer.close();
  }

  @Test
  void assignShouldSetPublisherIdAndClientUnderTheProducerLock() throws Exception {
    StreamProducer producer =
        new StreamProducer(
            null,
            "stream",
            1,
            10,
            true,
            Compression.NONE,
            Duration.ofMillis(100),
            100,
            Duration.ofSeconds(1),
            Duration.ofSeconds(10),
            true,
            null,
            emptyList(),
            env);
    Client client2 = Mockito.mock(Client.class);
    when(client2.connectionName()).thenReturn("client-2");

    CountDownLatch assigned = new CountDownLatch(1);
    producer.lock();
    try {
      new Thread(
              () -> {
                producer.assign((byte) 3, client2);
                assigned.countDown();
              })
          .start();
      assertThat(assigned.await(200, TimeUnit.MILLISECONDS)).isFalse();
    } finally {
      producer.unlock();
    }
    assertThat(assigned.await(10, TimeUnit.SECONDS)).isTrue();

    producer.publishInternal(emptyList());
    Mockito.verify(client2)
        .publishInternal(
            anyShort(),
            Mockito.eq((byte) 3),
            anyList(),
            any(OutboundEntityWriteCallback.class),
            any());
    assertThat(producer.toString()).contains("client-2");
    producer.close();
  }

  @ParameterizedTest
  @CsvSource({
    "true,1,10", "false,1,10", "false,7,10", "true,7,10",
    "true,1,0", "false,1,0", "false,7,0", "true,7,0",
  })
  void rejectedMessagesShouldNotUseUnconfirmedPermits(
      boolean dynamicBatch, int subEntrySize, long enqueueTimeoutMs) throws Exception {
    when(client.maxFrameSize()).thenReturn(1024);
    StreamProducer producer =
        producer(dynamicBatch, subEntrySize, 2, Duration.ofMillis(enqueueTimeoutMs), null);
    Queue<ConfirmationStatus> statuses = new ConcurrentLinkedQueue<>();
    ConfirmationHandler handler = statuses::add;
    // in another thread, as sending blocks forever on leaked permits when enqueue timeout is 0
    runWithTimeout(
        () -> {
          range(0, 3)
              .forEach(
                  i ->
                      assertThatThrownBy(() -> producer.send(message(producer, 2048), handler))
                          .isInstanceOf(IllegalArgumentException.class));
          assertThat(statuses).isEmpty();
          range(0, 2).forEach(i -> producer.send(message(producer, 10), handler));
        });
    assertThat(statuses).isEmpty();
    producer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void filterValueExtractorErrorShouldNotUseUnconfirmedPermits(boolean dynamicBatch)
      throws Exception {
    when(env.filteringSupported()).thenReturn(true);
    StreamProducer producer =
        producer(dynamicBatch, 1, 2, Duration.ofMillis(10), failingFilterValueExtractor());
    Queue<ConfirmationStatus> statuses = new ConcurrentLinkedQueue<>();
    ConfirmationHandler handler = statuses::add;
    runWithTimeout(
        () -> {
          range(0, 3)
              .forEach(
                  i ->
                      assertThatThrownBy(() -> producer.send(message(producer, "bad"), handler))
                          .isInstanceOf(IllegalStateException.class));
          range(0, 2).forEach(i -> producer.send(message(producer, "good"), handler));
        });
    assertThat(statuses).isEmpty();
    producer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectedMessageShouldNotUsePublishingId(boolean dynamicBatch) throws Exception {
    when(env.filteringSupported()).thenReturn(true);
    StreamProducer producer =
        producer(dynamicBatch, 1, 10, Duration.ofMillis(10), failingFilterValueExtractor());
    Queue<ConfirmationStatus> statuses = new ConcurrentLinkedQueue<>();
    producer.send(message(producer, "good"), statuses::add);
    assertThatThrownBy(() -> producer.send(message(producer, "bad"), statuses::add))
        .isInstanceOf(IllegalStateException.class);
    producer.send(message(producer, "good"), statuses::add);
    waitAtMost(() -> producer.unconfirmedCount() == 2);
    producer.confirm(0);
    producer.confirm(1);
    assertThat(statuses).hasSize(2).allMatch(ConfirmationStatus::isConfirmed);
    producer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void interruptedSendShouldNotUseUnconfirmedPermit(boolean dynamicBatch) throws Exception {
    when(env.filteringSupported()).thenReturn(true);
    StreamProducer producer =
        producer(dynamicBatch, 1, 1, Duration.ofMillis(10), failingFilterValueExtractor());
    Queue<ConfirmationStatus> statuses = new ConcurrentLinkedQueue<>();
    runWithTimeout(
        () -> {
          // the extractor interrupts the thread, so enqueuing the message fails
          assertThatThrownBy(() -> producer.send(message(producer, "interrupt"), statuses::add))
              .isInstanceOf(RuntimeException.class);
          Thread.interrupted();
          producer.send(message(producer, "good"), statuses::add);
        });
    assertThat(statuses).isEmpty();
    producer.close();
  }

  @ParameterizedTest
  @CsvSource({"true,1", "false,1", "false,7", "true,7"})
  void messageShouldBeEncodedOnceAfterObservationStarts(boolean dynamicBatch, int subEntrySize)
      throws Exception {
    Queue<String> calls = new ConcurrentLinkedQueue<>();
    Codec delegate = new SimpleCodec();
    Codec codec =
        new Codec() {
          @Override
          public EncodedMessage encode(Message message) {
            calls.add("encode");
            return delegate.encode(message);
          }

          @Override
          public Message decode(ByteBuf buf, int length) {
            return delegate.decode(buf, length);
          }

          @Override
          public MessageBuilder messageBuilder() {
            return delegate.messageBuilder();
          }
        };
    when(env.codec()).thenReturn(codec);
    ObservationCollector<Object> observationCollector =
        new ObservationCollector<Object>() {
          @Override
          public Object prePublish(String stream, Message message) {
            calls.add("prePublish");
            return new Object();
          }

          @Override
          public void published(Object context, Message message) {}

          @Override
          public MessageHandler subscribe(MessageHandler handler) {
            return handler;
          }
        };
    when(env.observationCollector()).thenAnswer(invocation -> observationCollector);
    StreamProducer producer = producer(dynamicBatch, subEntrySize, 10, Duration.ofMillis(10), null);
    producer.send(message(producer, 10), s -> {});
    waitAtMost(() -> producer.unconfirmedCount() == 1);
    // the observation can annotate the message, so it must start before the encoding
    assertThat(calls).containsExactly("prePublish", "encode");
    producer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void subEntriesShouldFitInFrame(boolean dynamicBatch) throws Exception {
    int maxFrameSize = 1000;
    when(client.maxFrameSize()).thenReturn(maxFrameSize);
    StreamProducer producer = producer(dynamicBatch, 7, 10, Duration.ofMillis(10), null);
    // fits in a frame as a plain message, not as a sub-entry
    assertThatThrownBy(() -> producer.send(message(producer, 970), s -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    range(0, 7).forEach(i -> producer.send(message(producer, 300), s -> {}));
    // 3 messages at most per sub-entry
    waitAtMost(() -> producer.unconfirmedCount() >= 3);
    // the frame size does not include the size field
    assertThat(buffers).allMatch(b -> b.writerIndex() - 4 <= maxFrameSize);
    producer.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void closeShouldWakeUpBlockedSenders(boolean streamDeletion) throws Exception {
    StreamProducer producer = producer(true, 1, 1, Duration.ZERO, null);
    producer.send(message(producer, 10), s -> {});
    Queue<ConfirmationStatus> statuses = new ConcurrentLinkedQueue<>();
    CountDownLatch sentLatch = new CountDownLatch(2);
    range(0, 2)
        .forEach(
            i ->
                new Thread(
                        () -> {
                          producer.send(message(producer, 10), statuses::add);
                          sentLatch.countDown();
                        })
                    .start());
    waitAtMost(() -> producer.unconfirmedMessagesSemaphoreQueueLength() == 2);
    if (streamDeletion) {
      producer.closeAfterStreamDeletion(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    } else {
      producer.close();
    }
    assertThat(sentLatch.await(10, TimeUnit.SECONDS)).isTrue();
    assertThat(statuses).hasSize(2).allMatch(st -> st.getCode() == Constants.CODE_PRODUCER_CLOSED);
  }

  @ParameterizedTest
  @CsvSource({"-1,false", "0,true", "500,false", "1000,true", "5000,true"})
  void confirmTimeoutCanZeroAndLongerThanOneSecond(int timeoutInMs, boolean ok) throws Throwable {
    Duration timeout = Duration.ofMillis(timeoutInMs);
    StreamProducerBuilder builder = new StreamProducerBuilder(env);
    ThrowingCallable call = () -> builder.confirmTimeout(timeout);
    if (ok) {
      call.call();
    } else {
      assertThatThrownBy(call).isInstanceOf(IllegalArgumentException.class);
    }
  }

  private StreamProducer producer(
      boolean dynamicBatch,
      int subEntrySize,
      int maxUnconfirmedMessages,
      Duration enqueueTimeout,
      Function<Message, String> filterValueExtractor) {
    return new StreamProducer(
        null,
        "stream",
        subEntrySize,
        10,
        dynamicBatch,
        Compression.NONE,
        Duration.ofMillis(100),
        maxUnconfirmedMessages,
        Duration.ofMinutes(1),
        enqueueTimeout,
        true,
        filterValueExtractor,
        emptyList(),
        env);
  }

  private static Message message(StreamProducer producer, int size) {
    return producer.messageBuilder().addData(new byte[size]).build();
  }

  private static Message message(StreamProducer producer, String body) {
    return producer.messageBuilder().addData(body.getBytes(StandardCharsets.UTF_8)).build();
  }

  private static Function<Message, String> failingFilterValueExtractor() {
    return m -> {
      String body = new String(m.getBodyAsBinary(), StandardCharsets.UTF_8);
      if ("bad".equals(body)) {
        throw new IllegalStateException();
      } else if ("interrupt".equals(body)) {
        Thread.currentThread().interrupt();
      }
      return "filter";
    };
  }

  private static void runWithTimeout(Runnable task) throws Exception {
    CompletableFuture.runAsync(task).get(10, TimeUnit.SECONDS);
  }
}
