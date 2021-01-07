// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.codec.SimpleCodec;
import com.rabbitmq.stream.impl.Client.OutboundEntityWriteCallback;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToLongFunction;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

public class StreamProducerUnitTest {

  @Mock StreamEnvironment env;
  @Mock Client client;
  @Mock Channel channel;
  @Mock ChannelFuture channelFuture;

  Set<ByteBuf> buffers = ConcurrentHashMap.newKeySet();

  ScheduledExecutorService executorService;
  Clock clock = new Clock();

  AutoCloseable mocks;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    executorService = Executors.newScheduledThreadPool(2);
    when(channel.alloc()).thenReturn(ByteBufAllocator.DEFAULT);
    when(channel.writeAndFlush(Mockito.any())).thenReturn(channelFuture);
    when(client.allocateNoCheck(any(ByteBufAllocator.class), anyInt()))
        .thenAnswer(
            (Answer<ByteBuf>)
                invocation -> {
                  ByteBufAllocator allocator = invocation.getArgument(0);
                  int capacity = invocation.getArgument(1);
                  ByteBuf buffer = allocator.buffer(capacity);
                  buffers.add(buffer);
                  return buffer;
                });
    when(client.maxFrameSize()).thenReturn(Integer.MAX_VALUE);
    when(client.publishInternal(
            anyByte(),
            anyList(),
            any(OutboundEntityWriteCallback.class),
            any(ToLongFunction.class)))
        .thenAnswer(
            invocation ->
                client.publishInternal(
                    channel,
                    invocation.getArgument(0),
                    invocation.getArgument(1),
                    invocation.getArgument(2),
                    invocation.getArgument(3)));

    when(client.publishInternal(
            any(Channel.class),
            anyByte(),
            anyList(),
            any(OutboundEntityWriteCallback.class),
            any(ToLongFunction.class)))
        .thenCallRealMethod();
    when(env.scheduledExecutorService()).thenReturn(executorService);
    when(env.locator()).thenReturn(client);
    when(env.clock()).thenReturn(clock);
    when(env.codec()).thenReturn(new SimpleCodec());
    doAnswer(
            (Answer<Runnable>)
                invocationOnMock -> {
                  StreamProducer p = invocationOnMock.getArgument(0);
                  p.setClient(client);
                  p.setPublisherId((byte) 0);
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
  void confirmTimeout(long confirmTimeoutMs, long waitTime, int subEntrySize) throws Exception {
    Duration confirmTimeout = Duration.ofMillis(confirmTimeoutMs);
    clock.refresh();
    int messageCount = 10_000;
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
            null, "stream", subEntrySize, 100, Duration.ofMillis(100), 10_000, confirmTimeout, env);

    IntStream.range(0, messageCount)
        .forEach(
            i ->
                producer.send(
                    producer.messageBuilder().addData("".getBytes()).build(), confirmationHandler));

    IntStream.range(0, confirmedPart).forEach(publishingId -> producer.confirm(publishingId));
    assertThat(confirmedCount.get()).isEqualTo(expectedConfirmed);
    assertThat(erroredCount.get()).isZero();

    executorService.scheduleAtFixedRate(() -> clock.refresh(), 100, 100, TimeUnit.MILLISECONDS);

    Thread.sleep(waitTime);
    assertThat(confirmedCount.get()).isEqualTo(expectedConfirmed);
    if (confirmTimeout.isZero()) {
      assertThat(erroredCount.get()).isZero();
      assertThat(responseCodes).isEmpty();
    } else {
      assertThat(erroredCount.get()).isEqualTo(messageCount - expectedConfirmed);
      assertThat(responseCodes).hasSize(1).contains(Constants.CODE_PUBLISH_CONFIRM_TIMEOUT);
    }
  }
}
