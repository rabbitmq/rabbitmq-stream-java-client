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

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.ServerFrameHandler.DeliverVersion1FrameHandler;
import com.rabbitmq.stream.metrics.NoOpMetricsCollector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

public class DeliveryTest {

  static final Codec NO_OP_CODEC =
      new Codec() {
        @Override
        public EncodedMessage encode(Message message) {
          return null;
        }

        @Override
        public Message decode(byte[] data) {
          return null;
        }

        @Override
        public MessageBuilder messageBuilder() {
          return null;
        }
      };

  ByteBuf generateFrameBuffer(
      int nbMessages, long chunkOffset, int dataSize, Iterable<byte[]> messages) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(1024);
    bb.writeShort(Utils.encodeRequestCode(Constants.COMMAND_DELIVER))
        .writeShort(Constants.VERSION_1)
        .writeByte(1) // subscription id
        .writeByte(1) // magic and version
        .writeByte(0) // chunk type, always 0 in our case
        .writeShort(nbMessages) // num entries
        .writeInt(nbMessages) // num messages
        .writeLong(System.currentTimeMillis())
        .writeLong(0) // epoch
        .writeLong(chunkOffset) // offset
        .writeInt(0) // CRC
        .writeInt(dataSize) // data size
        .writeInt(0) // trailer size
        .writeInt(0); // 4 reserved bytes

    for (byte[] message : messages) {
      bb.writeInt(message.length).writeBytes(message);
    }
    return bb;
  }

  Iterable<byte[]> generateMessages(int nbMessages) {
    Random random = new Random();
    List<byte[]> messages = new ArrayList<>();
    for (int i = 0; i < nbMessages; i++) {
      // <<0=SimpleEntryType:1, Size:31/unsigned,Data:Size/binary>>
      int messageSize = random.nextInt(100) + 1;
      messages.add(new byte[messageSize]);
    }
    return messages;
  }

  int computeDataSize(Iterable<byte[]> messages) {
    int dataSize = 0;
    for (byte[] message : messages) {
      dataSize += 4 + message.length;
    }
    return dataSize;
  }

  @Test
  void handleDeliveryShouldFilterMessagesBeforeSubscriptionOffsetAndCallCallbacks() {
    class TestConfig {
      final long chunkOffset;
      final long subscriptionOffset;

      public TestConfig(long chunkOffset, long subscriptionOffset) {
        this.chunkOffset = chunkOffset;
        this.subscriptionOffset = subscriptionOffset;
      }
    }

    Stream.of(
            new TestConfig(10, 10), // no filtering
            new TestConfig(10, 20), // filtering
            new TestConfig(Long.MAX_VALUE - 20, Long.MAX_VALUE - 10) // large unsigned long
            )
        .forEach(
            config -> {
              long chunkOffset = config.chunkOffset;
              long subscriptionOffset = config.subscriptionOffset;

              int nbMessages = (int) (subscriptionOffset - chunkOffset) + 100;
              Iterable<byte[]> messages = generateMessages(nbMessages);
              int dataSize = computeDataSize(messages);

              ByteBuf bb = generateFrameBuffer(nbMessages, chunkOffset, dataSize, messages);

              bb.readShort(); // read command key
              bb.readShort(); // read command version

              AtomicInteger chunkCountInCallback = new AtomicInteger();
              AtomicLong messageCountInCallback = new AtomicLong();

              List<Client.SubscriptionOffset> subscriptionOffsets = new ArrayList<>();
              if (chunkOffset != subscriptionOffset) {
                subscriptionOffsets.add(new Client.SubscriptionOffset(1, subscriptionOffset));
              }

              AtomicLong filteredMessageCount = new AtomicLong();

              DeliverVersion1FrameHandler.handleDeliverVersion1(
                  bb,
                  null,
                  (client, subscriptionId, offset, messageCount, sizeOfData) -> {
                    assertThat(messageCount).isEqualTo(nbMessages);
                    chunkCountInCallback.incrementAndGet();
                    return null;
                  },
                  (subscriptionId,
                      offset,
                      chunkTimestamp,
                      committedChunkId,
                      chunkContext,
                      message) -> messageCountInCallback.incrementAndGet(),
                  (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {
                    filteredMessageCount.incrementAndGet();
                  },
                  NO_OP_CODEC,
                  subscriptionOffsets,
                  ChunkChecksum.NO_OP,
                  NoOpMetricsCollector.SINGLETON);

              long expectedMessageCount = nbMessages - (subscriptionOffset - chunkOffset);
              long expectedFilteredMessageCount = nbMessages - expectedMessageCount;
              assertThat(chunkCountInCallback).hasValue(1);
              assertThat(messageCountInCallback).hasValue(expectedMessageCount);
              assertThat(filteredMessageCount).hasValue(expectedFilteredMessageCount);
              bb.release();
            });
  }
}
