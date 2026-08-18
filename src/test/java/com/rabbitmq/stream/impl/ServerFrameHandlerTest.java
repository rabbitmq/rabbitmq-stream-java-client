// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.impl.ServerFrameHandler.CHUNK_COST_DELIVER_V1_OFFSET;
import static com.rabbitmq.stream.impl.ServerFrameHandler.CHUNK_COST_DELIVER_V2_OFFSET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.impl.ServerFrameHandler.DeliverVersion1FrameHandler;
import com.rabbitmq.stream.impl.ServerFrameHandler.DeliverVersion2FrameHandler;
import com.rabbitmq.stream.impl.ServerFrameHandler.FrameHandlerInfo;
import com.rabbitmq.stream.metrics.NoOpMetricsCollector;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class ServerFrameHandlerTest {

  static final byte SUBSCRIPTION_ID = 1;

  static final Codec NO_OP_CODEC =
      new Codec() {
        @Override
        public EncodedMessage encode(Message message) {
          return null;
        }

        @Override
        public Message decode(ByteBuf buf, int length) {
          buf.skipBytes(length);
          return null;
        }

        @Override
        public MessageBuilder messageBuilder() {
          return null;
        }
      };

  @Mock Client client;
  AutoCloseable mocks;
  ChannelHandlerContext ctx;

  @BeforeEach
  void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);
    ctx = Mockito.mock(ChannelHandlerContext.class);
    when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);
    when(client.extractInitialSubscriptionOffset(SUBSCRIPTION_ID)).thenReturn(-1L);
    setClientField("codec", NO_OP_CODEC);
    setClientField("chunkChecksum", ChunkChecksum.NO_OP);
    setClientField("metricsCollector", NoOpMetricsCollector.SINGLETON);
    setClientField(
        "messageListener",
        (Client.MessageListener)
            (subscriptionId,
                offset,
                chunkTimestamp,
                committedChunkId,
                chunkContext,
                message) -> {});
    setClientField(
        "messageIgnoredListener",
        (Client.MessageIgnoredListener)
            (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {});
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  private void setClientField(String name, Object value) throws Exception {
    Field field = Client.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(client, value);
  }

  private static ByteBuf chunkFrameBody(byte[] messageBody) {
    ByteBuf bb = Utils.byteBufAllocator().buffer(256);
    bb.writeByte(1) // magic and version
        .writeByte(0) // chunk type, always 0 in our case
        .writeShort(1) // num entries
        .writeInt(1) // num records
        .writeLong(System.currentTimeMillis())
        .writeLong(0) // epoch
        .writeLong(0) // chunk offset
        .writeInt(0) // CRC
        .writeInt(4 + messageBody.length) // data size
        .writeInt(0) // trailer size
        .writeInt(0); // 4 reserved bytes
    bb.writeInt(messageBody.length).writeBytes(messageBody);
    return bb;
  }

  @Test
  void commandVersionsHasDeliver() {
    FrameHandlerInfo deliverInfo =
        ServerFrameHandler.commandVersions().stream()
            .filter(info -> info.getKey() == Constants.COMMAND_DELIVER)
            .findFirst()
            .get();

    assertThat(deliverInfo.getKey()).isEqualTo(Constants.COMMAND_DELIVER);
    assertThat(deliverInfo.getMinVersion()).isEqualTo(Constants.VERSION_1);
    assertThat(deliverInfo.getMaxVersion()).isGreaterThanOrEqualTo(Constants.VERSION_2);
  }

  @Test
  void deliverVersion1ChunkByteCountIsFrameSizeMinus5() throws Exception {
    AtomicLong capturedChunkByteCount = new AtomicLong(-1);
    setClientField(
        "chunkListener",
        (Client.ChunkListener)
            (client, subscriptionId, offset, messageCount, dataSize, chunkByteCount) -> {
              capturedChunkByteCount.set(chunkByteCount);
              return null;
            });

    ByteBuf bb = Utils.byteBufAllocator().buffer(256);
    bb.writeShort(Utils.encodeRequestCode(Constants.COMMAND_DELIVER))
        .writeShort(Constants.VERSION_1)
        .writeByte(SUBSCRIPTION_ID);
    ByteBuf chunk = chunkFrameBody(new byte[20]);
    bb.writeBytes(chunk);
    chunk.release();

    int frameSize = bb.readableBytes();
    bb.readShort(); // command key
    bb.readShort(); // command version

    new DeliverVersion1FrameHandler().handle(client, frameSize, ctx, bb);

    assertThat(capturedChunkByteCount.get()).isEqualTo(frameSize - CHUNK_COST_DELIVER_V1_OFFSET);
  }

  @Test
  void deliverVersion2ChunkByteCountIsFrameSizeMinus13() throws Exception {
    AtomicLong capturedChunkByteCount = new AtomicLong(-1);
    setClientField(
        "chunkListener",
        (Client.ChunkListener)
            (client, subscriptionId, offset, messageCount, dataSize, chunkByteCount) -> {
              capturedChunkByteCount.set(chunkByteCount);
              return null;
            });

    ByteBuf bb = Utils.byteBufAllocator().buffer(256);
    bb.writeShort(Utils.encodeRequestCode(Constants.COMMAND_DELIVER))
        .writeShort(Constants.VERSION_2)
        .writeByte(SUBSCRIPTION_ID)
        .writeLong(0); // committed chunk id
    ByteBuf chunk = chunkFrameBody(new byte[20]);
    bb.writeBytes(chunk);
    chunk.release();

    int frameSize = bb.readableBytes();
    bb.readShort(); // command key
    bb.readShort(); // command version

    new DeliverVersion2FrameHandler().handle(client, frameSize, ctx, bb);

    assertThat(capturedChunkByteCount.get()).isEqualTo(frameSize - CHUNK_COST_DELIVER_V2_OFFSET);
  }
}
