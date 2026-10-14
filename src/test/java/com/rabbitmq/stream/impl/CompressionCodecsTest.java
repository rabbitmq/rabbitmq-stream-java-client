// Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.Codec.EncodedMessage;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.ByteArrayEncodedMessage;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressGzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressLz4CompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressSnappyCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.CommonsCompressZstdCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.GzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.Lz4JavaCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.XerialSnappyCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.ZstdJniCompressionCodec;
import com.rabbitmq.stream.impl.Client.CompressedEncodedMessageBatch;
import com.rabbitmq.stream.impl.Client.EncodedMessageBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class CompressionCodecsTest {

  static Stream<Arguments> compressedEncodedMessageBatch() {
    return Stream.of(
        Arguments.of(new GzipCompressionCodec(), new GzipCompressionCodec()),
        Arguments.of(
            new CommonsCompressGzipCompressionCodec(), new CommonsCompressGzipCompressionCodec()),
        Arguments.of(new GzipCompressionCodec(), new CommonsCompressGzipCompressionCodec()),
        Arguments.of(new XerialSnappyCompressionCodec(), new XerialSnappyCompressionCodec()),
        Arguments.of(
            new CommonsCompressSnappyCompressionCodec(),
            new CommonsCompressSnappyCompressionCodec()),
        Arguments.of(
            new XerialSnappyCompressionCodec(), new CommonsCompressSnappyCompressionCodec()),
        Arguments.of(new Lz4JavaCompressionCodec(), new Lz4JavaCompressionCodec()),
        Arguments.of(
            new CommonsCompressLz4CompressionCodec(), new CommonsCompressLz4CompressionCodec()),
        Arguments.of(new Lz4JavaCompressionCodec(), new CommonsCompressLz4CompressionCodec()),
        Arguments.of(new ZstdJniCompressionCodec(), new ZstdJniCompressionCodec()),
        Arguments.of(
            new CommonsCompressZstdCompressionCodec(), new CommonsCompressZstdCompressionCodec()),
        Arguments.of(new ZstdJniCompressionCodec(), new CommonsCompressZstdCompressionCodec()));
  }

  @ParameterizedTest
  @MethodSource
  void compressedEncodedMessageBatch(
      CompressionCodec compressionCodec, CompressionCodec decompressionCodec) throws IOException {
    assertThat(compressionCodec.code()).isEqualTo(decompressionCodec.code());
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    int messageCount = 100;
    EncodedMessageBatch encodedMessageBatch =
        new CompressedEncodedMessageBatch(allocator, compressionCodec, messageCount);
    List<EncodedMessage> encodedMessages = new ArrayList<>(messageCount);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              byte[] body = ("message " + i).getBytes(StandardCharsets.UTF_8);
              EncodedMessage encodedMessage = new ByteArrayEncodedMessage(body.length, body);
              encodedMessageBatch.add(encodedMessage);
              encodedMessages.add(encodedMessage);
            });
    encodedMessageBatch.close();

    int plainSize =
        encodedMessages.stream().mapToInt(EncodedMessage::getSize).sum()
            + encodedMessages.size() * 4;
    int compressedSize = encodedMessageBatch.sizeInBytes();

    ByteBuf destinationBb = allocator.buffer(compressedSize);
    encodedMessageBatch.write(destinationBb);

    assertThat(compressedSize).isLessThan(plainSize);
    assertThat(destinationBb.writerIndex()).isEqualTo(compressedSize);

    ByteBuf outBb = allocator.buffer(plainSize);
    destinationBb.readerIndex(0);
    InputStream inputStream = decompressionCodec.decompress(new ByteBufInputStream(destinationBb));
    int n;
    while (-1 != (n = inputStream.read())) {
      outBb.writeByte(n);
    }

    List<EncodedMessage> decompressedMessages = new ArrayList<>(messageCount);
    while (outBb.isReadable()) {
      int size = outBb.readInt();
      byte[] msg = new byte[size];
      outBb.readBytes(msg);
      decompressedMessages.add(new ByteArrayEncodedMessage(size, msg));
    }

    assertThat(decompressedMessages).hasSameSizeAs(encodedMessages);
    IntStream.range(0, messageCount)
        .forEach(
            i -> {
              EncodedMessage originalMessage = encodedMessages.get(i);
              EncodedMessage decompressedMessage = decompressedMessages.get(i);
              assertThat(decompressedMessage.getSize()).isEqualTo(originalMessage.getSize());
              assertThat(TestUtils.encodedMessageByteBuf(decompressedMessage))
                  .isEqualTo(TestUtils.encodedMessageByteBuf(originalMessage));
            });

    destinationBb.release();
    outBb.release();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 100, 10_000, 100_000, 1_000_000})
  void maxCompressedLengthShouldBeUpperBoundForIncompressibleData(int messageSize) {
    byte[] body = new byte[messageSize];
    new Random().nextBytes(body);
    EncodedMessage encodedMessage = new ByteArrayEncodedMessage(body.length, body);
    compressedEncodedMessageBatch()
        .map(arguments -> (CompressionCodec) arguments.get()[0])
        .forEach(
            codec -> {
              EncodedMessageBatch batch =
                  new CompressedEncodedMessageBatch(ByteBufAllocator.DEFAULT, codec, 1);
              batch.add(encodedMessage);
              batch.close();
              assertThat(batch.sizeInBytes())
                  .as(codec.toString())
                  .isLessThanOrEqualTo(codec.maxCompressedLength(batch.uncompressedSizeInBytes()));
              ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(batch.sizeInBytes());
              batch.write(bb);
              bb.release();
            });
  }

  @ParameterizedTest
  @MethodSource("compressedEncodedMessageBatch")
  void compressedEncodedMessageBatchCanBeWrittenSeveralTimes(
      CompressionCodec compressionCodec, CompressionCodec ignored) {
    TrackingAllocator allocator = new TrackingAllocator();
    EncodedMessageBatch batch =
        new CompressedEncodedMessageBatch(allocator.allocator, compressionCodec, 10);
    IntStream.range(0, 10)
        .forEach(
            i -> {
              byte[] body = ("message " + i).getBytes(StandardCharsets.UTF_8);
              batch.add(new ByteArrayEncodedMessage(body.length, body));
            });
    batch.close();

    ByteBuf first = ByteBufAllocator.DEFAULT.buffer(batch.sizeInBytes());
    ByteBuf second = ByteBufAllocator.DEFAULT.buffer(batch.sizeInBytes());
    try {
      batch.write(first);
      batch.write(second);
      assertThat(first.writerIndex()).isEqualTo(batch.sizeInBytes());
      assertThat(second).isEqualTo(first);
      assertThat(allocator.buffers).hasSize(1);
      assertThat(allocator.allReleased()).isTrue();
    } finally {
      first.release();
      second.release();
    }
  }

  @Test
  void compressedEncodedMessageBatchShouldReleaseBufferWhenCompressionFails() {
    TrackingAllocator allocator = new TrackingAllocator();
    CompressionCodec failingCodec =
        new GzipCompressionCodec() {
          @Override
          public OutputStream compress(OutputStream target) {
            throw new IllegalStateException("compression failure");
          }
        };
    EncodedMessageBatch batch =
        new CompressedEncodedMessageBatch(allocator.allocator, failingCodec, 1);
    byte[] body = "message".getBytes(StandardCharsets.UTF_8);
    batch.add(new ByteArrayEncodedMessage(body.length, body));
    assertThatThrownBy(batch::close).isInstanceOf(StreamException.class);
    assertThat(allocator.buffers).hasSize(1);
    assertThat(allocator.allReleased()).isTrue();
  }

  private static class TrackingAllocator {

    private final List<ByteBuf> buffers = new ArrayList<>();
    private final ByteBufAllocator allocator = mock(ByteBufAllocator.class);

    private TrackingAllocator() {
      when(this.allocator.buffer(anyInt()))
          .thenAnswer(
              invocation -> {
                ByteBuf buffer = Unpooled.buffer(invocation.getArgument(0));
                this.buffers.add(buffer);
                return buffer;
              });
    }

    private boolean allReleased() {
      return this.buffers.stream().allMatch(b -> b.refCnt() == 0);
    }
  }
}
