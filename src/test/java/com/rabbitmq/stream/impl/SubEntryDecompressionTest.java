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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.ChunkChecksum;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.codec.ByteArrayEncodedMessage;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.compression.CompressionUtils.GzipCompressionCodec;
import com.rabbitmq.stream.compression.CompressionUtils.ZstdJniCompressionCodec;
import com.rabbitmq.stream.compression.DefaultCompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.CompressedEncodedMessageBatch;
import com.rabbitmq.stream.impl.ServerFrameHandler.DeliverVersion1FrameHandler;
import com.rabbitmq.stream.metrics.NoOpMetricsCollector;
import io.netty.buffer.AbstractByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SubEntryDecompressionTest {

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

  @BeforeEach
  void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    // an unstubbed mock returns 0, which rejects every entry, so every test needs these
    when(client.maxUncompressedSubEntryBatchSize())
        .thenReturn(Client.DEFAULT_MAX_UNCOMPRESSED_SUB_ENTRY_BATCH_SIZE);
    when(client.maxUncompressedSizePerChunk())
        .thenReturn(Client.DEFAULT_MAX_UNCOMPRESSED_SIZE_PER_CHUNK);
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  private void useCompressionCodec(CompressionCodec codec) throws Exception {
    Field field = Client.class.getDeclaredField("compressionCodecFactory");
    field.setAccessible(true);
    field.set(client, (CompressionCodecFactory) c -> codec);
  }

  private static ChannelHandlerContext mockContext(ByteBufAllocator allocator) {
    ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
    when(ctx.alloc()).thenReturn(allocator);
    return ctx;
  }

  // one sub-batch entry, as written in a chunk's entry list
  private static final class SubBatchEntry {
    final byte compressionCode;
    final int numRecordsInBatch;
    final int uncompressedDataSize;
    final byte[] payload;

    SubBatchEntry(
        byte compressionCode, int numRecordsInBatch, int uncompressedDataSize, byte[] payload) {
      this.compressionCode = compressionCode;
      this.numRecordsInBatch = numRecordsInBatch;
      this.uncompressedDataSize = uncompressedDataSize;
      this.payload = payload;
    }
  }

  // builds a chunk containing the given sub-batch entries, using the header fields under test
  private static ByteBuf buildChunk(List<SubBatchEntry> entries, long chunkNumRecords) {
    int totalPayloadSize = entries.stream().mapToInt(e -> e.payload.length).sum();
    ByteBuf bb = Utils.byteBufAllocator().buffer(64 + totalPayloadSize + 16 * entries.size());
    bb.writeShort(Utils.encodeRequestCode(Constants.COMMAND_DELIVER))
        .writeShort(Constants.VERSION_1)
        .writeByte(1) // subscription id
        .writeByte(1) // magic and version
        .writeByte(0) // chunk type
        .writeShort(entries.size()) // num entries
        .writeInt((int) chunkNumRecords) // num records
        .writeLong(System.currentTimeMillis())
        .writeLong(0) // epoch
        .writeLong(0) // offset
        .writeInt(0) // CRC, unchecked with ChunkChecksum.NO_OP
        .writeInt(0) // data length, unused with ChunkChecksum.NO_OP
        .writeInt(0) // trailer length
        .writeInt(0); // 4 reserved bytes

    for (SubBatchEntry entry : entries) {
      byte entryType = (byte) (0x80 | (entry.compressionCode << 4));
      bb.writeByte(entryType)
          .writeShort(entry.numRecordsInBatch)
          .writeInt(entry.uncompressedDataSize)
          .writeInt(entry.payload.length)
          .writeBytes(entry.payload);
    }
    return bb;
  }

  // builds a chunk containing a single sub-batch entry, using the header fields under test
  private static ByteBuf buildChunk(
      byte compressionCode,
      int declaredNumRecordsInBatch,
      int declaredUncompressedDataSize,
      byte[] entryPayload,
      long chunkNumRecords) {
    return buildChunk(
        List.of(
            new SubBatchEntry(
                compressionCode,
                declaredNumRecordsInBatch,
                declaredUncompressedDataSize,
                entryPayload)),
        chunkNumRecords);
  }

  private static byte[] randomBytes(int size, long seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  private static final class CompressedPayload {
    final byte[] bytes;
    final int uncompressedSize;
    final int recordCount;

    CompressedPayload(byte[] bytes, int uncompressedSize, int recordCount) {
      this.bytes = bytes;
      this.uncompressedSize = uncompressedSize;
      this.recordCount = recordCount;
    }
  }

  private static CompressedPayload compress(
      CompressionCodec codec, int messageCount, int messageSize) {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    CompressedEncodedMessageBatch batch =
        new CompressedEncodedMessageBatch(allocator, codec, messageCount);
    for (int i = 0; i < messageCount; i++) {
      byte[] body = randomBytes(messageSize, i);
      batch.add(new ByteArrayEncodedMessage(body.length, body));
    }
    batch.close();
    int uncompressedSize = batch.uncompressedSizeInBytes();
    ByteBuf out = allocator.buffer(batch.sizeInBytes());
    batch.write(out);
    byte[] bytes = new byte[out.readableBytes()];
    out.readBytes(bytes);
    out.release();
    return new CompressedPayload(bytes, uncompressedSize, messageCount);
  }

  // identical bodies compress at a much higher ratio than varying ones, which is what makes the
  // v1.9.0 MAX_COMPRESSION_RATIO guard reject legitimate repetitive traffic (heartbeats, etc.)
  private static CompressedPayload compressIdentical(
      CompressionCodec codec, int messageCount, int messageSize) {
    ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    CompressedEncodedMessageBatch batch =
        new CompressedEncodedMessageBatch(allocator, codec, messageCount);
    byte[] body = randomBytes(messageSize, 42);
    for (int i = 0; i < messageCount; i++) {
      batch.add(new ByteArrayEncodedMessage(body.length, body));
    }
    batch.close();
    int uncompressedSize = batch.uncompressedSizeInBytes();
    ByteBuf out = allocator.buffer(batch.sizeInBytes());
    batch.write(out);
    byte[] bytes = new byte[out.readableBytes()];
    out.readBytes(bytes);
    out.release();
    return new CompressedPayload(bytes, uncompressedSize, messageCount);
  }

  // counts and records the heap buffers requested through heapBuffer(initialCapacity,
  // maxCapacity), so tests can assert on allocation sizing and on release
  private static final class RecordingByteBufAllocator extends AbstractByteBufAllocator {

    final List<int[]> requests = new ArrayList<>();
    final List<ByteBuf> buffers = new ArrayList<>();

    RecordingByteBufAllocator() {
      super(false);
    }

    @Override
    protected ByteBuf newHeapBuffer(int initialCapacity, int maxCapacity) {
      requests.add(new int[] {initialCapacity, maxCapacity});
      ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.heapBuffer(initialCapacity, maxCapacity);
      buffers.add(buffer);
      return buffer;
    }

    @Override
    protected ByteBuf newDirectBuffer(int initialCapacity, int maxCapacity) {
      return UnpooledByteBufAllocator.DEFAULT.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public boolean isDirectBufferPooled() {
      return false;
    }
  }

  private void deliver(ByteBuf bb, ChannelHandlerContext ctx, AtomicInteger messageCount) {
    bb.readShort(); // command key
    bb.readShort(); // command version
    DeliverVersion1FrameHandler.handleDeliverVersion1(
        bb,
        client,
        ctx,
        (client, subscriptionId, offset, count, sizeOfData) -> null,
        (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext, message) ->
            messageCount.incrementAndGet(),
        (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {},
        NO_OP_CODEC,
        ChunkChecksum.NO_OP,
        NoOpMetricsCollector.SINGLETON);
  }

  @Test
  void zeroUncompressedSizeMustThrow() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 5, 20);
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            0, // declares 0 uncompressed bytes
            payload.bytes,
            payload.recordCount);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () ->
            assertThatThrownBy(() -> deliver(bb, ctx, new AtomicInteger()))
                .isInstanceOf(StreamException.class));
    bb.release();
  }

  @Test
  void decompressedDataExceedingDeclaredSizeMustThrowAndStayBounded() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 200, 50);
    int declaredUncompressedSize = payload.uncompressedSize / 2; // declares half the truth
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            declaredUncompressedSize,
            payload.bytes,
            payload.recordCount);
    RecordingByteBufAllocator allocator = new RecordingByteBufAllocator();
    ChannelHandlerContext ctx = mockContext(allocator);

    assertThatThrownBy(() -> deliver(bb, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class);

    assertThat(allocator.requests).hasSize(1);
    assertThat(allocator.requests.get(0)[1]).isEqualTo(declaredUncompressedSize);
    assertThat(allocator.buffers.get(0).refCnt()).isZero();
    bb.release();
  }

  @Test
  void overDeclaredSizeMustOnlyAllocateInitialBufferSize() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 10, 50);
    int declaredUncompressedSize = payload.bytes.length * 1024; // declares far more than the truth
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            declaredUncompressedSize,
            payload.bytes,
            payload.recordCount);
    RecordingByteBufAllocator allocator = new RecordingByteBufAllocator();
    ChannelHandlerContext ctx = mockContext(allocator);
    AtomicInteger messageCount = new AtomicInteger();

    assertThatCode(() -> deliver(bb, ctx, messageCount)).doesNotThrowAnyException();

    assertThat(messageCount).hasValue(payload.recordCount);
    assertThat(allocator.requests).hasSize(1);
    assertThat(allocator.requests.get(0)[0])
        .isEqualTo(ServerFrameHandler.INITIAL_DECOMPRESSION_BUFFER_SIZE);
    assertThat(allocator.requests.get(0)[1]).isEqualTo(declaredUncompressedSize);
    assertThat(allocator.buffers.get(0).refCnt()).isZero();
    bb.release();
  }

  @Test
  void corruptMessageInSubBatchMustReleaseDecompressionBuffer() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 5, 20);
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            payload.uncompressedSize,
            payload.bytes,
            payload.recordCount);
    RecordingByteBufAllocator allocator = new RecordingByteBufAllocator();
    ChannelHandlerContext ctx = mockContext(allocator);
    bb.readShort(); // command key
    bb.readShort(); // command version

    Codec corruptCodec =
        new Codec() {
          @Override
          public EncodedMessage encode(Message message) {
            return null;
          }

          @Override
          public Message decode(ByteBuf buf, int length) {
            buf.skipBytes(length);
            throw new StreamException("corrupt message rejected");
          }

          @Override
          public MessageBuilder messageBuilder() {
            return null;
          }
        };

    assertThatThrownBy(
            () ->
                DeliverVersion1FrameHandler.handleDeliverVersion1(
                    bb,
                    client,
                    ctx,
                    (client, subscriptionId, offset, count, sizeOfData) -> null,
                    (subscriptionId,
                        offset,
                        chunkTimestamp,
                        committedChunkId,
                        chunkContext,
                        message) -> {},
                    (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {},
                    corruptCodec,
                    ChunkChecksum.NO_OP,
                    NoOpMetricsCollector.SINGLETON))
        .isInstanceOf(StreamException.class);

    assertThat(allocator.requests).hasSize(1);
    assertThat(allocator.buffers.get(0).refCnt()).isZero();
    bb.release();
  }

  @Test
  void recordCountLargerThanDecompressedDataMustThrowStreamException() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 5, 20);
    int bogusRecordCount = 1000; // more than the decompressed data (5 records) can hold
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(),
            bogusRecordCount,
            payload.uncompressedSize,
            payload.bytes,
            bogusRecordCount);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    assertThatThrownBy(() -> deliver(bb, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class)
        .hasMessageContaining("Invalid declared count");
    bb.release();
  }

  @Test
  void declaredSizeJustBelowCapIsAccepted() {
    int maxUncompressedSize = 10_000;
    when(client.maxUncompressedSubEntryBatchSize()).thenReturn(maxUncompressedSize);
    byte[] payload = new byte[100];
    ByteBuf bb = buildChunk(Compression.NONE.code(), 1, maxUncompressedSize - 1, payload, 1);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    assertThatCode(() -> deliver(bb, ctx, new AtomicInteger())).doesNotThrowAnyException();
    bb.release();
  }

  @Test
  void declaredSizeJustAboveCapIsRejected() {
    int maxUncompressedSize = 10_000;
    when(client.maxUncompressedSubEntryBatchSize()).thenReturn(maxUncompressedSize);
    byte[] payload = new byte[100];
    ByteBuf bb = buildChunk(Compression.NONE.code(), 1, maxUncompressedSize + 1, payload, 1);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    assertThatThrownBy(() -> deliver(bb, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class)
        .hasMessageContaining("Uncompressed sub-entry batch size");
    bb.release();
  }

  @Test
  void capIsConfigurable() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 50, 50);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    when(client.maxUncompressedSubEntryBatchSize()).thenReturn(payload.uncompressedSize + 1);
    ByteBuf accepted =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            payload.uncompressedSize,
            payload.bytes,
            payload.recordCount);
    assertThatCode(() -> deliver(accepted, ctx, new AtomicInteger())).doesNotThrowAnyException();
    accepted.release();

    when(client.maxUncompressedSubEntryBatchSize()).thenReturn(payload.uncompressedSize - 1);
    ByteBuf rejected =
        buildChunk(
            Compression.GZIP.code(),
            payload.recordCount,
            payload.uncompressedSize,
            payload.bytes,
            payload.recordCount);
    assertThatThrownBy(() -> deliver(rejected, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class)
        .hasMessageContaining("maxUncompressedSubEntryBatchSize");
    rejected.release();
  }

  @Test
  void ratioRegressionHighCompressionRatioIsAccepted() throws Exception {
    // the v1.9.0 bug: a legitimate, highly compressible batch rejected by MAX_COMPRESSION_RATIO
    useCompressionCodec(new ZstdJniCompressionCodec());
    CompressedPayload payload = compressIdentical(new ZstdJniCompressionCodec(), 5000, 60);
    assertThat(payload.uncompressedSize).isGreaterThan(payload.bytes.length * 2000);
    ByteBuf bb =
        buildChunk(
            Compression.ZSTD.code(),
            payload.recordCount,
            payload.uncompressedSize,
            payload.bytes,
            payload.recordCount);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);
    AtomicInteger messageCount = new AtomicInteger();

    assertThatCode(() -> deliver(bb, ctx, messageCount)).doesNotThrowAnyException();

    assertThat(messageCount).hasValue(payload.recordCount);
    bb.release();
  }

  @Test
  void chunkBudgetAcceptsJustUnderAndRejectsJustOver() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 1, 5);
    when(client.maxUncompressedSizePerChunk()).thenReturn(1000);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    List<SubBatchEntry> underBudget =
        List.of(
            new SubBatchEntry(Compression.GZIP.code(), payload.recordCount, 499, payload.bytes),
            new SubBatchEntry(Compression.GZIP.code(), payload.recordCount, 499, payload.bytes));
    ByteBuf accepted = buildChunk(underBudget, 2L * payload.recordCount);
    assertThatCode(() -> deliver(accepted, ctx, new AtomicInteger())).doesNotThrowAnyException();
    accepted.release();

    // asserts the budget is per chunk, not accumulated across calls
    ByteBuf acceptedAgain = buildChunk(underBudget, 2L * payload.recordCount);
    assertThatCode(() -> deliver(acceptedAgain, ctx, new AtomicInteger()))
        .doesNotThrowAnyException();
    acceptedAgain.release();

    List<SubBatchEntry> overBudget =
        List.of(
            new SubBatchEntry(Compression.GZIP.code(), payload.recordCount, 501, payload.bytes),
            new SubBatchEntry(Compression.GZIP.code(), payload.recordCount, 501, payload.bytes));
    ByteBuf rejected = buildChunk(overBudget, 2L * payload.recordCount);
    assertThatThrownBy(() -> deliver(rejected, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class)
        .hasMessageContaining("maxUncompressedSizePerChunk");
    rejected.release();
  }

  @Test
  void budgetCheckedBeforeAllocatingBuffer() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 1, 5);
    when(client.maxUncompressedSizePerChunk()).thenReturn(10);
    ByteBuf bb =
        buildChunk(
            Compression.GZIP.code(), payload.recordCount, 500, payload.bytes, payload.recordCount);
    RecordingByteBufAllocator allocator = new RecordingByteBufAllocator();
    ChannelHandlerContext ctx = mockContext(allocator);

    assertThatThrownBy(() -> deliver(bb, ctx, new AtomicInteger()))
        .isInstanceOf(StreamException.class)
        .hasMessageContaining("maxUncompressedSizePerChunk");

    assertThat(allocator.requests).isEmpty();
    bb.release();
  }

  @Test
  void noneEntriesDoNotConsumeBudget() {
    when(client.maxUncompressedSizePerChunk()).thenReturn(100);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);
    byte[] zeroLengthRecord = new byte[4]; // a single record whose entrySize is 0

    List<SubBatchEntry> entries =
        List.of(
            new SubBatchEntry(Compression.NONE.code(), 1, 1000, zeroLengthRecord),
            new SubBatchEntry(Compression.NONE.code(), 1, 1000, zeroLengthRecord));
    ByteBuf bb = buildChunk(entries, 2);

    assertThatCode(() -> deliver(bb, ctx, new AtomicInteger())).doesNotThrowAnyException();
    bb.release();
  }

  @Test
  void zeroRecordSubBatchIsAcceptedNotRejected() throws Exception {
    useCompressionCodec(new GzipCompressionCodec());
    CompressedPayload payload = compress(new GzipCompressionCodec(), 0, 20);
    ByteBuf bb = buildChunk(Compression.GZIP.code(), 0, payload.uncompressedSize, payload.bytes, 0);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);

    assertThatCode(() -> deliver(bb, ctx, new AtomicInteger())).doesNotThrowAnyException();
    bb.release();
  }

  static List<CompressionCodec> codecs() {
    CompressionCodecFactory factory = new DefaultCompressionCodecFactory();
    return Arrays.stream(Compression.values())
        .filter(c -> c != Compression.NONE)
        .map(factory::get)
        .collect(Collectors.toList());
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void decompressionBufferGrowsPastInitialCapacityForLargeBatches(CompressionCodec codec)
      throws Exception {
    useCompressionCodec(codec);
    CompressedPayload payload = compress(codec, 2000, 50);
    assertThat(payload.uncompressedSize)
        .isGreaterThan(ServerFrameHandler.INITIAL_DECOMPRESSION_BUFFER_SIZE);
    ByteBuf bb =
        buildChunk(
            codec.code(),
            payload.recordCount,
            payload.uncompressedSize,
            payload.bytes,
            payload.recordCount);
    ChannelHandlerContext ctx = mockContext(UnpooledByteBufAllocator.DEFAULT);
    AtomicInteger messageCount = new AtomicInteger();

    assertThatCode(() -> deliver(bb, ctx, messageCount)).doesNotThrowAnyException();

    assertThat(messageCount).hasValue(payload.recordCount);
    bb.release();
  }
}
