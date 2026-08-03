// Copyright (c) 2026 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.codec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.amqp.UnsignedInteger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class Amqp10DecodingSafetyTest {

  private static final Codec CODEC = new InternalCodec();

  private static byte[] hexToBytes(String hex) {
    String clean = hex.replace(" ", "");
    byte[] data = new byte[clean.length() / 2];
    for (int i = 0; i < clean.length(); i += 2) {
      data[i / 2] =
          (byte)
              ((Character.digit(clean.charAt(i), 16) << 4)
                  + Character.digit(clean.charAt(i + 1), 16));
    }
    return data;
  }

  private static ByteBuf heapBuf(String hex) {
    return Unpooled.wrappedBuffer(hexToBytes(hex));
  }

  // --- Attack cases: one per finding ---

  @Test
  void vbin32HugeLengthRejected() {
    ByteBuf buf = heapBuf("b0 7fffffff");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void vbin32NegativeLengthRejected() {
    ByteBuf buf = heapBuf("b0 ffffffff");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void str32HugeLengthOnDirectBufferRejected() {
    byte[] data = hexToBytes("b1 7fffffff");
    ByteBuf buf = Unpooled.directBuffer(data.length).writeBytes(data);
    try {
      assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
    } finally {
      buf.release();
    }
  }

  @Test
  void sym32HugeLengthRejected() {
    ByteBuf buf = heapBuf("b3 7fffffff");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void list32HugeSizeRejected() {
    ByteBuf buf = heapBuf("d0 7fffffff");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void map32HugeSizeRejected() {
    ByteBuf buf = heapBuf("d1 7fffffff");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void mapWithOddElementCountRejected() {
    ByteBuf buf = heapBuf("c1 02 01 40");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void array32OfDoublesHugeCountRejected() {
    ByteBuf buf = heapBuf("f0 00000005 0fffffff 82");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void array32OfZeroWidthElementsHugeCountRejected() {
    ByteBuf buf = heapBuf("f0 00000005 3fffffff 41");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void array32OfNullsWellFormedButRejectedByDesign() {
    ByteBuf buf = heapBuf("f0 00000005 000003e8 40");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void arrayOverDeclaringItsSizeRejected() {
    ByteBuf buf = heapBuf("f0 0000000a 00000001 51 07 00000000");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void array32WithNoRoomForElementConstructorRejected() {
    ByteBuf buf = heapBuf("f0 00000004 00000000");
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void deepNestingRejected() {
    byte[] data = new byte[200_002];
    // 200 000 described-type constructors (0x00), then a described type value (NULL NULL)
    data[200_000] = Amqp10.NULL;
    data[200_001] = Amqp10.NULL;
    ByteBuf buf = Unpooled.wrappedBuffer(data);
    assertThatThrownBy(() -> Amqp10.readObject(buf)).isInstanceOf(StreamException.class);
  }

  @Test
  void nullMapKeyInAnnotationsRejected() {
    ByteBuf buf = heapBuf("00 53 72 c1 03 02 40 40");
    assertThatThrownBy(() -> Amqp10.decodeMessage(buf, buf.readableBytes()))
        .isInstanceOf(StreamException.class);
  }

  @Test
  void dataSectionHoldingAStringRejected() {
    ByteBuf buf = heapBuf("00 53 75 a1 01 61");
    assertThatThrownBy(() -> Amqp10.decodeMessage(buf, buf.readableBytes()))
        .isInstanceOf(StreamException.class);
  }

  @Test
  void list32HugeCountValidSizeDecodesToEmptyList() {
    ByteBuf buf = heapBuf("d0 00000004 3fffffff");
    Object result = Amqp10.readObject(buf);
    assertThat(result).isInstanceOf(List.class);
    assertThat((List<?>) result).isEmpty();
  }

  @Test
  void map32HugeCountValidSizeDecodesToEmptyMap() {
    ByteBuf buf = heapBuf("d1 00000004 3fffffff");
    Object result = Amqp10.readObject(buf);
    assertThat(result).isInstanceOf(Map.class);
    assertThat((Map<?, ?>) result).isEmpty();
  }

  @Test
  void map32WithWrongDeclaredCountOverValidRegionDecodesCorrectly() {
    // declared count is 3 (odd, and wrong: only one entry is actually present), size is correct
    ByteBuf buf = heapBuf("d1 00000009 00000003 a1 01 61 52 07");
    Object result = Amqp10.readObject(buf);
    assertThat(result).isInstanceOf(Map.class);
    Map<?, ?> map = (Map<?, ?>) result;
    assertThat(map).hasSize(1);
    assertThat(map.get("a")).isEqualTo(UnsignedInteger.valueOf(7));
  }

  // --- Invariant cases ---

  @Test
  void messageEmbeddedInLargerBufferDoesNotReadFiller() {
    byte[] message = hexToBytes("00 53 77 a1 02 6869"); // amqp-value: STR8 "hi"
    byte[] filler = new byte[10];
    java.util.Arrays.fill(filler, (byte) 0xFF);
    ByteBuf buf = Unpooled.wrappedBuffer(message, filler);

    Amqp10.DecodedMessage decoded = Amqp10.decodeMessage(buf, message.length);

    assertThat(decoded.body).isEqualTo("hi");
    assertThat(buf.readerIndex()).isEqualTo(message.length);
    assertThat(buf.getByte(buf.readerIndex())).isEqualTo((byte) 0xFF);
  }

  @Test
  void readerIndexLandsExactlyOnMessageEndOnSuccessAndFailure() {
    byte[] validMessage = hexToBytes("00 53 77 a1 02 6869"); // amqp-value: STR8 "hi"
    ByteBuf validBuf = Unpooled.wrappedBuffer(validMessage);
    Amqp10.decodeMessage(validBuf, validMessage.length);
    assertThat(validBuf.readerIndex()).isEqualTo(validMessage.length);

    byte[] invalidMessage = hexToBytes("00 53 72 c1 03 02 40 40"); // null annotation key
    ByteBuf invalidBuf = Unpooled.wrappedBuffer(invalidMessage);
    assertThatThrownBy(() -> Amqp10.decodeMessage(invalidBuf, invalidMessage.length))
        .isInstanceOf(StreamException.class);
    assertThat(invalidBuf.readerIndex()).isEqualTo(invalidMessage.length);
  }

  @Test
  void oversizedLengthArgumentRejected() {
    ByteBuf buf = heapBuf("40");
    assertThatThrownBy(() -> Amqp10.decodeMessage(buf, 1000)).isInstanceOf(StreamException.class);
  }

  @Test
  void fuzzingRandomBytesNeverEscapesWithUnexpectedExceptionOrError() {
    Random random = new Random(42);
    for (int i = 0; i < 5_000; i++) {
      int len = 1 + random.nextInt(64);
      byte[] data = new byte[len];
      random.nextBytes(data);
      ByteBuf buf = Unpooled.wrappedBuffer(data);
      try {
        CODEC.decode(buf, len);
      } catch (StreamException | IllegalArgumentException expected) {
        // acceptable, well-typed decoding failure; unknown/reserved type codes are out of scope
        // for this hardening pass (see decision D3 in codec-decoding-security-plan.md)
      }
      assertThat(buf.readerIndex()).isEqualTo(len);
    }
  }

  // --- Regression cases ---

  @Test
  void emptyArrayStillRoundTripsToEmptyObjectArray() {
    ByteBuf buf = heapBuf("e0 02 00 40");
    Object result = Amqp10.readObject(buf);
    assertThat(result).isInstanceOf(Object[].class);
    assertThat((Object[]) result).isEmpty();
  }

  @Test
  void list32WithWrongDeclaredCountOverValidRegionDecodesCorrectly() {
    // declared count is 5, but the region (correctly sized) holds only 2 elements
    ByteBuf buf = heapBuf("d0 0000000a 00000005 a1 01 61 a1 01 62");
    Object result = Amqp10.readObject(buf);
    assertThat(result).isInstanceOf(List.class);
    @SuppressWarnings("unchecked")
    List<Object> list = (List<Object>) result;
    assertThat(list).containsExactly("a", "b");
  }

  @Test
  void largeLegitimateMessageDecodesUnchanged() {
    byte[] largeBody = new byte[1_000_000];
    new Random(1).nextBytes(largeBody);

    Map<String, Object> largeMap = new LinkedHashMap<>();
    for (int i = 0; i < 5_000; i++) {
      largeMap.put("key-" + i, "value-" + i);
    }
    List<String> nestedList =
        IntStream.range(0, 1_000)
            .mapToObj(i -> "element-" + i)
            .collect(java.util.stream.Collectors.toList());

    MessageBuilder builder = CODEC.messageBuilder();
    Message msg =
        builder
            .messageAnnotations()
            .entry("large-map", largeMap)
            .entry("nested-list", nestedList)
            .messageBuilder()
            .addData(largeBody)
            .build();

    Codec.EncodedMessage encoded = CODEC.encode(msg);
    ByteBuf buf = Unpooled.buffer(encoded.getSize() + 4);
    encoded.writeTo(buf);
    buf.readInt(); // skip size
    Message decoded = CODEC.decode(buf, encoded.getSize());

    assertThat(decoded.getBodyAsBinary()).isEqualTo(largeBody);
    @SuppressWarnings("unchecked")
    Map<String, Object> decodedMap =
        (Map<String, Object>) decoded.getMessageAnnotations().get("large-map");
    assertThat(decodedMap).hasSize(5_000);
    assertThat(decodedMap.get("key-0")).isEqualTo("value-0");
    assertThat(decodedMap.get("key-4999")).isEqualTo("value-4999");
    @SuppressWarnings("unchecked")
    List<Object> decodedList = (List<Object>) decoded.getMessageAnnotations().get("nested-list");
    assertThat(decodedList).hasSize(1_000);
    assertThat(decodedList.get(0)).isEqualTo("element-0");
    assertThat(decodedList.get(999)).isEqualTo("element-999");
  }
}
