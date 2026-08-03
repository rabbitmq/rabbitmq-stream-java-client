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
package com.rabbitmq.stream.codec;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.amqp.UnsignedInteger;
import com.rabbitmq.stream.amqp.UnsignedLong;
import com.rabbitmq.stream.amqp.UnsignedShort;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Low-level AMQP 1.0 encoding and decoding utilities for RabbitMQ Stream messages.
 *
 * <p>This class provides the primitives to serialize and deserialize messages using the <a
 * href="https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-types-v1.0-os.html">AMQP 1.0 type
 * system</a>. It supports the full range of AMQP 1.0 primitive types, composite types (lists, maps,
 * arrays), and the message sections defined in the AMQP 1.0 message format (message annotations,
 * properties, application properties, and body sections including data, amqp-value, and
 * amqp-sequence).
 *
 * <p>Encoding is performed directly into Netty {@link ByteBuf} instances, using compact sub-type
 * encodings where the specification allows (e.g. {@code smalluint}, {@code ulong0}). The class also
 * provides size-calculation methods that mirror the encoding logic, enabling callers to pre-compute
 * the exact encoded size of a message without materializing the bytes.
 *
 * <p><strong>This class is experimental and subject to change in future releases.</strong>
 *
 * @see InternalCodec
 * @see StreamingEncodedMessage
 */
final class Amqp10 {

  private static final byte[] EMPTY_BODY = new byte[0];

  // AMQP 1.0 type format codes
  static final byte DESCRIBED_TYPE_CONSTRUCTOR = 0x00;

  static final byte NULL = 0x40;
  static final byte BOOLEAN_TRUE = 0x41;
  static final byte BOOLEAN_FALSE = 0x42;
  static final byte BOOLEAN = 0x56;

  static final byte UBYTE = 0x50;
  static final byte BYTE = 0x51;
  static final byte USHORT = 0x60;
  static final byte SHORT = 0x61;

  static final byte UINT = 0x70;
  static final byte SMALLUINT = 0x52;
  static final byte UINT0 = 0x43;
  static final byte INT = 0x71;
  static final byte SMALLINT = 0x54;

  static final byte ULONG = (byte) 0x80;
  static final byte SMALLULONG = 0x53;
  static final byte ULONG0 = 0x44;
  static final byte LONG = (byte) 0x81;
  static final byte SMALLLONG = 0x55;

  static final byte FLOAT = 0x72;
  static final byte DOUBLE = (byte) 0x82;
  static final byte CHAR = 0x73;
  static final byte TIMESTAMP = (byte) 0x83;
  static final byte UUID_TYPE = (byte) 0x98;

  static final byte VBIN8 = (byte) 0xa0;
  static final byte VBIN32 = (byte) 0xb0;
  static final byte STR8 = (byte) 0xa1;
  static final byte STR32 = (byte) 0xb1;
  static final byte SYM8 = (byte) 0xa3;
  static final byte SYM32 = (byte) 0xb3;

  static final byte LIST0 = 0x45;
  static final byte LIST8 = (byte) 0xc0;
  static final byte LIST32 = (byte) 0xd0;
  static final byte MAP8 = (byte) 0xc1;
  static final byte MAP32 = (byte) 0xd1;
  static final byte ARRAY8 = (byte) 0xe0;
  static final byte ARRAY32 = (byte) 0xf0;

  // Section descriptor codes (AMQP 1.0 message format)
  static final long MESSAGE_ANNOTATIONS_DESCRIPTOR = 0x72L;
  static final long PROPERTIES_DESCRIPTOR = 0x73L;
  static final long APPLICATION_PROPERTIES_DESCRIPTOR = 0x74L;
  static final long DATA_DESCRIPTOR = 0x75L;
  static final long AMQP_SEQUENCE_DESCRIPTOR = 0x76L;
  static final long AMQP_VALUE_DESCRIPTOR = 0x77L;

  private Amqp10() {}

  // --- Size Calculation: Primitives ---

  static int sizeOfNull() {
    return 1; // NULL
  }

  static int sizeOfBoolean(boolean value) {
    return 1; // BOOLEAN_TRUE or BOOLEAN_FALSE
  }

  static int sizeOfUByte(byte value) {
    return 2; // UBYTE + value
  }

  static int sizeOfByte(byte value) {
    return 2; // BYTE + value
  }

  static int sizeOfUShort(short value) {
    return 3; // USHORT + value
  }

  static int sizeOfShort(short value) {
    return 3; // SHORT + value
  }

  static int sizeOfUInt(int value) {
    if (value == 0) {
      return 1; // UINT0
    } else if ((value & 0xFF) == value) {
      return 2; // SMALLUINT + value
    } else {
      return 5; // UINT + value
    }
  }

  static int sizeOfInt(int value) {
    if (value >= -128 && value <= 127) {
      return 2; // SMALLINT + value
    } else {
      return 5; // INT + value
    }
  }

  static int sizeOfULong(long value) {
    if (value == 0) {
      return 1; // ULONG0
    } else if ((value & 0xFFL) == value) {
      return 2; // SMALLULONG + value
    } else {
      return 9; // ULONG + value
    }
  }

  static int sizeOfLong(long value) {
    if (value >= -128 && value <= 127) {
      return 2; // SMALLLONG + value
    } else {
      return 9; // LONG + value
    }
  }

  static int sizeOfFloat(float value) {
    return 5; // FLOAT + 4 bytes
  }

  static int sizeOfDouble(double value) {
    return 9; // DOUBLE + 8 bytes
  }

  static int sizeOfChar(int codePoint) {
    return 5; // CHAR + 4 bytes
  }

  static int sizeOfTimestamp(long millis) {
    return 9; // TIMESTAMP + 8 bytes
  }

  static int sizeOfUuid(UUID uuid) {
    return 17; // UUID_TYPE + 16 bytes
  }

  static int sizeOfBinary(byte[] data) {
    if (data.length <= 255) {
      return 2 + data.length; // VBIN8 + length + data
    } else {
      return 5 + data.length; // VBIN32 + length + data
    }
  }

  static int sizeOfString(String value) {
    int utf8Length = ByteBufUtil.utf8Bytes(value);
    if (utf8Length <= 255) {
      return 2 + utf8Length; // STR8 + length + data
    } else {
      return 5 + utf8Length; // STR32 + length + data
    }
  }

  static int sizeOfSymbol(String value) {
    int utf8Length = ByteBufUtil.utf8Bytes(value);
    if (utf8Length <= 255) {
      return 2 + utf8Length; // SYM8 + length + data
    } else {
      return 5 + utf8Length; // SYM32 + length + data
    }
  }

  // --- Size Calculation: Composites ---

  private static boolean fitsInCompact8(int elementSize, int count) {
    return count <= 255 && elementSize + 1 <= 255;
  }

  private static boolean mapFitsInCompact8(int elementSize, int count) {
    return count * 2 <= 255 && elementSize + 1 <= 255;
  }

  static int sizeOfList(List<?> list) {
    if (list.isEmpty()) {
      return 1; // LIST0
    }
    int elementSize = 0;
    for (Object item : list) {
      elementSize += sizeOfObject(item);
    }
    int count = list.size();
    if (fitsInCompact8(elementSize, count)) {
      return 1 + 1 + 1 + elementSize; // LIST8 + size + count + elements
    }
    return 1 + 4 + 4 + elementSize; // LIST32 + size + count + elements
  }

  static int sizeOfMap(Map<?, ?> map) {
    int elementSize = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      elementSize += sizeOfObject(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      return 1 + 1 + 1 + elementSize; // MAP8 + size + count + elements
    }
    return 1 + 4 + 4 + elementSize; // MAP32 + size + count + elements
  }

  static int sizeOfSymbolKeyMap(Map<String, Object> map) {
    int elementSize = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      elementSize += sizeOfSymbol(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      return 1 + 1 + 1 + elementSize; // MAP8 + size + count + elements
    }
    return 1 + 4 + 4 + elementSize; // MAP32 + size + count + elements
  }

  static int sizeOfStringKeyMap(Map<String, Object> map) {
    int elementSize = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      elementSize += sizeOfString(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      return 1 + 1 + 1 + elementSize; // MAP8 + size + count + elements
    }
    return 1 + 4 + 4 + elementSize; // MAP32 + size + count + elements
  }

  static int sizeOfArray(Object[] array) {
    if (array.length == 0) {
      return 4; // ARRAY8 + size(1) + count(1) + type(NULL)
    }
    byte elementTypeCode = arrayTypeCodeFor(array[0]);
    int elementSize = 0;
    for (Object item : array) {
      elementSize += sizeOfArrayElementValue(item, elementTypeCode);
    }
    int count = array.length;
    // content inside the size field: count + type constructor + elements
    int innerContent8 = 1 + 1 + elementSize; // count(1) + type(1) + elements
    if (count <= 255 && innerContent8 <= 255) {
      return 1 + 1 + innerContent8; // ARRAY8 + size + content
    }
    int innerContent32 = 4 + 1 + elementSize; // count(4) + type(1) + elements
    return 1 + 4 + innerContent32; // ARRAY32 + size + content
  }

  private static int sizeOfArrayElementValue(Object value, byte elementTypeCode) {
    switch (elementTypeCode) {
      case BOOLEAN:
        return 1; // single byte
      case BYTE:
        return 1;
      case SHORT:
        return 2;
      case INT:
        return 4;
      case LONG:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case CHAR:
        return 4;
      case UUID_TYPE:
        return 16;
      case VBIN32:
        byte[] data = (byte[]) value;
        return 4 + data.length;
      case STR32:
        String str = (String) value;
        return 4 + ByteBufUtil.utf8Bytes(str);
      case SYM32:
        String symbol = value.toString();
        return 4 + ByteBufUtil.utf8Bytes(symbol);
      case UBYTE:
        return 1;
      case USHORT:
        return 2;
      case UINT:
        return 4;
      case ULONG:
        return 8;
      default:
        throw new IllegalArgumentException(
            "Array element type not supported: 0x" + Integer.toHexString(elementTypeCode & 0xFF));
    }
  }

  // --- Encoding: Primitives ---

  static void writeNull(ByteBuf buf) {
    buf.writeByte(NULL);
  }

  static void writeBoolean(ByteBuf buf, boolean value) {
    buf.writeByte(value ? BOOLEAN_TRUE : BOOLEAN_FALSE);
  }

  static void writeUByte(ByteBuf buf, byte value) {
    buf.writeByte(UBYTE);
    buf.writeByte(value);
  }

  static void writeByte(ByteBuf buf, byte value) {
    buf.writeByte(BYTE);
    buf.writeByte(value);
  }

  static void writeUShort(ByteBuf buf, short value) {
    buf.writeByte(USHORT);
    buf.writeShort(value);
  }

  static void writeShort(ByteBuf buf, short value) {
    buf.writeByte(SHORT);
    buf.writeShort(value);
  }

  static void writeUInt(ByteBuf buf, int value) {
    // Note: When UnsignedInteger > Integer.MAX_VALUE is converted to int, it becomes negative.
    // The logic below works correctly for negative ints because:
    // - value == 0 check fails (negative != 0)
    // - (value & 0xFF) == value check fails for all negative values
    // - Falls through to UINT encoding which handles the full 32-bit range
    if (value == 0) {
      buf.writeByte(UINT0);
    } else if ((value & 0xFF) == value) {
      buf.writeByte(SMALLUINT);
      buf.writeByte(value);
    } else {
      buf.writeByte(UINT);
      buf.writeInt(value);
    }
  }

  static void writeInt(ByteBuf buf, int value) {
    if (value >= -128 && value <= 127) {
      buf.writeByte(SMALLINT);
      buf.writeByte(value);
    } else {
      buf.writeByte(INT);
      buf.writeInt(value);
    }
  }

  static void writeULong(ByteBuf buf, long value) {
    if (value == 0) {
      buf.writeByte(ULONG0);
    } else if ((value & 0xFFL) == value) {
      buf.writeByte(SMALLULONG);
      buf.writeByte((int) value);
    } else {
      buf.writeByte(ULONG);
      buf.writeLong(value);
    }
  }

  static void writeLong(ByteBuf buf, long value) {
    if (value >= -128 && value <= 127) {
      buf.writeByte(SMALLLONG);
      buf.writeByte((int) value);
    } else {
      buf.writeByte(LONG);
      buf.writeLong(value);
    }
  }

  static void writeFloat(ByteBuf buf, float value) {
    buf.writeByte(FLOAT);
    buf.writeInt(Float.floatToRawIntBits(value));
  }

  static void writeDouble(ByteBuf buf, double value) {
    buf.writeByte(DOUBLE);
    buf.writeLong(Double.doubleToRawLongBits(value));
  }

  static void writeChar(ByteBuf buf, int codePoint) {
    buf.writeByte(CHAR);
    buf.writeInt(codePoint);
  }

  static void writeTimestamp(ByteBuf buf, long millis) {
    buf.writeByte(TIMESTAMP);
    buf.writeLong(millis);
  }

  static void writeUuid(ByteBuf buf, UUID uuid) {
    buf.writeByte(UUID_TYPE);
    buf.writeLong(uuid.getMostSignificantBits());
    buf.writeLong(uuid.getLeastSignificantBits());
  }

  static void writeBinary(ByteBuf buf, byte[] data) {
    if (data.length <= 255) {
      buf.writeByte(VBIN8);
      buf.writeByte(data.length);
    } else {
      buf.writeByte(VBIN32);
      buf.writeInt(data.length);
    }
    buf.writeBytes(data);
  }

  static void writeString(ByteBuf buf, String value) {
    int length = ByteBufUtil.utf8Bytes(value);
    if (length <= 255) {
      buf.writeByte(STR8);
      buf.writeByte(length);
    } else {
      buf.writeByte(STR32);
      buf.writeInt(length);
    }
    buf.writeCharSequence(value, StandardCharsets.UTF_8);
  }

  static void writeSymbol(ByteBuf buf, String value) {
    int length = ByteBufUtil.utf8Bytes(value);
    if (length <= 255) {
      buf.writeByte(SYM8);
      buf.writeByte(length);
    } else {
      buf.writeByte(SYM32);
      buf.writeInt(length);
    }
    buf.writeCharSequence(value, StandardCharsets.US_ASCII);
  }

  // --- Encoding: Composites ---

  static void writeList(ByteBuf buf, List<?> list) {
    if (list.isEmpty()) {
      buf.writeByte(LIST0);
      return;
    }
    int elementSize = 0;
    for (Object item : list) {
      elementSize += sizeOfObject(item);
    }
    int count = list.size();
    if (fitsInCompact8(elementSize, count)) {
      buf.writeByte(LIST8);
      buf.writeByte(elementSize + 1); // size = count(1) + elements
      buf.writeByte(count);
    } else {
      buf.writeByte(LIST32);
      buf.writeInt(elementSize + 4); // size = count(4) + elements
      buf.writeInt(count);
    }
    for (Object item : list) {
      writeObject(buf, item);
    }
  }

  static void writeMap(ByteBuf buf, Map<?, ?> map) {
    int elementSize = 0;
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      elementSize += sizeOfObject(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      buf.writeByte(MAP8);
      buf.writeByte(elementSize + 1); // size = count(1) + elements
      buf.writeByte(count * 2);
    } else {
      buf.writeByte(MAP32);
      buf.writeInt(elementSize + 4); // size = count(4) + elements
      buf.writeInt(count * 2);
    }
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      writeObject(buf, entry.getKey());
      writeObject(buf, entry.getValue());
    }
  }

  static void writeSymbolKeyMap(ByteBuf buf, Map<String, Object> map) {
    int elementSize = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      elementSize += sizeOfSymbol(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      buf.writeByte(MAP8);
      buf.writeByte(elementSize + 1); // size = count(1) + elements
      buf.writeByte(count * 2);
    } else {
      buf.writeByte(MAP32);
      buf.writeInt(elementSize + 4); // size = count(4) + elements
      buf.writeInt(count * 2);
    }
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      writeSymbol(buf, entry.getKey());
      writeObject(buf, entry.getValue());
    }
  }

  static void writeStringKeyMap(ByteBuf buf, Map<String, Object> map) {
    int elementSize = 0;
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      elementSize += sizeOfString(entry.getKey());
      elementSize += sizeOfObject(entry.getValue());
    }
    int count = map.size();
    if (mapFitsInCompact8(elementSize, count)) {
      buf.writeByte(MAP8);
      buf.writeByte(elementSize + 1); // size = count(1) + elements
      buf.writeByte(count * 2);
    } else {
      buf.writeByte(MAP32);
      buf.writeInt(elementSize + 4); // size = count(4) + elements
      buf.writeInt(count * 2);
    }
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      writeString(buf, entry.getKey());
      writeObject(buf, entry.getValue());
    }
  }

  static void writeArray(ByteBuf buf, Object[] array) {
    if (array.length == 0) {
      buf.writeByte(ARRAY8);
      buf.writeByte(2); // size = 1 (count) + 1 (type code)
      buf.writeByte(0); // count
      buf.writeByte(NULL);
      return;
    }
    byte elementTypeCode = arrayTypeCodeFor(array[0]);
    int elementSize = 0;
    for (Object item : array) {
      elementSize += sizeOfArrayElementValue(item, elementTypeCode);
    }
    int count = array.length;
    int innerContent8 = 1 + 1 + elementSize; // count(1) + type(1) + elements
    if (count <= 255 && innerContent8 <= 255) {
      buf.writeByte(ARRAY8);
      buf.writeByte(innerContent8);
      buf.writeByte(count);
    } else {
      buf.writeByte(ARRAY32);
      int innerContent32 = 4 + 1 + elementSize; // count(4) + type(1) + elements
      buf.writeInt(innerContent32);
      buf.writeInt(count);
    }
    buf.writeByte(elementTypeCode);
    for (Object item : array) {
      writeArrayElementValue(buf, item, elementTypeCode);
    }
  }

  /**
   * Returns the AMQP type code for the array element constructor. Always returns the widest
   * encoding for variable-width and compact types so that all elements use a uniform
   * representation.
   */
  private static byte arrayTypeCodeFor(Object value) {
    if (value instanceof Boolean) {
      return BOOLEAN;
    } else if (value instanceof Byte) {
      return BYTE;
    } else if (value instanceof Short) {
      return SHORT;
    } else if (value instanceof Integer) {
      return INT;
    } else if (value instanceof Long) {
      return LONG;
    } else if (value instanceof Float) {
      return FLOAT;
    } else if (value instanceof Double) {
      return DOUBLE;
    } else if (value instanceof Character) {
      return CHAR;
    } else if (value instanceof UUID) {
      return UUID_TYPE;
    } else if (value instanceof byte[]) {
      return VBIN32;
    } else if (value instanceof String) {
      return STR32;
    } else if (value instanceof com.rabbitmq.stream.amqp.Symbol) {
      return SYM32;
    } else if (value instanceof UnsignedByte) {
      return UBYTE;
    } else if (value instanceof UnsignedShort) {
      return USHORT;
    } else if (value instanceof UnsignedInteger) {
      return UINT;
    } else if (value instanceof UnsignedLong) {
      return ULONG;
    } else {
      throw new IllegalArgumentException(
          "Array element type not supported: " + value.getClass().getName());
    }
  }

  /**
   * Writes the value bytes for an element inside an array, strictly formatted according to the
   * given element type code (no type code prefix).
   */
  private static void writeArrayElementValue(ByteBuf buf, Object value, byte elementTypeCode) {
    switch (elementTypeCode) {
      case BOOLEAN:
        buf.writeByte(((Boolean) value) ? 1 : 0);
        break;
      case BYTE:
        buf.writeByte((Byte) value);
        break;
      case SHORT:
        buf.writeShort((Short) value);
        break;
      case INT:
        buf.writeInt((Integer) value);
        break;
      case LONG:
        buf.writeLong((Long) value);
        break;
      case FLOAT:
        buf.writeInt(Float.floatToRawIntBits((Float) value));
        break;
      case DOUBLE:
        buf.writeLong(Double.doubleToRawLongBits((Double) value));
        break;
      case CHAR:
        buf.writeInt((Character) value);
        break;
      case UUID_TYPE:
        {
          UUID uuid = (UUID) value;
          buf.writeLong(uuid.getMostSignificantBits());
          buf.writeLong(uuid.getLeastSignificantBits());
        }
        break;
      case VBIN32:
        {
          byte[] data = (byte[]) value;
          buf.writeInt(data.length);
          buf.writeBytes(data);
        }
        break;
      case STR32:
        {
          String str = (String) value;
          int length = ByteBufUtil.utf8Bytes(str);
          buf.writeInt(length);
          buf.writeCharSequence(str, StandardCharsets.UTF_8);
        }
        break;
      case SYM32:
        {
          String symbol = value.toString();
          int length = ByteBufUtil.utf8Bytes(symbol);
          buf.writeInt(length);
          buf.writeCharSequence(symbol, StandardCharsets.US_ASCII);
        }
        break;
      case UBYTE:
        buf.writeByte(((UnsignedByte) value).byteValue());
        break;
      case USHORT:
        buf.writeShort(((UnsignedShort) value).shortValue());
        break;
      case UINT:
        buf.writeInt(((UnsignedInteger) value).intValue());
        break;
      case ULONG:
        buf.writeLong(((UnsignedLong) value).longValue());
        break;
      default:
        throw new IllegalArgumentException(
            "Array element type not supported: 0x" + Integer.toHexString(elementTypeCode & 0xFF));
    }
  }

  // --- Size Calculation: Object dispatch ---

  static int sizeOfObject(Object value) {
    if (value == null) {
      return sizeOfNull();
    } else if (value instanceof Boolean) {
      return sizeOfBoolean((Boolean) value);
    } else if (value instanceof Byte) {
      return sizeOfByte((Byte) value);
    } else if (value instanceof Short) {
      return sizeOfShort((Short) value);
    } else if (value instanceof Integer) {
      return sizeOfInt((Integer) value);
    } else if (value instanceof Long) {
      return sizeOfLong((Long) value);
    } else if (value instanceof UnsignedByte) {
      return sizeOfUByte(((UnsignedByte) value).byteValue());
    } else if (value instanceof UnsignedShort) {
      return sizeOfUShort(((UnsignedShort) value).shortValue());
    } else if (value instanceof UnsignedInteger) {
      return sizeOfUInt(((UnsignedInteger) value).intValue());
    } else if (value instanceof UnsignedLong) {
      return sizeOfULong(((UnsignedLong) value).longValue());
    } else if (value instanceof Float) {
      return sizeOfFloat((Float) value);
    } else if (value instanceof Double) {
      return sizeOfDouble((Double) value);
    } else if (value instanceof Character) {
      return sizeOfChar((Character) value);
    } else if (value instanceof Date) {
      return sizeOfTimestamp(((Date) value).getTime());
    } else if (value instanceof UUID) {
      return sizeOfUuid((UUID) value);
    } else if (value instanceof byte[]) {
      return sizeOfBinary((byte[]) value);
    } else if (value instanceof String) {
      return sizeOfString((String) value);
    } else if (value instanceof com.rabbitmq.stream.amqp.Symbol) {
      return sizeOfSymbol(value.toString());
    } else if (value instanceof List) {
      return sizeOfList((List<?>) value);
    } else if (value instanceof Map) {
      return sizeOfMap((Map<?, ?>) value);
    } else if (value instanceof Object[]) {
      return sizeOfArray((Object[]) value);
    } else {
      throw new IllegalArgumentException("Type not supported: " + value.getClass().getName());
    }
  }

  // --- Size Calculation: Described types (sections) ---

  static int sizeOfDescriptor(long descriptorCode) {
    return 1 + sizeOfULong(descriptorCode); // DESCRIBED_TYPE_CONSTRUCTOR + descriptor
  }

  // --- Size Calculation: Message sections ---

  static int sizeOfMessageAnnotations(Map<String, Object> annotations) {
    return sizeOfDescriptor(MESSAGE_ANNOTATIONS_DESCRIPTOR) + sizeOfSymbolKeyMap(annotations);
  }

  static int sizeOfApplicationProperties(Map<String, Object> appProperties) {
    return sizeOfDescriptor(APPLICATION_PROPERTIES_DESCRIPTOR) + sizeOfStringKeyMap(appProperties);
  }

  static int sizeOfData(byte[] data) {
    return sizeOfDescriptor(DATA_DESCRIPTOR) + sizeOfBinary(data);
  }

  static int sizeOfProperties(Properties props) {
    int descriptorSize = sizeOfDescriptor(PROPERTIES_DESCRIPTOR);

    Object[] fields = buildPropertyFields(props);

    int lastNonNull = lastNonNullIndex(fields);

    if (lastNonNull == -1) {
      return descriptorSize + 1; // LIST0
    }

    int count = lastNonNull + 1;
    int elementSize = 0;
    for (int i = 0; i < count; i++) {
      elementSize += sizeOfPropertyField(i, fields[i]);
    }

    if (fitsInCompact8(elementSize, count)) {
      return descriptorSize + 1 + 1 + 1 + elementSize; // LIST8 + size + count + elements
    }
    return descriptorSize + 1 + 4 + 4 + elementSize; // LIST32 + size + count + elements
  }

  private static int sizeOfPropertyField(int index, Object value) {
    if (value == null) {
      return sizeOfNull();
    }
    switch (index) {
      case 0: // message-id: can be ulong, uuid, binary, or string
      case 5: // correlation-id: same types
        return sizeOfIdField(value);
      case 1: // user-id: binary
        return sizeOfBinary((byte[]) value);
      case 2: // to: string
      case 3: // subject: string
      case 4: // reply-to: string
      case 10: // group-id: string
      case 12: // reply-to-group-id: string
        return sizeOfString((String) value);
      case 6: // content-type: symbol
      case 7: // content-encoding: symbol
        return sizeOfSymbol((String) value);
      case 8: // absolute-expiry-time: timestamp
      case 9: // creation-time: timestamp
        return sizeOfTimestamp((Long) value);
      case 11: // group-sequence: sequence-no (uint)
        return sizeOfUInt(toUInt((Long) value));
      default:
        return sizeOfNull();
    }
  }

  private static int sizeOfIdField(Object id) {
    if (id instanceof Long) {
      return sizeOfULong((Long) id);
    } else if (id instanceof String) {
      return sizeOfString((String) id);
    } else if (id instanceof byte[]) {
      return sizeOfBinary((byte[]) id);
    } else if (id instanceof UUID) {
      return sizeOfUuid((UUID) id);
    } else {
      throw new IllegalArgumentException(
          "Unsupported message-id/correlation-id type: " + id.getClass().getName());
    }
  }

  // --- Message Size Calculation ---

  static int calculateMessageSize(Message message) {
    int size = 0;

    if (message.getMessageAnnotations() != null && !message.getMessageAnnotations().isEmpty()) {
      size += sizeOfMessageAnnotations(message.getMessageAnnotations());
    }

    if (message.getProperties() != null) {
      size += sizeOfProperties(message.getProperties());
    }

    if (message.getApplicationProperties() != null
        && !message.getApplicationProperties().isEmpty()) {
      size += sizeOfApplicationProperties(message.getApplicationProperties());
    }

    // Body size calculation - mirrors InternalCodec.encode logic exactly
    byte[] bodyBinary = null;
    try {
      bodyBinary = message.getBodyAsBinary();
    } catch (IllegalStateException e) {
      // non-binary body (e.g. AmqpValue with a String), handled below
    }

    if (bodyBinary != null) {
      size += sizeOfData(bodyBinary);
    } else {
      Object body = message.getBody();
      if (body != null) {
        size += sizeOfDescriptor(AMQP_VALUE_DESCRIPTOR) + sizeOfObject(body);
      } else {
        size += sizeOfData(EMPTY_BODY);
      }
    }

    return size;
  }

  // --- Encoding: Object dispatch ---

  static void writeObject(ByteBuf buf, Object value) {
    if (value == null) {
      writeNull(buf);
    } else if (value instanceof Boolean) {
      writeBoolean(buf, (Boolean) value);
    } else if (value instanceof Byte) {
      writeByte(buf, (Byte) value);
    } else if (value instanceof Short) {
      writeShort(buf, (Short) value);
    } else if (value instanceof Integer) {
      writeInt(buf, (Integer) value);
    } else if (value instanceof Long) {
      writeLong(buf, (Long) value);
    } else if (value instanceof UnsignedByte) {
      writeUByte(buf, ((UnsignedByte) value).byteValue());
    } else if (value instanceof UnsignedShort) {
      writeUShort(buf, ((UnsignedShort) value).shortValue());
    } else if (value instanceof UnsignedInteger) {
      writeUInt(buf, ((UnsignedInteger) value).intValue());
    } else if (value instanceof UnsignedLong) {
      writeULong(buf, ((UnsignedLong) value).longValue());
    } else if (value instanceof Float) {
      writeFloat(buf, (Float) value);
    } else if (value instanceof Double) {
      writeDouble(buf, (Double) value);
    } else if (value instanceof Character) {
      writeChar(buf, (Character) value);
    } else if (value instanceof Date) {
      writeTimestamp(buf, ((Date) value).getTime());
    } else if (value instanceof UUID) {
      writeUuid(buf, (UUID) value);
    } else if (value instanceof byte[]) {
      writeBinary(buf, (byte[]) value);
    } else if (value instanceof String) {
      writeString(buf, (String) value);
    } else if (value instanceof com.rabbitmq.stream.amqp.Symbol) {
      writeSymbol(buf, value.toString());
    } else if (value instanceof List) {
      writeList(buf, (List<?>) value);
    } else if (value instanceof Map) {
      writeMap(buf, (Map<?, ?>) value);
    } else if (value instanceof Object[]) {
      writeArray(buf, (Object[]) value);
    } else {
      throw new IllegalArgumentException("Type not supported: " + value.getClass().getName());
    }
  }

  // --- Encoding: Described types (sections) ---

  static void writeDescriptor(ByteBuf buf, long descriptorCode) {
    buf.writeByte(DESCRIBED_TYPE_CONSTRUCTOR);
    writeULong(buf, descriptorCode);
  }

  // --- Encoding: Message sections ---

  static void writeMessageAnnotations(ByteBuf buf, Map<String, Object> annotations) {
    writeDescriptor(buf, MESSAGE_ANNOTATIONS_DESCRIPTOR);
    writeSymbolKeyMap(buf, annotations);
  }

  static void writeApplicationProperties(ByteBuf buf, Map<String, Object> appProperties) {
    writeDescriptor(buf, APPLICATION_PROPERTIES_DESCRIPTOR);
    writeStringKeyMap(buf, appProperties);
  }

  static void writeData(ByteBuf buf, byte[] data) {
    writeDescriptor(buf, DATA_DESCRIPTOR);
    writeBinary(buf, data);
  }

  static void writeProperties(ByteBuf buf, Properties props) {
    writeDescriptor(buf, PROPERTIES_DESCRIPTOR);
    Object[] fields = buildPropertyFields(props);

    int lastNonNull = lastNonNullIndex(fields);

    if (lastNonNull == -1) {
      buf.writeByte(LIST0);
      return;
    }

    int count = lastNonNull + 1;
    int elementSize = 0;
    for (int i = 0; i < count; i++) {
      elementSize += sizeOfPropertyField(i, fields[i]);
    }

    if (fitsInCompact8(elementSize, count)) {
      buf.writeByte(LIST8);
      buf.writeByte(elementSize + 1); // size = count(1) + elements
      buf.writeByte(count);
    } else {
      buf.writeByte(LIST32);
      buf.writeInt(elementSize + 4); // size = count(4) + elements
      buf.writeInt(count);
    }
    for (int i = 0; i < count; i++) {
      writePropertyField(buf, i, fields[i]);
    }
  }

  private static Object[] buildPropertyFields(Properties props) {
    return new Object[] {
      encodeMessageIdOrCorrelationId(props.getMessageId()), // 0: message-id
      props.getUserId(), // 1: user-id (binary)
      props.getTo(), // 2: to (string)
      props.getSubject(), // 3: subject (string)
      props.getReplyTo(), // 4: reply-to (string)
      encodeMessageIdOrCorrelationId(props.getCorrelationId()), // 5: correlation-id
      props.getContentType(), // 6: content-type (symbol)
      props.getContentEncoding(), // 7: content-encoding (symbol)
      props.getAbsoluteExpiryTime() != 0 ? props.getAbsoluteExpiryTime() : null, // 8
      props.getCreationTime() != 0 ? props.getCreationTime() : null, // 9
      props.getGroupId(), // 10: group-id (string)
      props.getGroupSequence() >= 0 ? props.getGroupSequence() : null, // 11: group-sequence
      props.getReplyToGroupId() // 12: reply-to-group-id (string)
    };
  }

  private static int lastNonNullIndex(Object[] fields) {
    for (int i = fields.length - 1; i >= 0; i--) {
      if (fields[i] != null) {
        return i;
      }
    }
    return -1;
  }

  private static int toUInt(long value) {
    if (value < 0 || value > 0xFFFFFFFFL) {
      throw new IllegalArgumentException(
          "Value " + value + " is outside the uint range [0, " + 0xFFFFFFFFL + "]");
    }
    return (int) value;
  }

  private static Object encodeMessageIdOrCorrelationId(Object id) {
    if (id == null) {
      return null;
    }
    if (id instanceof Long || id instanceof String || id instanceof byte[] || id instanceof UUID) {
      return id;
    }
    // External codec types (QPid, SwiftMQ) may wrap IDs in their own Number/binary types
    if (id instanceof Number) {
      return ((Number) id).longValue();
    }
    return id;
  }

  private static void writePropertyField(ByteBuf buf, int index, Object value) {
    if (value == null) {
      writeNull(buf);
      return;
    }
    switch (index) {
      case 0: // message-id: can be ulong, uuid, binary, or string
      case 5: // correlation-id: same types
        writeIdField(buf, value);
        break;
      case 1: // user-id: binary
        writeBinary(buf, (byte[]) value);
        break;
      case 2: // to: string
      case 3: // subject: string
      case 4: // reply-to: string
      case 10: // group-id: string
      case 12: // reply-to-group-id: string
        writeString(buf, (String) value);
        break;
      case 6: // content-type: symbol
      case 7: // content-encoding: symbol
        writeSymbol(buf, (String) value);
        break;
      case 8: // absolute-expiry-time: timestamp
      case 9: // creation-time: timestamp
        writeTimestamp(buf, (Long) value);
        break;
      case 11: // group-sequence: sequence-no (uint)
        writeUInt(buf, toUInt((Long) value));
        break;
      default:
        writeNull(buf);
    }
  }

  private static void writeIdField(ByteBuf buf, Object id) {
    if (id instanceof Long) {
      writeULong(buf, (Long) id);
    } else if (id instanceof String) {
      writeString(buf, (String) id);
    } else if (id instanceof byte[]) {
      writeBinary(buf, (byte[]) id);
    } else if (id instanceof UUID) {
      writeUuid(buf, (UUID) id);
    } else {
      throw new IllegalArgumentException(
          "Unsupported message-id/correlation-id type: " + id.getClass().getName());
    }
  }

  // --- Decoding ---

  // Guardrails against malformed input.
  // Mirrors deps/amqp10_common/src/amqp10_binary_parser.erl in rabbitmq-server.

  /**
   * Maximum number of sections in a message. Same value as MAX_MSG_SECTIONS in
   * amqp10_binary_parser.erl: an unlimited number of tiny sections would let a single message
   * consume disproportionate CPU time. No real world sender comes near this.
   */
  private static final int MAX_MSG_SECTIONS = 10_000;

  /** Maximum nesting depth of AMQP types. Real messages nest two or three levels deep. */
  private static final int MAX_NESTING_DEPTH = 64;

  /** Number of bytes still readable before the current region ends. */
  private static int remaining(ByteBuf buf, int limit) {
    return limit - buf.readerIndex();
  }

  private static void requireBytes(ByteBuf buf, int limit, int needed) {
    if (remaining(buf, limit) < needed) {
      throw new StreamException(
          "Insufficient input: %d byte(s) needed, %d available", needed, remaining(buf, limit));
    }
  }

  /** Reads a 1-byte size field and checks the announced bytes are present. */
  private static int readSize8(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 1);
    int size = buf.readByte() & 0xFF;
    requireBytes(buf, limit, size);
    return size;
  }

  /**
   * Reads a 4-byte size field and checks it is non-negative and the announced bytes are present.
   */
  private static int readSize32(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 4);
    int size = buf.readInt();
    if (size < 0) {
      throw new StreamException("Invalid declared size: %d", size);
    }
    requireBytes(buf, limit, size);
    return size;
  }

  private static byte readByteChecked(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 1);
    return buf.readByte();
  }

  private static short readShortChecked(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 2);
    return buf.readShort();
  }

  private static int readIntChecked(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 4);
    return buf.readInt();
  }

  private static long readLongChecked(ByteBuf buf, int limit) {
    requireBytes(buf, limit, 8);
    return buf.readLong();
  }

  private static void checkDepth(int depth) {
    if (depth > MAX_NESTING_DEPTH) {
      throw new StreamException(
          "AMQP 1.0 nesting depth exceeds the limit of %d", MAX_NESTING_DEPTH);
    }
  }

  /**
   * Minimum byte size of one array element of the given type, excluding the constructor that all
   * elements of the array share. Port of array_element_size/2 in amqp10_binary_parser.erl.
   * Variable-width types return the size of their length prefix, which is their minimum.
   */
  private static int arrayElementSize(byte type) {
    int t = type & 0xFF;
    if (t == DESCRIBED_TYPE_CONSTRUCTOR) return 2; // descriptor + value, at least 1 byte each
    if (t >= 0x40 && t <= 0x4f) return 0; // zero-width: null, true, false, uint0, ulong0, list0
    if (t >= 0x50 && t <= 0x5f) return 1;
    if (t >= 0x60 && t <= 0x6f) return 2;
    if (t >= 0x70 && t <= 0x7f) return 4;
    if (t >= 0x80 && t <= 0x8f) return 8;
    if (t >= 0x90 && t <= 0x9f) return 16;
    if (t >= 0xa0 && t <= 0xaf) return 1; // 1-byte length prefix
    if (t >= 0xb0 && t <= 0xbf) return 4; // 4-byte length prefix
    if (t >= 0xc0 && t <= 0xcf) return 1;
    if (t >= 0xd0 && t <= 0xdf) return 4;
    if (t >= 0xe0 && t <= 0xef) return 1;
    if (t >= 0xf0) return 4;
    throw new StreamException("Unsupported array element type: 0x%02X", t);
  }

  static Object readObject(ByteBuf buf) {
    return readObject(buf, buf.writerIndex(), 0);
  }

  private static Object readObject(ByteBuf buf, int limit, int depth) {
    byte code = readByteChecked(buf, limit);
    return readObjectWithCode(buf, code, limit, depth);
  }

  @SuppressWarnings("fallthrough")
  private static Object readObjectWithCode(ByteBuf buf, byte code, int limit, int depth) {
    switch (code) {
      case NULL:
        return null;
      case BOOLEAN_TRUE:
        return Boolean.TRUE;
      case BOOLEAN_FALSE:
        return Boolean.FALSE;
      case BOOLEAN:
        return readByteChecked(buf, limit) != 0 ? Boolean.TRUE : Boolean.FALSE;
      case UBYTE:
        return UnsignedByte.valueOf(readByteChecked(buf, limit));
      case BYTE:
        return readByteChecked(buf, limit);
      case USHORT:
        return UnsignedShort.valueOf(readShortChecked(buf, limit));
      case SHORT:
        return readShortChecked(buf, limit);
      case UINT:
        return UnsignedInteger.valueOf(readIntChecked(buf, limit));
      case SMALLUINT:
        return UnsignedInteger.valueOf(readByteChecked(buf, limit) & 0xFF);
      case UINT0:
        return UnsignedInteger.valueOf(0);
      case INT:
        return readIntChecked(buf, limit);
      case SMALLINT:
        return (int) readByteChecked(buf, limit);
      case ULONG:
        return UnsignedLong.valueOf(readLongChecked(buf, limit));
      case SMALLULONG:
        return UnsignedLong.valueOf(readByteChecked(buf, limit) & 0xFFL);
      case ULONG0:
        return UnsignedLong.valueOf(0);
      case LONG:
        return readLongChecked(buf, limit);
      case SMALLLONG:
        return (long) readByteChecked(buf, limit);
      case FLOAT:
        return Float.intBitsToFloat(readIntChecked(buf, limit));
      case DOUBLE:
        return Double.longBitsToDouble(readLongChecked(buf, limit));
      case CHAR:
        return readIntChecked(buf, limit);
      case TIMESTAMP:
        // Returns raw milliseconds as Long, not Date object.
        // This causes type fidelity loss during Date roundtrip encoding/decoding.
        // Consistent with QPid Proton behavior but should be documented.
        return readLongChecked(buf, limit);
      case UUID_TYPE:
        return new UUID(readLongChecked(buf, limit), readLongChecked(buf, limit));
      case VBIN8:
        {
          int len = readSize8(buf, limit);
          byte[] data = new byte[len];
          buf.readBytes(data);
          return data;
        }
      case VBIN32:
        {
          int len = readSize32(buf, limit);
          byte[] data = new byte[len];
          buf.readBytes(data);
          return data;
        }
      case STR8:
        return readText(buf, readSize8(buf, limit), StandardCharsets.UTF_8);
      case STR32:
        return readText(buf, readSize32(buf, limit), StandardCharsets.UTF_8);
      case SYM8:
        return readText(buf, readSize8(buf, limit), StandardCharsets.US_ASCII);
      case SYM32:
        return readText(buf, readSize32(buf, limit), StandardCharsets.US_ASCII);
      case LIST0:
        return new ArrayList<>(0);
      case LIST8:
        {
          int size = readSize8(buf, limit);
          if (size < 1) {
            throw new StreamException("Invalid list size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int declaredCount = buf.readByte() & 0xFF; // allocation hint only, never a loop bound
          return readListElements(buf, end, declaredCount, depth + 1);
        }
      case LIST32:
        {
          int size = readSize32(buf, limit);
          if (size < 4) {
            throw new StreamException("Invalid list size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int declaredCount = buf.readInt(); // allocation hint only, never a loop bound
          return readListElements(buf, end, declaredCount, depth + 1);
        }
      case MAP8:
        {
          int size = readSize8(buf, limit);
          if (size < 1) {
            throw new StreamException("Invalid map size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int declaredCount = buf.readByte() & 0xFF; // allocation hint only
          return readMapEntries(buf, end, declaredCount, depth + 1);
        }
      case MAP32:
        {
          int size = readSize32(buf, limit);
          if (size < 4) {
            throw new StreamException("Invalid map size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int declaredCount = buf.readInt(); // allocation hint only
          return readMapEntries(buf, end, declaredCount, depth + 1);
        }
      case ARRAY8:
        {
          // An array's size covers count(1) + element constructor(1) + elements, so 2 is the
          // minimum.
          int size = readSize8(buf, limit);
          if (size < 2) {
            throw new StreamException("Invalid array size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int count = buf.readByte() & 0xFF;
          return readArrayElements(buf, end, count, depth + 1);
        }
      case ARRAY32:
        {
          // An array's size covers count(4) + element constructor(1) + elements, so 5 is the
          // minimum.
          int size = readSize32(buf, limit);
          if (size < 5) {
            throw new StreamException("Invalid array size: %d", size);
          }
          int end = buf.readerIndex() + size;
          int count = buf.readInt();
          if (count < 0) {
            throw new StreamException("Invalid array count: %d", count);
          }
          return readArrayElements(buf, end, count, depth + 1);
        }
      case DESCRIBED_TYPE_CONSTRUCTOR:
        return readDescribedType(buf, limit, depth + 1);
      default:
        throw new IllegalArgumentException(
            "Unknown AMQP type code: 0x" + Integer.toHexString(code & 0xFF));
    }
  }

  private static String readText(ByteBuf buf, int length, Charset charset) {
    String result = buf.toString(buf.readerIndex(), length, charset);
    buf.skipBytes(length);
    return result;
  }

  /**
   * Initial capacity for a decoded collection. The declared count is a hint from the peer, so it is
   * clamped to the largest number of elements the remaining bytes can hold: 1 byte minimum per list
   * element (its constructor byte), 2 bytes minimum per map entry (a key and a value constructor).
   * For a well-formed message the declared count is exact and is used as-is.
   */
  private static int sanitizedCapacity(
      int declaredCount, int availableBytes, int minBytesPerElement) {
    if (declaredCount <= 0) {
      return 0;
    }
    return Math.min(declaredCount, availableBytes / minBytesPerElement);
  }

  private static List<Object> readListElements(ByteBuf buf, int end, int declaredCount, int depth) {
    checkDepth(depth);
    List<Object> list =
        new ArrayList<>(sanitizedCapacity(declaredCount, end - buf.readerIndex(), 1));
    while (buf.readerIndex() < end) {
      list.add(readObject(buf, end, depth));
    }
    return list;
  }

  private static Map<Object, Object> readMapEntries(
      ByteBuf buf, int end, int declaredCount, int depth) {
    checkDepth(depth);
    // The declared count of a map is the number of elements, i.e. twice the number of entries.
    Map<Object, Object> map =
        new LinkedHashMap<>(sanitizedCapacity(declaredCount / 2, end - buf.readerIndex(), 2));
    while (buf.readerIndex() < end) {
      Object key = readObject(buf, end, depth);
      if (buf.readerIndex() >= end) {
        // "Map encodings MUST contain an even number of items" [1.6.23]
        throw new StreamException("Map with an odd number of elements");
      }
      map.put(key, readObject(buf, end, depth));
    }
    return map;
  }

  private static Object readArrayElements(ByteBuf buf, int end, int count, int depth) {
    checkDepth(depth);
    if (buf.readerIndex() >= end) {
      throw new StreamException("Missing array element constructor");
    }
    byte elementType = buf.readByte();
    int elementSize = arrayElementSize(elementType);
    Object array;
    if (elementSize == 0) {
      // A huge count of zero-width elements costs a handful of bytes on the wire, so the
      // count cannot be trusted and the elements cannot be materialized safely (CWE-770).
      // amqp10_binary_parser.erl keeps such an array opaque; this codec must return real
      // objects, so it rejects it instead.
      if (count > 0) {
        throw new StreamException(
            "Array of %d zero-width elements (constructor 0x%02X) not supported",
            count, elementType & 0xFF);
      }
      array = new Object[0];
    } else {
      if ((long) count * elementSize > end - buf.readerIndex()) {
        throw new StreamException(
            "Declared array count %d exceeds the %d byte(s) available",
            count, end - buf.readerIndex());
      }
      array = readArrayValues(buf, elementType, count, end, depth);
    }
    // An array must fill its declared size exactly. Mirrors
    // failed_to_parse_array_extra_input_remaining in amqp10_binary_parser.erl.
    if (buf.readerIndex() != end) {
      throw new StreamException(
          "Array declared %d byte(s) beyond its elements", end - buf.readerIndex());
    }
    return array;
  }

  private static Object readArrayValues(
      ByteBuf buf, byte elementType, int count, int end, int depth) {
    switch (elementType) {
      case BYTE:
        {
          byte[] arr = new byte[count];
          buf.readBytes(arr);
          return arr;
        }
      case INT:
        {
          int[] arr = new int[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readInt();
          return arr;
        }
      case SMALLINT:
        {
          int[] arr = new int[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readByte();
          return arr;
        }
      case LONG:
        {
          long[] arr = new long[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readLong();
          return arr;
        }
      case SMALLLONG:
        {
          long[] arr = new long[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readByte();
          return arr;
        }
      case FLOAT:
        {
          float[] arr = new float[count];
          for (int i = 0; i < count; i++) arr[i] = Float.intBitsToFloat(buf.readInt());
          return arr;
        }
      case DOUBLE:
        {
          double[] arr = new double[count];
          for (int i = 0; i < count; i++) arr[i] = Double.longBitsToDouble(buf.readLong());
          return arr;
        }
      case BOOLEAN:
        {
          boolean[] arr = new boolean[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readByte() != 0;
          return arr;
        }
      case UBYTE:
        {
          byte[] arr = new byte[count];
          buf.readBytes(arr);
          return arr;
        }
      case USHORT:
        {
          short[] arr = new short[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readShort();
          return arr;
        }
      case SHORT:
        {
          short[] arr = new short[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readShort();
          return arr;
        }
      case UINT:
        {
          int[] arr = new int[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readInt();
          return arr;
        }
      case SMALLUINT:
        {
          int[] arr = new int[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readByte() & 0xFF;
          return arr;
        }
      case ULONG:
        {
          long[] arr = new long[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readLong();
          return arr;
        }
      case SMALLULONG:
        {
          long[] arr = new long[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readByte() & 0xFFL;
          return arr;
        }
      case CHAR:
        {
          int[] arr = new int[count];
          for (int i = 0; i < count; i++) arr[i] = buf.readInt();
          return arr;
        }
      default:
        {
          Object[] result = new Object[count];
          for (int i = 0; i < count; i++) {
            result[i] = readObjectWithCode(buf, elementType, end, depth);
          }
          return result;
        }
    }
  }

  // --- Decoding: Described types ---

  private static Object readDescribedType(ByteBuf buf, int limit, int depth) {
    checkDepth(depth);
    // The constructor byte (0x00) has already been consumed
    Object descriptor = readObject(buf, limit, depth + 1);
    Object value = readObject(buf, limit, depth + 1);
    return new DescribedValue(descriptor, value);
  }

  /** Internal holder for described types encountered during decoding. */
  static final class DescribedValue {
    final Object descriptor;
    final Object value;

    DescribedValue(Object descriptor, Object value) {
      this.descriptor = descriptor;
      this.value = value;
    }

    long descriptorCode() {
      if (descriptor instanceof UnsignedLong) {
        return ((UnsignedLong) descriptor).longValue();
      }
      return -1;
    }
  }

  // --- Decoding: Message sections ---

  static DecodedMessage decodeMessage(ByteBuf buf, int length) {
    if (length < 0 || length > buf.readableBytes()) {
      throw new StreamException(
          "Invalid message length %d, only %d byte(s) available", length, buf.readableBytes());
    }
    int endIndex = buf.readerIndex() + length;
    try {
      return decodeSections(buf, endIndex);
    } finally {
      // The frame layer assumes the codec consumed exactly `length` bytes, whether decoding
      // succeeded or not. Never leave the reader index short of, or past, the message end.
      buf.readerIndex(endIndex);
    }
  }

  private static DecodedMessage decodeSections(ByteBuf buf, int endIndex) {
    Map<String, Object> messageAnnotations = null;
    Properties properties = null;
    Map<String, Object> applicationProperties = null;
    byte[] bodyData = null;
    Object body = null;

    int sections = 0;
    while (buf.readerIndex() < endIndex) {
      if (++sections > MAX_MSG_SECTIONS) {
        throw new StreamException("Message has more than %d sections", MAX_MSG_SECTIONS);
      }
      Object section = readObject(buf, endIndex, 0);
      if (!(section instanceof DescribedValue)) {
        continue;
      }
      DescribedValue dv = (DescribedValue) section;
      long code = dv.descriptorCode();
      if (code == MESSAGE_ANNOTATIONS_DESCRIPTOR) {
        messageAnnotations = toStringKeyMap(asMap(dv.value, "message annotations"));
      } else if (code == PROPERTIES_DESCRIPTOR) {
        properties = decodeProperties(asList(dv.value, "properties"));
      } else if (code == APPLICATION_PROPERTIES_DESCRIPTOR) {
        applicationProperties = toStringKeyMap(asMap(dv.value, "application properties"));
      } else if (code == DATA_DESCRIPTOR) {
        bodyData = asBinary(dv.value, "data");
        body = bodyData;
      } else if (code == AMQP_SEQUENCE_DESCRIPTOR) {
        if (body == null) {
          body = dv.value;
        }
      } else if (code == AMQP_VALUE_DESCRIPTOR) {
        body = dv.value;
        if (dv.value instanceof byte[]) {
          bodyData = (byte[]) dv.value;
        }
      }
    }

    return new DecodedMessage(
        messageAnnotations, properties, applicationProperties, bodyData, body);
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, Object> asMap(Object value, String section) {
    if (value == null || value instanceof Map) {
      return (Map<Object, Object>) value;
    }
    throw new StreamException(
        "Expected a map for the %s section, got %s", section, value.getClass().getSimpleName());
  }

  @SuppressWarnings("unchecked")
  private static List<Object> asList(Object value, String section) {
    if (value == null || value instanceof List) {
      return (List<Object>) value;
    }
    throw new StreamException(
        "Expected a list for the %s section, got %s", section, value.getClass().getSimpleName());
  }

  private static byte[] asBinary(Object value, String section) {
    if (value == null || value instanceof byte[]) {
      return (byte[]) value;
    }
    throw new StreamException(
        "Expected binary for the %s section, got %s", section, value.getClass().getSimpleName());
  }

  private static Map<String, Object> toStringKeyMap(Map<Object, Object> source) {
    if (source == null) {
      return null;
    }
    Map<String, Object> result = new LinkedHashMap<>(source.size());
    for (Map.Entry<Object, Object> entry : source.entrySet()) {
      if (entry.getKey() == null) {
        throw new StreamException("Null key in a string-keyed message section");
      }
      result.put(entry.getKey().toString(), entry.getValue());
    }
    return result;
  }

  private static Properties decodeProperties(List<Object> fields) {
    if (fields == null) {
      return null;
    }
    return new DecodedProperties(fields);
  }

  static final class DecodedMessage {
    final Map<String, Object> messageAnnotations;
    final Properties properties;
    final Map<String, Object> applicationProperties;
    final byte[] bodyData;
    final Object body;

    DecodedMessage(
        Map<String, Object> messageAnnotations,
        Properties properties,
        Map<String, Object> applicationProperties,
        byte[] bodyData,
        Object body) {
      this.messageAnnotations = messageAnnotations;
      this.properties = properties;
      this.applicationProperties = applicationProperties;
      this.bodyData = bodyData;
      this.body = body;
    }
  }

  static final class DecodedProperties implements Properties {
    private static final long NULL_GROUP_SEQUENCE = -1L;
    private static final long NULL_TIMESTAMP = 0L;
    private final List<Object> fields;

    DecodedProperties(List<Object> fields) {
      this.fields = fields;
    }

    private Object field(int index) {
      return index < fields.size() ? fields.get(index) : null;
    }

    @Override
    public Object getMessageId() {
      return field(0);
    }

    @Override
    public String getMessageIdAsString() {
      Object id = field(0);
      return id == null ? null : id.toString();
    }

    @Override
    public long getMessageIdAsLong() {
      Object id = field(0);
      if (id instanceof UnsignedLong) {
        return ((UnsignedLong) id).longValue();
      }
      return ((Number) id).longValue();
    }

    @Override
    public byte[] getMessageIdAsBinary() {
      return (byte[]) field(0);
    }

    @Override
    public UUID getMessageIdAsUuid() {
      return (UUID) field(0);
    }

    @Override
    public byte[] getUserId() {
      return (byte[]) field(1);
    }

    @Override
    public String getTo() {
      return (String) field(2);
    }

    @Override
    public String getSubject() {
      return (String) field(3);
    }

    @Override
    public String getReplyTo() {
      return (String) field(4);
    }

    @Override
    public Object getCorrelationId() {
      return field(5);
    }

    @Override
    public String getCorrelationIdAsString() {
      Object id = field(5);
      return id == null ? null : id.toString();
    }

    @Override
    public long getCorrelationIdAsLong() {
      Object id = field(5);
      if (id instanceof UnsignedLong) {
        return ((UnsignedLong) id).longValue();
      }
      return ((Number) id).longValue();
    }

    @Override
    public byte[] getCorrelationIdAsBinary() {
      return (byte[]) field(5);
    }

    @Override
    public UUID getCorrelationIdAsUuid() {
      return (UUID) field(5);
    }

    @Override
    public String getContentType() {
      return (String) field(6);
    }

    @Override
    public String getContentEncoding() {
      return (String) field(7);
    }

    @Override
    public long getAbsoluteExpiryTime() {
      Object v = field(8);
      return v == null ? NULL_TIMESTAMP : ((Number) v).longValue();
    }

    @Override
    public long getCreationTime() {
      Object v = field(9);
      return v == null ? NULL_TIMESTAMP : ((Number) v).longValue();
    }

    @Override
    public String getGroupId() {
      return (String) field(10);
    }

    @Override
    public long getGroupSequence() {
      Object v = field(11);
      if (v == null) {
        return NULL_GROUP_SEQUENCE;
      }
      if (v instanceof UnsignedInteger) {
        return ((UnsignedInteger) v).longValue();
      }
      return ((Number) v).longValue();
    }

    @Override
    public String getReplyToGroupId() {
      return (String) field(12);
    }
  }
}
