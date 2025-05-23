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
package com.rabbitmq.stream.codec;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Properties;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import org.apache.qpid.proton.amqp.*;
import org.apache.qpid.proton.amqp.messaging.*;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

public class QpidProtonCodec implements Codec {

  static final Section EMPTY_BODY = new Data(new Binary(new byte[0]));

  private static final Function<String, String> MESSAGE_ANNOTATIONS_STRING_KEY_EXTRACTOR = k -> k;
  private static final Function<Symbol, String> MESSAGE_ANNOTATIONS_SYMBOL_KEY_EXTRACTOR =
      Symbol::toString;

  private static Map<String, Object> createApplicationProperties(
      org.apache.qpid.proton.message.Message message) {
    if (message.getApplicationProperties() != null) {
      return createMapFromAmqpMap(
          MESSAGE_ANNOTATIONS_STRING_KEY_EXTRACTOR, message.getApplicationProperties().getValue());
    } else {
      return null;
    }
  }

  private static Map<String, Object> createMessageAnnotations(
      org.apache.qpid.proton.message.Message message) {
    if (message.getMessageAnnotations() != null) {
      return createMapFromAmqpMap(
          MESSAGE_ANNOTATIONS_SYMBOL_KEY_EXTRACTOR, message.getMessageAnnotations().getValue());
    } else {
      return new LinkedHashMap<>();
    }
  }

  private static <K> Map<String, Object> createMapFromAmqpMap(
      Function<K, String> keyMaker, Map<K, Object> amqpMap) {
    Map<String, Object> result;
    if (amqpMap != null) {
      result = new LinkedHashMap<>(amqpMap.size());
      for (Map.Entry<K, Object> entry : amqpMap.entrySet()) {
        result.put(keyMaker.apply(entry.getKey()), fromQpidToJava(entry.getValue()));
      }
    } else {
      result = null;
    }
    return result;
  }

  private static Object fromQpidToJava(Object value) {
    if (value instanceof Boolean
        || value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
        || value instanceof String
        || value instanceof Character
        || value instanceof UUID
        || value instanceof List
        || value instanceof Map
        || value instanceof Object[]) {
      return value;
    } else if (value instanceof Binary) {
      return ((Binary) value).getArray();
    } else if (value instanceof UnsignedByte) {
      return com.rabbitmq.stream.amqp.UnsignedByte.valueOf(((UnsignedByte) value).byteValue());
    } else if (value instanceof UnsignedShort) {
      return com.rabbitmq.stream.amqp.UnsignedShort.valueOf(((UnsignedShort) value).shortValue());
    } else if (value instanceof UnsignedInteger) {
      return com.rabbitmq.stream.amqp.UnsignedInteger.valueOf(((UnsignedInteger) value).intValue());
    } else if (value instanceof UnsignedLong) {
      return com.rabbitmq.stream.amqp.UnsignedLong.valueOf(((UnsignedLong) value).longValue());
    } else if (value instanceof Date) {
      return ((Date) value).getTime();
    } else if (value instanceof Symbol) {
      return ((Symbol) value).toString();
    } else if (value == null) {
      return null;
    } else if (value.getClass().isArray()) {
      return value;
    } else {
      throw new IllegalArgumentException("Type not supported: " + value.getClass());
    }
  }

  @Override
  public EncodedMessage encode(Message message) {
    org.apache.qpid.proton.message.Message qpidMessage;
    if (message instanceof QpidProtonAmqpMessageWrapper) {
      qpidMessage = ((QpidProtonAmqpMessageWrapper) message).message;
    } else {
      qpidMessage = org.apache.qpid.proton.message.Message.Factory.create();
      if (message.getProperties() != null) {
        Properties headers = message.getProperties();
        org.apache.qpid.proton.amqp.messaging.Properties properties =
            new org.apache.qpid.proton.amqp.messaging.Properties();
        boolean propertiesSet = false;
        if (headers.getMessageId() != null) {
          if (headers.getMessageId() instanceof String) {
            properties.setMessageId(headers.getMessageIdAsString());
          } else if (headers.getMessageId().getClass().isPrimitive()
              || headers.getMessageId() instanceof Long) {
            properties.setMessageId(new UnsignedLong(headers.getMessageIdAsLong()));
          } else if (headers.getMessageId().getClass().isArray()) {
            properties.setMessageId(new Binary(headers.getMessageIdAsBinary()));
          } else if (headers.getMessageId() instanceof UUID) {
            properties.setMessageId(headers.getMessageIdAsUuid());
          } else {
            throw new IllegalStateException(
                "Type not supported for message ID:" + properties.getMessageId().getClass());
          }
          propertiesSet = true;
        }

        if (headers.getUserId() != null) {
          properties.setUserId(new Binary(headers.getUserId()));
          propertiesSet = true;
        }

        if (headers.getTo() != null) {
          properties.setTo(headers.getTo());
          propertiesSet = true;
        }

        if (headers.getSubject() != null) {
          properties.setSubject(headers.getSubject());
          propertiesSet = true;
        }

        if (headers.getReplyTo() != null) {
          properties.setReplyTo(headers.getReplyTo());
          propertiesSet = true;
        }

        if (headers.getCorrelationId() != null) {
          if (headers.getCorrelationId() instanceof String) {
            properties.setCorrelationId(headers.getCorrelationIdAsString());
          } else if (headers.getCorrelationId().getClass().isPrimitive()
              || headers.getCorrelationId() instanceof Long) {
            properties.setCorrelationId(new UnsignedLong(headers.getCorrelationIdAsLong()));
          } else if (headers.getCorrelationId().getClass().isArray()) {
            properties.setCorrelationId(new Binary(headers.getCorrelationIdAsBinary()));
          } else if (headers.getCorrelationId() instanceof UUID) {
            properties.setCorrelationId(headers.getCorrelationIdAsUuid());
          } else {
            throw new IllegalStateException(
                "Type not supported for correlation ID:"
                    + properties.getCorrelationId().getClass());
          }
          propertiesSet = true;
        }

        if (headers.getContentType() != null) {
          properties.setContentType(Symbol.getSymbol(headers.getContentType()));
          propertiesSet = true;
        }

        if (headers.getContentEncoding() != null) {
          properties.setContentEncoding(Symbol.getSymbol(headers.getContentEncoding()));
          propertiesSet = true;
        }

        if (headers.getAbsoluteExpiryTime() > 0) {
          properties.setAbsoluteExpiryTime(new Date(headers.getAbsoluteExpiryTime()));
          propertiesSet = true;
        }

        if (headers.getCreationTime() > 0) {
          properties.setCreationTime(new Date(headers.getCreationTime()));
          propertiesSet = true;
        }

        if (headers.getGroupId() != null) {
          properties.setGroupId(headers.getGroupId());
          propertiesSet = true;
        }

        if (headers.getGroupSequence() >= 0) {
          properties.setGroupSequence(UnsignedInteger.valueOf(headers.getGroupSequence()));
          propertiesSet = true;
        }

        if (headers.getReplyToGroupId() != null) {
          properties.setReplyToGroupId(headers.getReplyToGroupId());
          propertiesSet = true;
        }

        if (propertiesSet) {
          qpidMessage.setProperties(properties);
        }
      }

      if (message.getApplicationProperties() != null
          && !message.getApplicationProperties().isEmpty()) {
        Map<String, Object> applicationProperties =
            new LinkedHashMap<>(message.getApplicationProperties().size());
        for (Map.Entry<String, Object> entry : message.getApplicationProperties().entrySet()) {
          applicationProperties.put(entry.getKey(), convertToQpidType(entry.getValue()));
        }
        qpidMessage.setApplicationProperties(new ApplicationProperties(applicationProperties));
      }

      if (message.getMessageAnnotations() != null && !message.getMessageAnnotations().isEmpty()) {
        Map<Symbol, Object> messageAnnotations =
            new LinkedHashMap<>(message.getMessageAnnotations().size());
        for (Map.Entry<String, Object> entry : message.getMessageAnnotations().entrySet()) {
          messageAnnotations.put(
              Symbol.getSymbol(entry.getKey()), convertToQpidType(entry.getValue()));
        }
        qpidMessage.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
      }

      if (message.getBodyAsBinary() == null) {
        qpidMessage.setBody(EMPTY_BODY);
      } else {
        qpidMessage.setBody(new Data(new Binary(message.getBodyAsBinary())));
      }
    }
    int bufferSize;
    if (qpidMessage.getBody() instanceof Data) {
      bufferSize = (int) (((Data) qpidMessage.getBody()).getValue().getLength() * 1.5);
      bufferSize = bufferSize == 0 ? 128 : bufferSize;
    } else {
      bufferSize = 8192;
    }
    ByteArrayWritableBuffer writableBuffer = new ByteArrayWritableBuffer(bufferSize);
    qpidMessage.encode(writableBuffer);
    return new EncodedMessage(writableBuffer.getArrayLength(), writableBuffer.getArray());
  }

  @Override
  public Message decode(byte[] data) {
    org.apache.qpid.proton.message.Message message =
        org.apache.qpid.proton.message.Message.Factory.create();
    message.decode(data, 0, data.length);
    return new QpidProtonMessage(
        message,
        createProperties(message),
        createApplicationProperties(message),
        createMessageAnnotations(message));
  }

  protected static Properties createProperties(org.apache.qpid.proton.message.Message message) {
    if (message.getProperties() != null) {
      return new QpidProtonProperties(message.getProperties());
    } else {
      return null;
    }
  }

  protected Object convertToQpidType(Object value) {
    if (value instanceof Boolean
        || value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
        || value instanceof String
        || value instanceof Character
        || value instanceof UUID
        || value instanceof Date
        || value instanceof List
        || value instanceof Map
        || value instanceof Object[]) {
      return value;
    } else if (value instanceof com.rabbitmq.stream.amqp.UnsignedByte) {
      return UnsignedByte.valueOf(((com.rabbitmq.stream.amqp.UnsignedByte) value).byteValue());
    } else if (value instanceof com.rabbitmq.stream.amqp.UnsignedShort) {
      return UnsignedShort.valueOf(((com.rabbitmq.stream.amqp.UnsignedShort) value).shortValue());
    } else if (value instanceof com.rabbitmq.stream.amqp.UnsignedInteger) {
      return UnsignedInteger.valueOf(((com.rabbitmq.stream.amqp.UnsignedInteger) value).intValue());
    } else if (value instanceof com.rabbitmq.stream.amqp.UnsignedLong) {
      return UnsignedLong.valueOf(((com.rabbitmq.stream.amqp.UnsignedLong) value).longValue());
    } else if (value instanceof com.rabbitmq.stream.amqp.Symbol) {
      return Symbol.getSymbol(value.toString());
    } else if (value instanceof byte[]) {
      return new Binary((byte[]) value);
    } else if (value == null) {
      return null;
    } else {
      throw new IllegalArgumentException("Type not supported: " + value.getClass());
    }
  }

  @Override
  public MessageBuilder messageBuilder() {
    return new QpidProtonMessageBuilder();
  }

  private static final class QpidProtonProperties implements Properties {

    private static final long NULL_GROUP_SEQUENCE = -1L;
    private static final long NULL_TIMESTAMP = 0L;
    private final org.apache.qpid.proton.amqp.messaging.Properties properties;

    private QpidProtonProperties(org.apache.qpid.proton.amqp.messaging.Properties properties) {
      this.properties = properties;
    }

    @Override
    public Object getMessageId() {
      return properties.getMessageId();
    }

    @Override
    public String getMessageIdAsString() {
      return properties.getMessageId() == null ? null : properties.getMessageId().toString();
    }

    @Override
    public long getMessageIdAsLong() {
      return ((UnsignedLong) properties.getMessageId()).longValue();
    }

    @Override
    public byte[] getMessageIdAsBinary() {
      return properties.getMessageId() == null
          ? null
          : ((Binary) properties.getMessageId()).getArray();
    }

    @Override
    public UUID getMessageIdAsUuid() {
      return properties.getMessageId() == null ? null : (UUID) properties.getMessageId();
    }

    @Override
    public byte[] getUserId() {
      return properties.getUserId() == null ? null : properties.getUserId().getArray();
    }

    @Override
    public String getTo() {
      return properties.getTo();
    }

    @Override
    public String getSubject() {
      return properties.getSubject();
    }

    @Override
    public String getReplyTo() {
      return properties.getReplyTo();
    }

    @Override
    public Object getCorrelationId() {
      return properties.getCorrelationId();
    }

    @Override
    public String getCorrelationIdAsString() {
      return properties.getCorrelationId() == null
          ? null
          : properties.getCorrelationId().toString();
    }

    @Override
    public long getCorrelationIdAsLong() {
      return ((UnsignedLong) properties.getCorrelationId()).longValue();
    }

    @Override
    public byte[] getCorrelationIdAsBinary() {
      return properties.getCorrelationId() == null
          ? null
          : ((Binary) properties.getCorrelationId()).getArray();
    }

    @Override
    public UUID getCorrelationIdAsUuid() {
      return properties.getCorrelationId() == null ? null : (UUID) properties.getCorrelationId();
    }

    @Override
    public String getContentType() {
      return properties.getContentType() == null ? null : properties.getContentType().toString();
    }

    @Override
    public String getContentEncoding() {
      return properties.getContentEncoding() == null
          ? null
          : properties.getContentEncoding().toString();
    }

    @Override
    public long getAbsoluteExpiryTime() {
      return properties.getAbsoluteExpiryTime() == null
          ? NULL_TIMESTAMP
          : properties.getAbsoluteExpiryTime().getTime();
    }

    @Override
    public long getCreationTime() {
      return properties.getCreationTime() == null
          ? NULL_TIMESTAMP
          : properties.getCreationTime().getTime();
    }

    @Override
    public String getGroupId() {
      return properties.getGroupId();
    }

    @Override
    public long getGroupSequence() {
      return properties.getGroupSequence() == null
          ? NULL_GROUP_SEQUENCE
          : properties.getGroupSequence().longValue();
    }

    @Override
    public String getReplyToGroupId() {
      return properties.getReplyToGroupId();
    }

    @Override
    public String toString() {
      return "QpidProtonProperties{" + "properties=" + properties + '}';
    }
  }

  private static class QpidProtonMessage implements Message {

    private final org.apache.qpid.proton.message.Message message;
    private final Properties properties;
    private final Map<String, Object> applicationProperties;
    private final Map<String, Object> messageAnnotations;

    private QpidProtonMessage(
        org.apache.qpid.proton.message.Message message,
        Properties properties,
        Map<String, Object> applicationProperties,
        Map<String, Object> messageAnnotations) {
      this.message = message;
      this.properties = properties;
      this.applicationProperties = applicationProperties;
      this.messageAnnotations = messageAnnotations;
    }

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
      if (message.getBody() == null) {
        return null;
      } else if (message.getBody() instanceof Data) {
        return ((Data) message.getBody()).getValue().getArray();
      } else if (message.getBody() instanceof AmqpValue) {
        AmqpValue value = (AmqpValue) message.getBody();
        if (value.getValue() instanceof byte[]) {
          return (byte[]) value.getValue();
        }
      }
      throw new IllegalStateException(
          "Body cannot by returned as array of bytes: "
              + message.getBody()
              + ". Use #getBody() to get native representation.");
    }

    @Override
    public Object getBody() {
      return message.getBody();
    }

    @Override
    public Properties getProperties() {
      return properties;
    }

    @Override
    public Map<String, Object> getApplicationProperties() {
      return applicationProperties;
    }

    @Override
    public Map<String, Object> getMessageAnnotations() {
      return messageAnnotations;
    }

    @Override
    public Message annotate(String key, Object value) {
      this.messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public Message copy() {
      return new QpidProtonMessage(
          message,
          createProperties(message),
          createApplicationProperties(message),
          createMessageAnnotations(message));
    }
  }

  static class QpidProtonAmqpMessageWrapper implements Message {

    private final boolean hasPublishingId;
    private final long publishingId;
    private final org.apache.qpid.proton.message.Message message;
    private Properties properties;
    private Map<String, Object> applicationProperties;
    private Map<String, Object> messageAnnotations;

    QpidProtonAmqpMessageWrapper(
        boolean hasPublishingId,
        long publishingId,
        org.apache.qpid.proton.message.Message message) {
      this.hasPublishingId = hasPublishingId;
      this.publishingId = publishingId;
      this.message = message;
    }

    @Override
    public boolean hasPublishingId() {
      return hasPublishingId;
    }

    @Override
    public long getPublishingId() {
      return publishingId;
    }

    @Override
    public byte[] getBodyAsBinary() {
      return message.getBody() == null ? null : ((Data) message.getBody()).getValue().getArray();
    }

    @Override
    public Object getBody() {
      return message.getBody();
    }

    @Override
    public Properties getProperties() {
      if (this.properties != null) {
        return this.properties;
      } else if (message.getProperties() != null) {
        this.properties = new QpidProtonProperties(message.getProperties());
        return this.properties;
      } else {
        return null;
      }
    }

    @Override
    public Map<String, Object> getApplicationProperties() {
      if (this.applicationProperties != null) {
        return this.applicationProperties;
      } else if (message.getApplicationProperties() != null) {
        this.applicationProperties = createApplicationProperties(this.message);
        return this.applicationProperties;
      } else {
        return null;
      }
    }

    @Override
    public Map<String, Object> getMessageAnnotations() {
      if (this.messageAnnotations != null) {
        return this.messageAnnotations;
      } else if (this.message.getMessageAnnotations() != null) {
        this.messageAnnotations = createMessageAnnotations(this.message);
        return this.messageAnnotations;
      } else {
        return null;
      }
    }

    @Override
    public Message annotate(String key, Object value) {
      MessageAnnotations annotations = this.message.getMessageAnnotations();
      if (annotations == null) {
        annotations = new MessageAnnotations(new LinkedHashMap<>());
        this.message.setMessageAnnotations(annotations);
      }
      annotations.getValue().put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public Message copy() {
      org.apache.qpid.proton.message.Message copy =
          org.apache.qpid.proton.message.Message.Factory.create();
      copy.setProperties(this.message.getProperties());
      copy.setBody(this.message.getBody());
      copy.setApplicationProperties(this.message.getApplicationProperties());
      if (this.message.getMessageAnnotations() != null) {
        Map<Symbol, Object> annotations = message.getMessageAnnotations().getValue();
        Map<Symbol, Object> annotationCopy;
        if (annotations == null) {
          annotationCopy = null;
        } else {
          annotationCopy = new LinkedHashMap<>(annotations.size());
          annotationCopy.putAll(annotations);
        }
        copy.setMessageAnnotations(new MessageAnnotations(annotationCopy));
      }
      return new QpidProtonAmqpMessageWrapper(this.hasPublishingId, this.publishingId, copy);
    }
  }

  // from
  // https://github.com/apache/activemq/blob/master/activemq-amqp/src/main/java/org/apache/activemq/transport/amqp/message/AmqpWritableBuffer.java
  static class ByteArrayWritableBuffer implements WritableBuffer {

    static final int DEFAULT_CAPACITY = 4 * 1024;

    private byte[] buffer;
    private int position;

    /** Creates a new WritableBuffer with default capacity. */
    ByteArrayWritableBuffer() {
      this(DEFAULT_CAPACITY);
    }

    /** Create a new WritableBuffer with the given capacity. */
    ByteArrayWritableBuffer(int capacity) {
      this.buffer = new byte[capacity];
    }

    byte[] getArray() {
      return buffer;
    }

    int getArrayLength() {
      return position;
    }

    @Override
    public void put(byte b) {
      int newPosition = position + 1;
      ensureCapacity(newPosition);
      buffer[position] = b;
      position = newPosition;
    }

    @Override
    public void putShort(short value) {
      ensureCapacity(position + 2);
      buffer[position++] = (byte) (value >>> 8);
      buffer[position++] = (byte) (value >>> 0);
    }

    @Override
    public void putInt(int value) {
      ensureCapacity(position + 4);
      buffer[position++] = (byte) (value >>> 24);
      buffer[position++] = (byte) (value >>> 16);
      buffer[position++] = (byte) (value >>> 8);
      buffer[position++] = (byte) (value >>> 0);
    }

    @Override
    public void putLong(long value) {
      ensureCapacity(position + 8);
      buffer[position++] = (byte) (value >>> 56);
      buffer[position++] = (byte) (value >>> 48);
      buffer[position++] = (byte) (value >>> 40);
      buffer[position++] = (byte) (value >>> 32);
      buffer[position++] = (byte) (value >>> 24);
      buffer[position++] = (byte) (value >>> 16);
      buffer[position++] = (byte) (value >>> 8);
      buffer[position++] = (byte) (value >>> 0);
    }

    @Override
    public void putFloat(float value) {
      putInt(Float.floatToRawIntBits(value));
    }

    @Override
    public void putDouble(double value) {
      putLong(Double.doubleToRawLongBits(value));
    }

    @Override
    public void put(byte[] src, int offset, int length) {
      if (length == 0) {
        return;
      }

      int newPosition = position + length;
      ensureCapacity(newPosition);
      System.arraycopy(src, offset, buffer, position, length);
      position = newPosition;
    }

    @Override
    public boolean hasRemaining() {
      return position < Integer.MAX_VALUE;
    }

    @Override
    public int remaining() {
      return Integer.MAX_VALUE - position;
    }

    @Override
    public int position() {
      return position;
    }

    @Override
    public void position(int position) {
      ensureCapacity(position);
      this.position = position;
    }

    @Override
    public void put(ByteBuffer payload) {
      int newPosition = position + payload.remaining();
      ensureCapacity(newPosition);
      while (payload.hasRemaining()) {
        buffer[position++] = payload.get();
      }

      position = newPosition;
    }

    @Override
    public int limit() {
      return Integer.MAX_VALUE;
    }

    @Override
    public void put(ReadableBuffer src) {
      ensureCapacity(position);
      src.get(this);
    }

    /**
     * Ensures the buffer has at least the minimumCapacity specified.
     *
     * @param minimumCapacity the minimum capacity needed to meet the next write operation.
     */
    private void ensureCapacity(int minimumCapacity) {
      if (minimumCapacity > buffer.length) {
        byte[] newBuffer = new byte[Math.max(buffer.length << 1, minimumCapacity)];
        System.arraycopy(buffer, 0, newBuffer, 0, position);
        buffer = newBuffer;
      }
    }
  }
}
