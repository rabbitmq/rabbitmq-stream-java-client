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
package com.rabbitmq.stream.codec;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.amqp.*;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import com.swiftmq.tools.util.DataByteArrayOutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class SwiftMqCodec implements Codec {

  private static Object convertAmqpMapValue(AMQPType value) {
    if (value instanceof AMQPBoolean) {
      return ((AMQPBoolean) value).getValue() ? Boolean.TRUE : Boolean.FALSE;
    } else if (value instanceof AMQPByte) {
      return ((AMQPByte) value).getValue();
    } else if (value instanceof AMQPUnsignedByte) {
      return UnsignedByte.valueOf((byte) ((AMQPUnsignedByte) value).getValue());
    } else if (value instanceof AMQPShort) {
      return (short) ((AMQPShort) value).getValue();
    } else if (value instanceof AMQPUnsignedShort) {
      return UnsignedShort.valueOf((short) ((AMQPUnsignedShort) value).getValue());
    } else if (value instanceof AMQPInt) {
      return ((AMQPInt) value).getValue();
    } else if (value instanceof AMQPUnsignedInt) {
      return UnsignedInteger.valueOf(((AMQPUnsignedInt) value).getValue());
    } else if (value instanceof AMQPLong) {
      return ((AMQPLong) value).getValue();
    } else if (value instanceof AMQPUnsignedLong) {
      return UnsignedLong.valueOf(((AMQPUnsignedLong) value).getValue());
    } else if (value instanceof AMQPFloat) {
      return ((AMQPFloat) value).getValue();
    } else if (value instanceof AMQPDouble) {
      return ((AMQPDouble) value).getValue();
    } else if (value instanceof AMQPBinary) {
      return ((AMQPBinary) value).getValue();
    } else if (value instanceof AMQPString) {
      return ((AMQPString) value).getValue();
    } else if (value instanceof AMQPChar) {
      return (char) (((AMQPChar) value).getValue() & 0xffff);
    } else if (value instanceof AMQPTimestamp) {
      return ((AMQPTimestamp) value).getValue();
    } else if (value instanceof AMQPUuid) {
      return ((AMQPUuid) value).getValue();
    } else if (value instanceof AMQPSymbol) {
      return ((AMQPSymbol) value).getValue();
    } else if (value instanceof AMQPNull) {
      return null;
    } else if (value == null) {
      return null;
    } else {
      throw new IllegalArgumentException(
          "Type not supported for an application property: " + value.getClass());
    }
  }

  private static Map<String, Object> createApplicationProperties(AMQPMessage amqpMessage) {
    if (amqpMessage.getApplicationProperties() != null) {
      Map<AMQPType, AMQPType> applicationProperties;
      try {
        applicationProperties = amqpMessage.getApplicationProperties().getValue();
      } catch (IOException e) {
        throw new StreamException("Error while reading application properties", e);
      }
      return createMapFromAmqpMap(applicationProperties);
    } else {
      return null;
    }
  }

  private static Map<String, Object> createMessageAnnotations(AMQPMessage amqpMessage) {
    if (amqpMessage.getMessageAnnotations() != null) {
      Map<AMQPType, AMQPType> messageAnnotations;
      try {
        messageAnnotations = amqpMessage.getMessageAnnotations().getValue();
      } catch (IOException e) {
        throw new StreamException("Error while reading message annotations", e);
      }
      return createMapFromAmqpMap(messageAnnotations);
    } else {
      return null;
    }
  }

  private static Map<String, Object> createMapFromAmqpMap(Map<AMQPType, AMQPType> map) {
    Map<String, Object> result;
    if (map != null) {
      result = new LinkedHashMap<>(map.size());
      for (Map.Entry<AMQPType, AMQPType> entry : map.entrySet()) {
        result.put(entry.getKey().getValueString(), convertAmqpMapValue(entry.getValue()));
      }
    } else {
      result = null;
    }
    return result;
  }

  @Override
  public MessageBuilder messageBuilder() {
    return new SwiftMqMessageBuilder();
  }

  @Override
  public EncodedMessage encode(Message message) {
    AMQPMessage outboundMessage;
    if (message instanceof SwiftMqAmqpMessageWrapper) {
      outboundMessage = ((SwiftMqAmqpMessageWrapper) message).message;
    } else {
      outboundMessage = new AMQPMessage();
      if (message.getProperties() != null) {
        com.rabbitmq.stream.Properties headers = message.getProperties();
        com.swiftmq.amqp.v100.generated.messaging.message_format.Properties properties =
            new com.swiftmq.amqp.v100.generated.messaging.message_format.Properties();
        boolean propertiesSet = false;
        if (headers.getMessageId() != null) {
          if (headers.getMessageId() instanceof String) {
            properties.setMessageId(new MessageIdString(headers.getMessageIdAsString()));
          } else if (headers.getMessageId().getClass().isPrimitive()
              || headers.getMessageId() instanceof Long) {
            properties.setMessageId(new MessageIdUlong(headers.getMessageIdAsLong()));
          } else if (headers.getMessageId().getClass().isArray()) {
            properties.setMessageId(new MessageIdBinary(headers.getMessageIdAsBinary()));
          } else if (headers.getMessageId() instanceof UUID) {
            properties.setMessageId(new MessageIdUuid(headers.getMessageIdAsUuid()));
          } else {
            throw new IllegalStateException(
                "Type not supported for message ID:" + properties.getMessageId().getClass());
          }
          propertiesSet = true;
        }

        if (headers.getUserId() != null) {
          properties.setUserId(new AMQPBinary(headers.getUserId()));
          propertiesSet = true;
        }

        if (headers.getTo() != null) {
          properties.setTo(new AddressString(headers.getTo()));
          propertiesSet = true;
        }

        if (headers.getSubject() != null) {
          properties.setSubject(new AMQPString(headers.getSubject()));
          propertiesSet = true;
        }

        if (headers.getReplyTo() != null) {
          properties.setReplyTo(new AddressString(headers.getReplyTo()));
          propertiesSet = true;
        }

        if (headers.getCorrelationId() != null) {
          if (headers.getCorrelationId() instanceof String) {
            properties.setCorrelationId(new MessageIdString(headers.getCorrelationIdAsString()));
          } else if (headers.getCorrelationId().getClass().isPrimitive()
              || headers.getCorrelationId() instanceof Long) {
            properties.setCorrelationId(new MessageIdUlong(headers.getCorrelationIdAsLong()));
          } else if (headers.getCorrelationId().getClass().isArray()) {
            properties.setCorrelationId(new MessageIdBinary(headers.getCorrelationIdAsBinary()));
          } else if (headers.getCorrelationId() instanceof UUID) {
            properties.setCorrelationId(new MessageIdUuid(headers.getCorrelationIdAsUuid()));
          } else {
            throw new IllegalStateException(
                "Type not supported for correlation ID:"
                    + properties.getCorrelationId().getClass());
          }
          propertiesSet = true;
        }

        if (headers.getContentType() != null) {
          properties.setContentType(new AMQPSymbol(headers.getContentType()));
          propertiesSet = true;
        }

        if (headers.getContentEncoding() != null) {
          properties.setContentEncoding(new AMQPSymbol(headers.getContentEncoding()));
          propertiesSet = true;
        }

        if (headers.getAbsoluteExpiryTime() > 0) {
          properties.setAbsoluteExpiryTime(new AMQPTimestamp(headers.getAbsoluteExpiryTime()));
          propertiesSet = true;
        }

        if (headers.getCreationTime() > 0) {
          properties.setCreationTime(new AMQPTimestamp(headers.getCreationTime()));
          propertiesSet = true;
        }

        if (headers.getGroupId() != null) {
          properties.setGroupId(new AMQPString(headers.getGroupId()));
          propertiesSet = true;
        }

        if (headers.getGroupSequence() >= 0) {
          properties.setGroupSequence(new SequenceNo(headers.getGroupSequence()));
          propertiesSet = true;
        }

        if (headers.getReplyToGroupId() != null) {
          properties.setReplyToGroupId(new AMQPString(headers.getReplyToGroupId()));
          propertiesSet = true;
        }

        if (propertiesSet) {
          outboundMessage.setProperties(properties);
        }
      }

      if (message.getApplicationProperties() != null
          && !message.getApplicationProperties().isEmpty()) {
        Map<AMQPType, AMQPType> applicationProperties =
            new LinkedHashMap<>(message.getApplicationProperties().size());
        for (Map.Entry<String, Object> entry : message.getApplicationProperties().entrySet()) {
          applicationProperties.put(
              new AMQPString(entry.getKey()), convertToSwiftMqType(entry.getValue()));
        }
        try {
          outboundMessage.setApplicationProperties(
              new ApplicationProperties(applicationProperties));
        } catch (IOException e) {
          throw new StreamException("Error while setting application properties", e);
        }
      }

      if (message.getMessageAnnotations() != null && !message.getMessageAnnotations().isEmpty()) {
        Map<AMQPType, AMQPType> messageAnnotations =
            new LinkedHashMap<>(message.getMessageAnnotations().size());
        for (Map.Entry<String, Object> entry : message.getMessageAnnotations().entrySet()) {
          messageAnnotations.put(
              new AMQPSymbol(entry.getKey()), convertToSwiftMqType(entry.getValue()));
        }
        try {
          outboundMessage.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
        } catch (IOException e) {
          throw new StreamException("Error while setting message annotations", e);
        }
      }

      if (message.getBodyAsBinary() != null) {
        outboundMessage.addData(new Data(message.getBodyAsBinary()));
      }
    }

    try {
      int bufferSize;
      // FIXME estimate the size with all message data
      if (outboundMessage.getData() != null && !outboundMessage.getData().isEmpty()) {
        bufferSize = outboundMessage.getData().get(0).getPredictedSize();
      } else {
        bufferSize = 8192;
      }
      DataByteArrayOutputStream output = new DataByteArrayOutputStream(bufferSize);
      outboundMessage.writeContent(output);
      return new EncodedMessage(output.getCount(), output.getBuffer());
    } catch (IOException e) {
      throw new StreamException("Error while writing AMQP 1.0 message to output stream");
    }
  }

  protected AMQPType convertToSwiftMqType(Object value) {
    if (value instanceof Boolean) {
      return ((Boolean) value).booleanValue() ? AMQPBoolean.TRUE : AMQPBoolean.FALSE;
    } else if (value instanceof Byte) {
      return new AMQPByte((Byte) value);
    } else if (value instanceof Short) {
      return new AMQPShort((Short) value);
    } else if (value instanceof Integer) {
      return new AMQPInt((Integer) value);
    } else if (value instanceof Long) {
      return new AMQPLong((Long) value);
    } else if (value instanceof UnsignedByte) {
      return new AMQPUnsignedByte(((UnsignedByte) value).byteValue());
    } else if (value instanceof UnsignedShort) {
      return new AMQPUnsignedShort(((UnsignedShort) value).shortValue());
    } else if (value instanceof UnsignedInteger) {
      return new AMQPUnsignedInt(((UnsignedInteger) value).intValue());
    } else if (value instanceof UnsignedLong) {
      return new AMQPUnsignedLong(((UnsignedLong) value).longValue());
    } else if (value instanceof Float) {
      return new AMQPFloat((Float) value);
    } else if (value instanceof Double) {
      return new AMQPDouble((Double) value);
    } else if (value instanceof byte[]) {
      return new AMQPBinary((byte[]) value);
    } else if (value instanceof String) {
      return new AMQPString((String) value);
    } else if (value instanceof Character) {
      return new AMQPChar((Character) value);
    } else if (value instanceof Date) {
      return new AMQPTimestamp(((Date) value).getTime());
    } else if (value instanceof Symbol) {
      return new AMQPSymbol(value.toString());
    } else if (value instanceof UUID) {
      return new AMQPUuid((UUID) value);
    } else if (value == value) {
      return AMQPNull.NULL;
    } else {
      throw new IllegalArgumentException(
          "Type not supported for an application property: " + value.getClass());
    }
  }

  @Override
  public Message decode(byte[] data) {
    return createMessage(data);
  }

  protected Object extractBody(AMQPMessage amqpMessage) {
    Object body;
    if (amqpMessage.getData() != null) {
      body = amqpMessage.getData();
    } else if (amqpMessage.getAmqpValue() != null) {
      body = amqpMessage.getAmqpValue();
    } else if (amqpMessage.getAmqpSequence() != null) {
      body = amqpMessage.getAmqpSequence();
    } else {
      body = null;
    }
    return body;
  }

  protected com.rabbitmq.stream.Properties createProperties(AMQPMessage amqpMessage) {
    if (amqpMessage.getProperties() != null) {
      return new SwiftMqProperties(amqpMessage.getProperties());
    } else {
      return null;
    }
  }

  protected Message createMessage(byte[] data) {
    AMQPMessage amqpMessage;
    try {
      amqpMessage = new AMQPMessage(data);
    } catch (Exception e) {
      throw new StreamException("Error while decoding AMQP 1.0 message");
    }

    Object body = extractBody(amqpMessage);

    com.rabbitmq.stream.Properties properties = createProperties(amqpMessage);

    Map<String, Object> applicationProperties = createApplicationProperties(amqpMessage);
    Map<String, Object> messageAnnotations = createMessageAnnotations(amqpMessage);
    return new SwiftMqMessage(
        amqpMessage, body, properties, applicationProperties, messageAnnotations);
  }

  private static final class SwiftMqMessage implements Message {

    private final AMQPMessage amqpMessage;
    private final Object body;
    private final com.rabbitmq.stream.Properties properties;
    private final Map<String, Object> applicationProperties;
    private final Map<String, Object> messageAnnotations;

    private SwiftMqMessage(
        AMQPMessage amqpMessage,
        Object body,
        com.rabbitmq.stream.Properties properties,
        Map<String, Object> applicationProperties,
        Map<String, Object> messageAnnotations) {
      this.amqpMessage = amqpMessage;
      this.body = body;
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
      if (amqpMessage.getData() != null) {
        return amqpMessage.getData().get(0).getValue();
      } else if (amqpMessage.getAmqpValue() != null) {
        AMQPType value = amqpMessage.getAmqpValue().getValue();
        if (value instanceof AMQPBinary) {
          return ((AMQPBinary) value).getValue();
        } else if (value instanceof AMQPArray) {
          try {
            AMQPType[] array = ((AMQPArray) value).getValue();
            if (array.length > 0) {
              // far-fetched
              if (array[0] instanceof AMQPByte) {
                byte[] result = new byte[array.length];
                for (int i = 0; i < array.length; i++) {
                  result[i] = ((AMQPByte) array[i]).getValue();
                }
                return result;
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else if (amqpMessage.getData() == null && amqpMessage.getAmqpValue() == null) {
        return null;
      }
      throw new IllegalStateException(
          "Body cannot by returned as array of bytes. Use #getBody() to get native representation.");
    }

    @Override
    public Object getBody() {
      if (amqpMessage.getData() != null) {
        return amqpMessage.getData();
      } else if (amqpMessage.getAmqpValue() != null) {
        return amqpMessage.getAmqpValue();
      } else {
        return null;
      }
    }

    @Override
    public com.rabbitmq.stream.Properties getProperties() {
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
  }

  private static final class SwiftMqProperties implements com.rabbitmq.stream.Properties {

    private static final long NULL_GROUP_SEQUENCE = -1;
    private final com.swiftmq.amqp.v100.generated.messaging.message_format.Properties
        amqpProperties;

    private SwiftMqProperties(
        com.swiftmq.amqp.v100.generated.messaging.message_format.Properties amqpProperties) {
      this.amqpProperties = amqpProperties;
    }

    @Override
    public Object getMessageId() {
      return amqpProperties.getMessageId();
    }

    @Override
    public String getMessageIdAsString() {
      return amqpProperties.getMessageId().getValueString();
    }

    @Override
    public long getMessageIdAsLong() {
      return ((MessageIdUlong) amqpProperties.getMessageId()).getValue();
    }

    @Override
    public byte[] getMessageIdAsBinary() {
      return ((MessageIdBinary) amqpProperties.getMessageId()).getValue();
    }

    @Override
    public UUID getMessageIdAsUuid() {
      return ((MessageIdUuid) amqpProperties.getMessageId()).getValue();
    }

    @Override
    public byte[] getUserId() {
      return amqpProperties.getUserId().getValue();
    }

    @Override
    public String getTo() {
      return amqpProperties.getTo().getValueString();
    }

    @Override
    public String getSubject() {
      return amqpProperties.getSubject().getValue();
    }

    @Override
    public String getReplyTo() {
      return amqpProperties.getReplyTo().getValueString();
    }

    @Override
    public Object getCorrelationId() {
      return amqpProperties.getCorrelationId();
    }

    @Override
    public String getCorrelationIdAsString() {
      return ((MessageIdString) amqpProperties.getCorrelationId()).getValue();
    }

    @Override
    public long getCorrelationIdAsLong() {
      return ((MessageIdUlong) amqpProperties.getCorrelationId()).getValue();
    }

    @Override
    public byte[] getCorrelationIdAsBinary() {
      return ((MessageIdBinary) amqpProperties.getCorrelationId()).getValue();
    }

    @Override
    public UUID getCorrelationIdAsUuid() {
      return ((MessageIdUuid) amqpProperties.getCorrelationId()).getValue();
    }

    @Override
    public String getContentType() {
      return amqpProperties.getContentType().getValue();
    }

    @Override
    public String getContentEncoding() {
      return amqpProperties.getContentEncoding().getValue();
    }

    @Override
    public long getAbsoluteExpiryTime() {
      return amqpProperties.getAbsoluteExpiryTime().getValue();
    }

    @Override
    public long getCreationTime() {
      return amqpProperties.getCreationTime().getValue();
    }

    @Override
    public String getGroupId() {
      return amqpProperties.getGroupId().getValue();
    }

    @Override
    public long getGroupSequence() {
      return amqpProperties.getGroupSequence() == null
          ? NULL_GROUP_SEQUENCE
          : amqpProperties.getGroupSequence().getValue();
    }

    @Override
    public String getReplyToGroupId() {
      return amqpProperties.getReplyToGroupId().getValue();
    }
  }

  static class SwiftMqAmqpMessageWrapper implements Message {

    private final boolean hasPublishingId;
    private final long publishingId;
    private final AMQPMessage message;

    private com.rabbitmq.stream.Properties properties;
    private Map<String, Object> applicationProperties;
    private Map<String, Object> messageAnnotations;

    SwiftMqAmqpMessageWrapper(boolean hasPublishingId, long publishingId, AMQPMessage message) {
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
      return message.getData().get(0).getValue();
    }

    @Override
    public Object getBody() {
      return message.getData();
    }

    @Override
    public com.rabbitmq.stream.Properties getProperties() {
      if (this.properties != null) {
        return this.properties;
      } else if (message.getProperties() != null) {
        this.properties = new SwiftMqProperties(message.getProperties());
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
  }
}
