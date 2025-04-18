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

import static com.rabbitmq.stream.codec.SwiftMqCodec.convertToSwiftMqType;
import static com.rabbitmq.stream.codec.SwiftMqCodec.toSwiftMqTypeCode;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.StreamException;
import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

class SwiftMqMessageBuilder implements MessageBuilder {

  private boolean hasPublishingId = false;

  private long publishingId = 0;

  private final AMQPMessage outboundMessage = new AMQPMessage();

  private SwiftMqPropertiesBuilder propertiesBuilder;

  private SwiftMqApplicationPropertiesBuilder applicationPropertiesBuilder;

  private SwiftMqMessageAnnotationsBuilder messageAnnotationsBuilder;
  private final AtomicBoolean built = new AtomicBoolean(false);

  @Override
  public Message build() {
    if (built.compareAndSet(false, true)) {
      if (messageAnnotationsBuilder != null) {
        try {
          outboundMessage.setMessageAnnotations(
              new MessageAnnotations(messageAnnotationsBuilder.map));
        } catch (IOException e) {
          throw new StreamException("Error while setting message annotations", e);
        }
      }
      if (propertiesBuilder != null) {
        outboundMessage.setProperties(propertiesBuilder.properties);
      }
      if (applicationPropertiesBuilder != null) {
        try {
          outboundMessage.setApplicationProperties(
              new ApplicationProperties(applicationPropertiesBuilder.map));
        } catch (IOException e) {
          throw new StreamException("Error while setting application properties", e);
        }
      }
      if (outboundMessage.getData() == null) {
        outboundMessage.addData(SwiftMqCodec.EMPTY_BODY);
      }
      return new SwiftMqCodec.SwiftMqAmqpMessageWrapper(
          hasPublishingId, publishingId, outboundMessage);
    } else {
      throw new IllegalStateException("A message builder can build only one message");
    }
  }

  @Override
  @SuppressFBWarnings({"AT_NONATOMIC_64BIT_PRIMITIVE", "AT_STALE_THREAD_WRITE_OF_PRIMITIVE"})
  public MessageBuilder publishingId(long publishingId) {
    this.publishingId = publishingId;
    this.hasPublishingId = true;
    return this;
  }

  @Override
  public PropertiesBuilder properties() {
    if (propertiesBuilder == null) {
      propertiesBuilder = new SwiftMqPropertiesBuilder(this);
    }
    return propertiesBuilder;
  }

  @Override
  public ApplicationPropertiesBuilder applicationProperties() {
    if (applicationPropertiesBuilder == null) {
      applicationPropertiesBuilder = new SwiftMqApplicationPropertiesBuilder(this);
    }
    return applicationPropertiesBuilder;
  }

  @Override
  public MessageAnnotationsBuilder messageAnnotations() {
    if (messageAnnotationsBuilder == null) {
      messageAnnotationsBuilder = new SwiftMqMessageAnnotationsBuilder(this);
    }
    return messageAnnotationsBuilder;
  }

  @Override
  public MessageBuilder addData(byte[] data) {
    outboundMessage.addData(new Data(data));
    return this;
  }

  private static class SwiftMqPropertiesBuilder implements PropertiesBuilder {

    private final com.swiftmq.amqp.v100.generated.messaging.message_format.Properties properties =
        new com.swiftmq.amqp.v100.generated.messaging.message_format.Properties();
    private final MessageBuilder messageBuilder;

    private SwiftMqPropertiesBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public PropertiesBuilder messageId(String id) {
      properties.setMessageId(new MessageIdString(id));
      return this;
    }

    @Override
    public PropertiesBuilder messageId(long id) {
      properties.setMessageId(new MessageIdUlong(id));
      return this;
    }

    @Override
    public PropertiesBuilder messageId(byte[] id) {
      properties.setMessageId(new MessageIdBinary(id));
      return this;
    }

    @Override
    public PropertiesBuilder messageId(UUID id) {
      properties.setMessageId(new MessageIdUuid(id));
      return this;
    }

    @Override
    public PropertiesBuilder userId(byte[] userId) {
      properties.setUserId(new AMQPBinary(userId));
      return this;
    }

    @Override
    public PropertiesBuilder to(String address) {
      properties.setTo(new AddressString(address));
      return this;
    }

    @Override
    public PropertiesBuilder subject(String subject) {
      properties.setSubject(new AMQPString(subject));
      return this;
    }

    @Override
    public PropertiesBuilder replyTo(String replyTo) {
      properties.setReplyTo(new AddressString(replyTo));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(String correlationId) {
      properties.setCorrelationId(new MessageIdString(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(long correlationId) {
      properties.setCorrelationId(new MessageIdUlong(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(byte[] correlationId) {
      properties.setCorrelationId(new MessageIdBinary(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(UUID correlationId) {
      properties.setCorrelationId(new MessageIdUuid(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder contentType(String contentType) {
      properties.setContentType(new AMQPSymbol(contentType));
      return this;
    }

    @Override
    public PropertiesBuilder contentEncoding(String contentEncoding) {
      properties.setContentEncoding(new AMQPSymbol(contentEncoding));
      return this;
    }

    @Override
    public PropertiesBuilder absoluteExpiryTime(long absoluteExpiryTime) {
      properties.setAbsoluteExpiryTime(new AMQPTimestamp(absoluteExpiryTime));
      return this;
    }

    @Override
    public PropertiesBuilder creationTime(long creationTime) {
      properties.setCreationTime(new AMQPTimestamp(creationTime));
      return this;
    }

    @Override
    public PropertiesBuilder groupId(String groupId) {
      properties.setGroupId(new AMQPString(groupId));
      return this;
    }

    @Override
    public PropertiesBuilder groupSequence(long groupSequence) {
      properties.setGroupSequence(new SequenceNo(groupSequence));
      return this;
    }

    @Override
    public PropertiesBuilder replyToGroupId(String replyToGroupId) {
      properties.setReplyToGroupId(new AMQPString(replyToGroupId));
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }

  private static class AmqpMapBuilderSupport {

    protected final Map<AMQPType, AMQPType> map = new LinkedHashMap<>();
    private final Function<String, AMQPType> keyMaker;

    private AmqpMapBuilderSupport(Function<String, AMQPType> keyMaker) {
      this.keyMaker = keyMaker;
    }

    protected void addEntry(String key, boolean value) {
      map.put(keyMaker.apply(key), new AMQPBoolean(value));
    }

    protected void addEntry(String key, byte value) {
      map.put(keyMaker.apply(key), new AMQPByte(value));
    }

    protected void addEntry(String key, short value) {
      map.put(keyMaker.apply(key), new AMQPShort(value));
    }

    protected void addEntry(String key, int value) {
      map.put(keyMaker.apply(key), new AMQPInt(value));
    }

    protected void addEntry(String key, long value) {
      map.put(keyMaker.apply(key), new AMQPLong(value));
    }

    protected void addEntryUnsigned(String key, byte value) {
      map.put(keyMaker.apply(key), new AMQPUnsignedByte(value));
    }

    protected void addEntryUnsigned(String key, short value) {
      map.put(keyMaker.apply(key), new AMQPUnsignedShort(value));
    }

    protected void addEntryUnsigned(String key, int value) {
      map.put(keyMaker.apply(key), new AMQPUnsignedInt(value));
    }

    protected void addEntryUnsigned(String key, long value) {
      map.put(keyMaker.apply(key), new AMQPUnsignedLong(value));
    }

    protected void addEntry(String key, float value) {
      map.put(keyMaker.apply(key), new AMQPFloat(value));
    }

    protected void addEntry(String key, double value) {
      map.put(keyMaker.apply(key), new AMQPDouble(value));
    }

    protected void addEntry(String key, char value) {
      map.put(keyMaker.apply(key), new AMQPChar(value));
    }

    protected void addEntryTimestamp(String key, long value) {
      map.put(keyMaker.apply(key), new AMQPTimestamp(value));
    }

    protected void addEntry(String key, UUID value) {
      map.put(keyMaker.apply(key), value == null ? AMQPNull.NULL : new AMQPUuid(value));
    }

    protected void addEntry(String key, byte[] value) {
      map.put(keyMaker.apply(key), value == null ? AMQPNull.NULL : new AMQPBinary(value));
    }

    protected void addEntry(String key, String value) {
      map.put(keyMaker.apply(key), value == null ? AMQPNull.NULL : new AMQPString(value));
    }

    protected void addEntrySymbol(String key, String value) {
      map.put(keyMaker.apply(key), value == null ? AMQPNull.NULL : new AMQPSymbol(value));
    }

    protected void addEntry(String key, List<?> list) {
      AMQPType amqpValue;
      if (list == null) {
        amqpValue = AMQPNull.NULL;
      } else {
        List<AMQPType> l = new ArrayList<>(list.size());
        for (Object o : list) {
          l.add(convertToSwiftMqType(o));
        }
        try {
          amqpValue = new AMQPList(l);
        } catch (IOException e) {
          throw new StreamException("Error while creating SwiftMq list", e);
        }
      }
      map.put(keyMaker.apply(key), amqpValue);
    }

    protected void addEntry(String key, Map<?, ?> mapEntry) {
      AMQPType amqpValue;
      if (mapEntry == null) {
        amqpValue = AMQPNull.NULL;
      } else {
        Map<AMQPType, AMQPType> m = new LinkedHashMap<>(mapEntry.size());
        mapEntry.forEach(
            (k, v) -> {
              m.put(convertToSwiftMqType(k), convertToSwiftMqType(v));
            });
        try {
          amqpValue = new AMQPMap(m);
        } catch (IOException e) {
          throw new StreamException("Error while creating SwiftMQ map", e);
        }
      }
      map.put(keyMaker.apply(key), amqpValue);
    }

    protected void addEntry(String key, Object[] array) {
      AMQPType amqpValue;
      if (array == null) {
        amqpValue = AMQPNull.NULL;
      } else {
        AMQPType[] a = new AMQPType[array.length];
        for (int i = 0; i < array.length; i++) {
          a[i] = convertToSwiftMqType(Array.get(array, i));
        }
        try {
          int code = a.length == 0 ? AMQPTypeDecoder.UNKNOWN : toSwiftMqTypeCode(array[0]);
          amqpValue = new AMQPArray(code, a);
        } catch (IOException e) {
          throw new StreamException("Error while creating SwiftMq list", e);
        }
      }
      map.put(keyMaker.apply(key), amqpValue);
    }
  }

  private static class SwiftMqApplicationPropertiesBuilder extends AmqpMapBuilderSupport
      implements ApplicationPropertiesBuilder {

    private static final Function<String, AMQPType> KEY_MAKER = key -> new AMQPString(key);

    private final MessageBuilder messageBuilder;

    private SwiftMqApplicationPropertiesBuilder(MessageBuilder messageBuilder) {
      super(KEY_MAKER);
      this.messageBuilder = messageBuilder;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, boolean value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, byte value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, short value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, int value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, long value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, byte value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, short value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, int value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, long value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, float value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, double value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryDecimal32(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationPropertiesBuilder entryDecimal64(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationPropertiesBuilder entryDecimal128(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, char value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryTimestamp(String key, long value) {
      addEntryTimestamp(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, UUID value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, byte[] value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, String value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entrySymbol(String key, String value) {
      addEntrySymbol(key, value);
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }

  private static class SwiftMqMessageAnnotationsBuilder extends AmqpMapBuilderSupport
      implements MessageAnnotationsBuilder {

    private static final Function<String, AMQPType> KEY_MAKER = key -> new AMQPSymbol(key);

    private final MessageBuilder messageBuilder;

    private SwiftMqMessageAnnotationsBuilder(MessageBuilder messageBuilder) {
      super(KEY_MAKER);
      this.messageBuilder = messageBuilder;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, boolean value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, short value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, int value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, long value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, byte value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, short value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, int value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, long value) {
      addEntryUnsigned(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, float value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, double value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryDecimal32(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageAnnotationsBuilder entryDecimal64(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageAnnotationsBuilder entryDecimal128(String key, BigDecimal value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, char value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryTimestamp(String key, long value) {
      addEntryTimestamp(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, UUID value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte[] value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, String value) {
      addEntry(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entrySymbol(String key, String value) {
      addEntrySymbol(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, List<?> list) {
      addEntry(key, list);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, Map<?, ?> map) {
      addEntry(key, map);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryArray(String key, Object[] array) {
      addEntry(key, array);
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }
}
