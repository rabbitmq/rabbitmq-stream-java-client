// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
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

import static com.rabbitmq.stream.codec.QpidProtonCodec.EMPTY_BODY;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.UnsignedByte;
import org.apache.qpid.proton.amqp.UnsignedInteger;
import org.apache.qpid.proton.amqp.UnsignedLong;
import org.apache.qpid.proton.amqp.UnsignedShort;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;

class QpidProtonMessageBuilder implements MessageBuilder {

  private final org.apache.qpid.proton.message.Message message =
      org.apache.qpid.proton.message.Message.Factory.create();
  private final AtomicBoolean built = new AtomicBoolean(false);
  private boolean hasPublishingId = false;
  private long publishingId = 0;
  private QpidProtonjPropertiesBuilder propertiesBuilder;
  private QpidProtonjApplicationPropertiesBuilder applicationPropertiesBuilder;
  private QpidProtonjMessageAnnotationsBuilder messageAnnotationsBuilder;

  @Override
  public Message build() {
    if (built.compareAndSet(false, true)) {
      if (propertiesBuilder != null) {
        message.setProperties(propertiesBuilder.properties);
      }
      if (applicationPropertiesBuilder != null) {
        message.setApplicationProperties(
            new ApplicationProperties(applicationPropertiesBuilder.applicationProperties));
      }
      if (messageAnnotationsBuilder != null) {
        message.setMessageAnnotations(
            new MessageAnnotations(messageAnnotationsBuilder.messageAnnotations));
      }
      if (message.getBody() == null) {
        message.setBody(EMPTY_BODY);
      }
      return new QpidProtonCodec.QpidProtonAmqpMessageWrapper(
          hasPublishingId, publishingId, message);
    } else {
      throw new IllegalStateException("A message builder can build only one message");
    }
  }

  @Override
  public MessageBuilder publishingId(long publishingId) {
    this.publishingId = publishingId;
    this.hasPublishingId = true;
    return this;
  }

  @Override
  public PropertiesBuilder properties() {
    if (propertiesBuilder == null) {
      propertiesBuilder = new QpidProtonjPropertiesBuilder(this);
    }
    return propertiesBuilder;
  }

  @Override
  public ApplicationPropertiesBuilder applicationProperties() {
    if (applicationPropertiesBuilder == null) {
      applicationPropertiesBuilder = new QpidProtonjApplicationPropertiesBuilder(this);
    }
    return applicationPropertiesBuilder;
  }

  @Override
  public MessageAnnotationsBuilder messageAnnotations() {
    if (messageAnnotationsBuilder == null) {
      messageAnnotationsBuilder = new QpidProtonjMessageAnnotationsBuilder(this);
    }
    return messageAnnotationsBuilder;
  }

  @Override
  public MessageBuilder addData(byte[] data) {
    message.setBody(new Data(new Binary(data)));
    return this;
  }

  private static class QpidProtonjPropertiesBuilder implements PropertiesBuilder {

    private final org.apache.qpid.proton.amqp.messaging.Properties properties =
        new org.apache.qpid.proton.amqp.messaging.Properties();
    private final MessageBuilder messageBuilder;

    private QpidProtonjPropertiesBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public PropertiesBuilder messageId(String id) {
      properties.setMessageId(id);
      return this;
    }

    @Override
    public PropertiesBuilder messageId(long id) {
      properties.setMessageId(new UnsignedLong(id));
      return this;
    }

    @Override
    public PropertiesBuilder messageId(byte[] id) {
      properties.setMessageId(new Binary(id));
      return this;
    }

    @Override
    public PropertiesBuilder messageId(UUID id) {
      properties.setMessageId(id);
      return this;
    }

    @Override
    public PropertiesBuilder userId(byte[] userId) {
      properties.setUserId(new Binary(userId));
      return this;
    }

    @Override
    public PropertiesBuilder to(String address) {
      properties.setTo(address);
      return this;
    }

    @Override
    public PropertiesBuilder subject(String subject) {
      properties.setSubject(subject);
      return this;
    }

    @Override
    public PropertiesBuilder replyTo(String replyTo) {
      properties.setReplyTo(replyTo);
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(String correlationId) {
      properties.setCorrelationId(correlationId);
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(long correlationId) {
      properties.setCorrelationId(new UnsignedLong(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(byte[] correlationId) {
      properties.setCorrelationId(new Binary(correlationId));
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(UUID correlationId) {
      properties.setCorrelationId(correlationId);
      return this;
    }

    @Override
    public PropertiesBuilder contentType(String contentType) {
      properties.setContentType(Symbol.valueOf(contentType));
      return this;
    }

    @Override
    public PropertiesBuilder contentEncoding(String contentEncoding) {
      properties.setContentEncoding(Symbol.valueOf(contentEncoding));
      return this;
    }

    @Override
    public PropertiesBuilder absoluteExpiryTime(long absoluteExpiryTime) {
      properties.setAbsoluteExpiryTime(new Date(absoluteExpiryTime));
      return this;
    }

    @Override
    public PropertiesBuilder creationTime(long creationTime) {
      properties.setCreationTime(new Date(creationTime));
      return this;
    }

    @Override
    public PropertiesBuilder groupId(String groupId) {
      properties.setGroupId(groupId);
      return this;
    }

    @Override
    public PropertiesBuilder groupSequence(long groupSequence) {
      properties.setGroupSequence(UnsignedInteger.valueOf(groupSequence));
      return this;
    }

    @Override
    public PropertiesBuilder replyToGroupId(String replyToGroupId) {
      properties.setReplyToGroupId(replyToGroupId);
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }

  private static class QpidProtonjMessageAnnotationsBuilder implements MessageAnnotationsBuilder {

    private final Map<Symbol, Object> messageAnnotations = new LinkedHashMap<>();

    private final MessageBuilder messageBuilder;

    private QpidProtonjMessageAnnotationsBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, boolean value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, short value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, int value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, long value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, byte value) {
      messageAnnotations.put(Symbol.getSymbol(key), new UnsignedByte(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, short value) {
      messageAnnotations.put(Symbol.getSymbol(key), new UnsignedShort(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, int value) {
      messageAnnotations.put(Symbol.getSymbol(key), new UnsignedInteger(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, long value) {
      messageAnnotations.put(Symbol.getSymbol(key), new UnsignedLong(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, float value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, double value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
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
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryTimestamp(String key, long value) {
      messageAnnotations.put(Symbol.getSymbol(key), new Date(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, UUID uuid) {
      messageAnnotations.put(Symbol.getSymbol(key), uuid);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte[] value) {
      messageAnnotations.put(Symbol.getSymbol(key), new Binary(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, String value) {
      messageAnnotations.put(Symbol.getSymbol(key), value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entrySymbol(String key, String value) {
      messageAnnotations.put(Symbol.getSymbol(key), Symbol.valueOf(value));
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }

  private static class QpidProtonjApplicationPropertiesBuilder
      implements ApplicationPropertiesBuilder {

    private final Map<String, Object> applicationProperties = new LinkedHashMap<>();
    private final MessageBuilder messageBuilder;

    private QpidProtonjApplicationPropertiesBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, boolean value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, byte value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, short value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, int value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, long value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, byte value) {
      applicationProperties.put(key, new UnsignedByte(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, short value) {
      applicationProperties.put(key, new UnsignedShort(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, int value) {
      applicationProperties.put(key, new UnsignedInteger(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryUnsigned(String key, long value) {
      applicationProperties.put(key, new UnsignedLong(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, float value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, double value) {
      applicationProperties.put(key, value);
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
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entryTimestamp(String key, long value) {
      applicationProperties.put(key, new Date(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, UUID uuid) {
      applicationProperties.put(key, uuid);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, byte[] value) {
      applicationProperties.put(key, new Binary(value));
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, String value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entrySymbol(String key, String value) {
      applicationProperties.put(key, Symbol.valueOf(value));
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return messageBuilder;
    }
  }
}
