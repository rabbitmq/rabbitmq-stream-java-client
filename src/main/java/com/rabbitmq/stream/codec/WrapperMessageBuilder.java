// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
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
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Properties;
import com.rabbitmq.stream.amqp.*;
import java.math.BigDecimal;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class WrapperMessageBuilder implements MessageBuilder {

  private Object body;
  private WrapperPropertiesBuilder propertiesBuilder;
  private WrapperApplicationPropertiesBuilder applicationPropertiesBuilder;
  private WrapperMessageAnnotationsBuilder messageAnnotationsBuilder;

  @Override
  public Message build() {
    return new SimpleMessage(
        body,
        this.messageAnnotationsBuilder == null
            ? null
            : this.messageAnnotationsBuilder.messageAnnotations,
        this.propertiesBuilder == null ? null : this.propertiesBuilder.properties,
        this.applicationPropertiesBuilder == null
            ? null
            : this.applicationPropertiesBuilder.applicationProperties);
  }

  @Override
  public PropertiesBuilder properties() {
    if (this.propertiesBuilder == null) {
      this.propertiesBuilder = new WrapperPropertiesBuilder(this);
    }
    return this.propertiesBuilder;
  }

  @Override
  public ApplicationPropertiesBuilder applicationProperties() {
    if (this.applicationPropertiesBuilder == null) {
      this.applicationPropertiesBuilder = new WrapperApplicationPropertiesBuilder(this);
    }
    return this.applicationPropertiesBuilder;
  }

  @Override
  public MessageAnnotationsBuilder messageAnnotations() {
    if (this.messageAnnotationsBuilder == null) {
      this.messageAnnotationsBuilder = new WrapperMessageAnnotationsBuilder(this);
    }
    return this.messageAnnotationsBuilder;
  }

  @Override
  public MessageBuilder addData(byte[] data) {
    this.body = data;
    return this;
  }

  private static class WrapperMessageAnnotationsBuilder implements MessageAnnotationsBuilder {

    private final MessageBuilder messageBuilder;
    private final Map<String, Object> messageAnnotations = new LinkedHashMap<>();

    private WrapperMessageAnnotationsBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, boolean value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, short value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, int value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, long value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, byte value) {
      messageAnnotations.put(key, new UnsignedByte(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, short value) {
      messageAnnotations.put(key, new UnsignedShort(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, int value) {
      messageAnnotations.put(key, new UnsignedInteger(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryUnsigned(String key, long value) {
      messageAnnotations.put(key, new UnsignedLong(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, float value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, double value) {
      messageAnnotations.put(key, value);
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
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entryTimestamp(String key, long value) {
      messageAnnotations.put(key, new Date(value));
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, UUID value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, byte[] value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entry(String key, String value) {
      messageAnnotations.put(key, value);
      return this;
    }

    @Override
    public MessageAnnotationsBuilder entrySymbol(String key, String value) {
      messageAnnotations.put(key, Symbol.valueOf(value));
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return this.messageBuilder;
    }
  }

  private static class WrapperPropertiesBuilder implements PropertiesBuilder {

    private final MessageBuilder messageBuilder;
    private final SimpleProperties properties = new SimpleProperties();

    private WrapperPropertiesBuilder(MessageBuilder messageBuilder) {
      this.messageBuilder = messageBuilder;
    }

    @Override
    public PropertiesBuilder messageId(String id) {
      properties.messageId = id;
      return this;
    }

    @Override
    public PropertiesBuilder messageId(long id) {
      properties.messageId = id;
      return this;
    }

    @Override
    public PropertiesBuilder messageId(byte[] id) {
      properties.messageId = id;
      return this;
    }

    @Override
    public PropertiesBuilder messageId(UUID id) {
      properties.messageId = id;
      return this;
    }

    @Override
    public PropertiesBuilder userId(byte[] userId) {
      properties.userId = userId;
      return this;
    }

    @Override
    public PropertiesBuilder to(String address) {
      properties.to = address;
      return this;
    }

    @Override
    public PropertiesBuilder subject(String subject) {
      properties.subject = subject;
      return this;
    }

    @Override
    public PropertiesBuilder replyTo(String replyTo) {
      properties.replyTo = replyTo;
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(String correlationId) {
      properties.correlationId = correlationId;
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(long correlationId) {
      properties.correlationId = correlationId;
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(byte[] correlationId) {
      properties.correlationId = correlationId;
      return this;
    }

    @Override
    public PropertiesBuilder correlationId(UUID correlationId) {
      properties.correlationId = correlationId;
      return this;
    }

    @Override
    public PropertiesBuilder contentType(String contentType) {
      properties.contentType = contentType;
      return this;
    }

    @Override
    public PropertiesBuilder contentEncoding(String contentEncoding) {
      properties.contentEncoding = contentEncoding;
      return this;
    }

    @Override
    public PropertiesBuilder absoluteExpiryTime(long absoluteExpiryTime) {
      properties.absoluteExpiryTime = absoluteExpiryTime;
      return this;
    }

    @Override
    public PropertiesBuilder creationTime(long creationTime) {
      properties.creationTime = creationTime;
      return this;
    }

    @Override
    public PropertiesBuilder groupId(String groupId) {
      properties.groupId = groupId;
      return this;
    }

    @Override
    public PropertiesBuilder groupSequence(long groupSequence) {
      properties.groupSequence = groupSequence;
      return this;
    }

    @Override
    public PropertiesBuilder replyToGroupId(String replyToGroupId) {
      properties.replyToGroupId = replyToGroupId;
      return this;
    }

    @Override
    public MessageBuilder messageBuilder() {
      return this.messageBuilder;
    }
  }

  private static class WrapperApplicationPropertiesBuilder implements ApplicationPropertiesBuilder {

    private final MessageBuilder messageBuilder;
    private final Map<String, Object> applicationProperties = new LinkedHashMap<>();

    private WrapperApplicationPropertiesBuilder(MessageBuilder messageBuilder) {
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
    public ApplicationPropertiesBuilder entry(String key, UUID value) {
      applicationProperties.put(key, value);
      return this;
    }

    @Override
    public ApplicationPropertiesBuilder entry(String key, byte[] value) {
      applicationProperties.put(key, value);
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
      return this.messageBuilder;
    }
  }

  private static class SimpleMessage implements Message {

    private final Object body;
    private final Map<String, Object> messageAnnotations;
    private final Properties properties;
    private final Map<String, Object> applicationProperties;

    private SimpleMessage(
        Object body,
        Map<String, Object> messageAnnotations,
        Properties properties,
        Map<String, Object> applicationProperties) {
      this.body = body;
      this.messageAnnotations = messageAnnotations;
      this.properties = properties;
      this.applicationProperties = applicationProperties;
    }

    @Override
    public byte[] getBodyAsBinary() {
      return (byte[]) body;
    }

    @Override
    public Object getBody() {
      return body;
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
  }

  private static class SimpleProperties implements Properties {

    private Object messageId;
    private byte[] userId;
    private String to;
    private String subject;
    private String replyTo;
    private Object correlationId;
    private String contentType;
    private String contentEncoding;
    private long absoluteExpiryTime;
    private long creationTime;
    private String groupId;
    private long groupSequence = -1;
    private String replyToGroupId;

    @Override
    public Object getMessageId() {
      return messageId;
    }

    @Override
    public String getMessageIdAsString() {
      return (String) messageId;
    }

    @Override
    public long getMessageIdAsLong() {
      return (Long) messageId;
    }

    @Override
    public byte[] getMessageIdAsBinary() {
      return (byte[]) messageId;
    }

    @Override
    public UUID getMessageIdAsUuid() {
      return (UUID) messageId;
    }

    @Override
    public byte[] getUserId() {
      return userId;
    }

    @Override
    public String getTo() {
      return to;
    }

    @Override
    public String getSubject() {
      return subject;
    }

    @Override
    public String getReplyTo() {
      return replyTo;
    }

    @Override
    public Object getCorrelationId() {
      return correlationId;
    }

    @Override
    public String getCorrelationIdAsString() {
      return (String) correlationId;
    }

    @Override
    public long getCorrelationIdAsLong() {
      return (long) correlationId;
    }

    @Override
    public byte[] getCorrelationIdAsBinary() {
      return (byte[]) correlationId;
    }

    @Override
    public UUID getCorrelationIdAsUuid() {
      return (UUID) correlationId;
    }

    @Override
    public String getContentType() {
      return contentType;
    }

    @Override
    public String getContentEncoding() {
      return contentEncoding;
    }

    @Override
    public long getAbsoluteExpiryTime() {
      return absoluteExpiryTime;
    }

    @Override
    public long getCreationTime() {
      return creationTime;
    }

    @Override
    public String getGroupId() {
      return groupId;
    }

    @Override
    public long getGroupSequence() {
      return groupSequence;
    }

    @Override
    public String getReplyToGroupId() {
      return replyToGroupId;
    }
  }
}
