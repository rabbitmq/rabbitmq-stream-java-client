// Copyright (c) 2020-2024 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * API to configure and create a {@link Message}.
 *
 * <p>A {@link MessageBuilder} is meant to create only one message, it should not be re-used for
 * several message instances.
 *
 * <p>Please see section 3.2 "message format" of the AMQP 1.0 specification to find out about the
 * exact meaning of the message sections.
 *
 * @see Message
 */
public interface MessageBuilder {

  /**
   * Create the message.
   *
   * @return the message
   */
  Message build();

  /**
   * Set the publishing ID (for deduplication).
   *
   * <p>This is value is used only for outbound messages and is not persisted.
   *
   * @param publishingId
   * @return this builder instance
   * @see ProducerBuilder#name(String)
   * @see <a
   *     href="https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#outbound-message-deduplication">Deduplication
   *     documentation</a>
   */
  MessageBuilder publishingId(long publishingId);

  /**
   * The builder for {@link Properties}.
   *
   * @return the properties builder
   * @see Message#getProperties()
   */
  PropertiesBuilder properties();

  /**
   * The builder to set application properties.
   *
   * @return the application properties builder
   * @see Message#getApplicationProperties()
   */
  ApplicationPropertiesBuilder applicationProperties();

  /**
   * The builder to set message annotations.
   *
   * @return the message annotations builder
   * @see Message#getMessageAnnotations()
   */
  MessageAnnotationsBuilder messageAnnotations();

  /**
   * Set binary data of the message.
   *
   * @param data
   * @return this builder instance
   * @see Message#getBodyAsBinary()
   */
  MessageBuilder addData(byte[] data);

  /**
   * API to set application properties.
   *
   * @see Message#getApplicationProperties()
   */
  interface ApplicationPropertiesBuilder {

    ApplicationPropertiesBuilder entry(String key, boolean value);

    ApplicationPropertiesBuilder entry(String key, byte value);

    ApplicationPropertiesBuilder entry(String key, short value);

    ApplicationPropertiesBuilder entry(String key, int value);

    ApplicationPropertiesBuilder entry(String key, long value);

    ApplicationPropertiesBuilder entryUnsigned(String key, byte value);

    ApplicationPropertiesBuilder entryUnsigned(String key, short value);

    ApplicationPropertiesBuilder entryUnsigned(String key, int value);

    ApplicationPropertiesBuilder entryUnsigned(String key, long value);

    ApplicationPropertiesBuilder entry(String key, float value);

    ApplicationPropertiesBuilder entry(String key, double value);

    ApplicationPropertiesBuilder entryDecimal32(String key, BigDecimal value);

    ApplicationPropertiesBuilder entryDecimal64(String key, BigDecimal value);

    ApplicationPropertiesBuilder entryDecimal128(String key, BigDecimal value);

    ApplicationPropertiesBuilder entry(String key, char value);

    ApplicationPropertiesBuilder entryTimestamp(String key, long value);

    ApplicationPropertiesBuilder entry(String key, UUID value);

    ApplicationPropertiesBuilder entry(String key, byte[] value);

    ApplicationPropertiesBuilder entry(String key, String value);

    ApplicationPropertiesBuilder entrySymbol(String key, String value);

    /**
     * Go back to the message builder
     *
     * @return the message builder
     */
    MessageBuilder messageBuilder();
  }

  /**
   * API to set message annotations.
   *
   * @see Message#getMessageAnnotations()
   */
  interface MessageAnnotationsBuilder {

    MessageAnnotationsBuilder entry(String key, boolean value);

    MessageAnnotationsBuilder entry(String key, byte value);

    MessageAnnotationsBuilder entry(String key, short value);

    MessageAnnotationsBuilder entry(String key, int value);

    MessageAnnotationsBuilder entry(String key, long value);

    MessageAnnotationsBuilder entryUnsigned(String key, byte value);

    MessageAnnotationsBuilder entryUnsigned(String key, short value);

    MessageAnnotationsBuilder entryUnsigned(String key, int value);

    MessageAnnotationsBuilder entryUnsigned(String key, long value);

    MessageAnnotationsBuilder entry(String key, float value);

    MessageAnnotationsBuilder entry(String key, double value);

    MessageAnnotationsBuilder entryDecimal32(String key, BigDecimal value);

    MessageAnnotationsBuilder entryDecimal64(String key, BigDecimal value);

    MessageAnnotationsBuilder entryDecimal128(String key, BigDecimal value);

    MessageAnnotationsBuilder entry(String key, char value);

    MessageAnnotationsBuilder entryTimestamp(String key, long value);

    MessageAnnotationsBuilder entry(String key, UUID value);

    MessageAnnotationsBuilder entry(String key, byte[] value);

    MessageAnnotationsBuilder entry(String key, String value);

    MessageAnnotationsBuilder entrySymbol(String key, String value);

    /**
     * Go back to the message builder
     *
     * @return the message builder
     */
    MessageBuilder messageBuilder();
  }

  /**
   * API to set message properties.
   *
   * @see Message#getProperties()
   */
  interface PropertiesBuilder {

    /**
     * Set the message ID as a string.
     *
     * @param id
     * @return this properties builder
     */
    PropertiesBuilder messageId(String id);

    /**
     * Set the message ID as long.
     *
     * @param id
     * @return this properties builder
     */
    PropertiesBuilder messageId(long id);

    /**
     * Set the message ID as an array of bytes.
     *
     * @param id
     * @return this properties builder
     */
    PropertiesBuilder messageId(byte[] id);

    /**
     * Set the message ID as an UUID.
     *
     * @param id
     * @return this properties builder
     */
    PropertiesBuilder messageId(UUID id);

    /**
     * Set the user ID.
     *
     * @param userId
     * @return this properties builder
     */
    PropertiesBuilder userId(byte[] userId);

    /**
     * Set the to address.
     *
     * @param address
     * @return this properties builder
     */
    PropertiesBuilder to(String address);

    /**
     * Set the subject.
     *
     * @param subject
     * @return this properties builder
     */
    PropertiesBuilder subject(String subject);

    /**
     * Set the reply to address.
     *
     * @param replyTo
     * @return this properties builder
     */
    PropertiesBuilder replyTo(String replyTo);

    /**
     * Set the correlation ID as a string.
     *
     * @param correlationId
     * @return this properties builder
     */
    PropertiesBuilder correlationId(String correlationId);

    /**
     * Set the correlation ID as a long.
     *
     * @param correlationId
     * @return this properties builder
     */
    PropertiesBuilder correlationId(long correlationId);

    /**
     * Set the correlation ID as an array of bytes.
     *
     * @param correlationId
     * @return this properties builder
     */
    PropertiesBuilder correlationId(byte[] correlationId);

    /**
     * Set the correlation ID as an UUID.
     *
     * @param correlationId
     * @return this properties builder
     */
    PropertiesBuilder correlationId(UUID correlationId);

    /**
     * Set the content type.
     *
     * @param contentType
     * @return this properties builder
     */
    PropertiesBuilder contentType(String contentType);

    /**
     * Set the content encoding.
     *
     * @param contentEncoding
     * @return this properties builder
     */
    PropertiesBuilder contentEncoding(String contentEncoding);

    /**
     * Set the expiry time.
     *
     * @param absoluteExpiryTime
     * @return this properties builder
     */
    PropertiesBuilder absoluteExpiryTime(long absoluteExpiryTime);

    /**
     * Set the creation time.
     *
     * @param creationTime
     * @return this properties builder
     */
    PropertiesBuilder creationTime(long creationTime);

    /**
     * Set the group ID.
     *
     * @param groupId
     * @return this properties builder
     */
    PropertiesBuilder groupId(String groupId);

    /**
     * Set the group sequence.
     *
     * @param groupSequence
     * @return this properties builder
     */
    PropertiesBuilder groupSequence(long groupSequence);

    /**
     * Set the reply-to group ID.
     *
     * @param replyToGroupId
     * @return this properties builder
     */
    PropertiesBuilder replyToGroupId(String replyToGroupId);

    /**
     * Go back to the message builder.
     *
     * @return the message builder
     */
    MessageBuilder messageBuilder();
  }
}
