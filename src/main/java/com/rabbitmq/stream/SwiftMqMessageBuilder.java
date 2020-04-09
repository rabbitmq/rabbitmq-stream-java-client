// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

import com.swiftmq.amqp.v100.generated.messaging.message_format.*;
import com.swiftmq.amqp.v100.generated.transport.definitions.SequenceNo;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class SwiftMqMessageBuilder implements MessageBuilder {

    private final AMQPMessage outboundMessage = new AMQPMessage();

    private SwiftMqPropertiesBuilder propertiesBuilder;

    private SwiftMqApplicationPropertiesBuilder applicationPropertiesBuilder;

    @Override
    public Message build() {
        if (propertiesBuilder != null) {
            outboundMessage.setProperties(propertiesBuilder.properties);
        }
        if (applicationPropertiesBuilder != null) {
            try {
                outboundMessage.setApplicationProperties(new ApplicationProperties(applicationPropertiesBuilder.applicationProperties));
            } catch (IOException e) {
                throw new ClientException("Error while setting application properties", e);
            }
        }
        return new SwiftMqCodec.SwiftMqAmqpMessageWrapper(outboundMessage);
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
    public MessageBuilder addData(byte[] data) {
        outboundMessage.addData(new Data(data));
        return this;
    }

    private static class SwiftMqPropertiesBuilder implements PropertiesBuilder {

        private final com.swiftmq.amqp.v100.generated.messaging.message_format.Properties properties = new com.swiftmq.amqp.v100.generated.messaging.message_format.Properties();
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

    private static class SwiftMqApplicationPropertiesBuilder implements ApplicationPropertiesBuilder {

        private final Map<AMQPType, AMQPType> applicationProperties = new LinkedHashMap<>();
        private final MessageBuilder messageBuilder;

        private SwiftMqApplicationPropertiesBuilder(MessageBuilder messageBuilder) {
            this.messageBuilder = messageBuilder;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, byte value) {
            applicationProperties.put(new AMQPString(key), new AMQPByte(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, short value) {
            applicationProperties.put(new AMQPString(key), new AMQPShort(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, int value) {
            applicationProperties.put(new AMQPString(key), new AMQPInt(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, long value) {
            applicationProperties.put(new AMQPString(key), new AMQPLong(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entryUnsigned(String key, byte value) {
            applicationProperties.put(new AMQPString(key), new AMQPUnsignedByte(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entryUnsigned(String key, short value) {
            applicationProperties.put(new AMQPString(key), new AMQPUnsignedShort(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entryUnsigned(String key, int value) {
            applicationProperties.put(new AMQPString(key), new AMQPUnsignedInt(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entryUnsigned(String key, long value) {
            applicationProperties.put(new AMQPString(key), new AMQPUnsignedLong(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, float value) {
            applicationProperties.put(new AMQPString(key), new AMQPFloat(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, double value) {
            applicationProperties.put(new AMQPString(key), new AMQPDouble(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entry(String key, char value) {
            applicationProperties.put(new AMQPString(key), new AMQPChar(value));
            return this;
        }

        @Override
        public ApplicationPropertiesBuilder entryTimestamp(String key, long value) {
            applicationProperties.put(new AMQPString(key), new AMQPTimestamp(value));
            return this;
        }

        @Override
        public MessageBuilder messageBuilder() {
            return messageBuilder;
        }
    }

}
