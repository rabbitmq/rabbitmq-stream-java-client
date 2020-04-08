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

import org.apache.qpid.proton.amqp.*;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class QpidProtonMessageBuilder implements MessageBuilder {

    private final org.apache.qpid.proton.message.Message message = org.apache.qpid.proton.message.Message.Factory.create();

    private QpidProtonjPropertiesBuilder propertiesBuilder;

    private QpidProtonjApplicationPropertiesBuilder applicationPropertiesBuilder;

    @Override
    public Message build() {
        if (propertiesBuilder != null) {
            message.setProperties(propertiesBuilder.properties);
        }
        if (applicationPropertiesBuilder != null) {
            message.setApplicationProperties(new ApplicationProperties(applicationPropertiesBuilder.applicationProperties));
        }
        return new QpidProtonCodec.QpidProtonAmqpMessageWrapper(message);
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
    public MessageBuilder addData(byte[] data) {
        message.setBody(new Data(new Binary(data)));
        return this;
    }

    private static class QpidProtonjPropertiesBuilder implements PropertiesBuilder {

        private final org.apache.qpid.proton.amqp.messaging.Properties properties = new org.apache.qpid.proton.amqp.messaging.Properties();
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
        public MessageBuilder messageBuilder() {
            return messageBuilder;
        }
    }

    private static class QpidProtonjApplicationPropertiesBuilder implements ApplicationPropertiesBuilder {

        private final Map<String, Object> applicationProperties = new LinkedHashMap<>();
        private final MessageBuilder messageBuilder;

        private QpidProtonjApplicationPropertiesBuilder(MessageBuilder messageBuilder) {
            this.messageBuilder = messageBuilder;
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
        public MessageBuilder messageBuilder() {
            return messageBuilder;
        }
    }

}
