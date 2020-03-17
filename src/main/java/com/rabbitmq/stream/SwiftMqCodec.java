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
import com.swiftmq.tools.util.DataByteArrayOutputStream;

import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class SwiftMqCodec implements Codec {

    @Override
    public EncodedMessage encode(Message message) {
        AMQPMessage outboundMessage = new AMQPMessage();
        if (message.getProperties() != null) {
            Properties headers = message.getProperties();
            com.swiftmq.amqp.v100.generated.messaging.message_format.Properties properties = new com.swiftmq.amqp.v100.generated.messaging.message_format.Properties();
            boolean propertiesSet = false;
            if (headers.getMessageId() != null) {
                if (headers.getMessageId() instanceof String) {
                    properties.setMessageId(new MessageIdString(headers.getMessageIdAsString()));
                } else if (headers.getMessageId().getClass().isPrimitive() || headers.getMessageId() instanceof Long) {
                    properties.setMessageId(new MessageIdUlong(headers.getMessageIdAsLong()));
                } else if (headers.getMessageId().getClass().isArray()) {
                    properties.setMessageId(new MessageIdBinary(headers.getMessageIdAsBinary()));
                } else if (headers.getMessageId() instanceof UUID) {
                    properties.setMessageId(new MessageIdUuid(headers.getMessageIdAsUuid()));
                } else {
                    throw new IllegalStateException("Type not supported for message ID:" + properties.getMessageId().getClass());
                }
                propertiesSet = true;
            }

            if (headers.getUserId() != null) {
                properties.setUserId(new AMQPBinary(headers.getUserAsBinary()));
                propertiesSet = true;
            }

            if (headers.getTo() != null) {
                properties.setTo(new AddressString(headers.getTo()));
                propertiesSet = true;
            }

            if (headers.getSubject() != null) {
                properties.setSubject(new AddressString(headers.getSubject()));
                propertiesSet = true;
            }

            if (headers.getReplyTo() != null) {
                properties.setReplyTo(new AddressString(headers.getReplyTo()));
                propertiesSet = true;
            }

            if (headers.getCorrelationId() != null) {
                if (headers.getCorrelationId() instanceof String) {
                    properties.setCorrelationId(new MessageIdString(headers.getCorrelationIdAsString()));
                } else if (headers.getCorrelationId().getClass().isPrimitive() || headers.getCorrelationId() instanceof Long) {
                    properties.setCorrelationId(new MessageIdUlong(headers.getCorrelationIdAsLong()));
                } else if (headers.getCorrelationId().getClass().isArray()) {
                    properties.setCorrelationId(new MessageIdBinary(headers.getCorrelationIdAsBinary()));
                } else if (headers.getCorrelationId() instanceof UUID) {
                    properties.setCorrelationId(new MessageIdUuid(headers.getCorrelationIdAsUuid()));
                } else {
                    throw new IllegalStateException("Type not supported for correlation ID:" + properties.getCorrelationId().getClass());
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

        if (message.getApplicationProperties() != null && !message.getApplicationProperties().isEmpty()) {
            Map<AMQPType, AMQPType> applicationProperties = new LinkedHashMap<>(message.getApplicationProperties().size());
            for (Map.Entry<String, Object> entry : message.getApplicationProperties().entrySet()) {
                applicationProperties.put(new AMQPString(entry.getKey()), convertApplicationProperty(entry.getValue()));
            }
            try {
                outboundMessage.setApplicationProperties(new ApplicationProperties(applicationProperties));
            } catch (IOException e) {
                throw new ClientException("Error while setting application properties", e);
            }
        }

        if (message.getBodyAsBinary() != null) {
            outboundMessage.addData(new Data(message.getBodyAsBinary()));
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
            throw new ClientException("Error while writing AMQP 1.0 message to output stream");
        }
    }

    protected AMQPType convertApplicationProperty(Object value) {
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
        } else if (value instanceof Float) {
            return new AMQPFloat((Float) value);
        } else if (value instanceof Double) {
            return new AMQPDouble((Double) value);
        } else if (value instanceof byte[]) {
            return new AMQPBinary((byte[]) value);
        } else if (value instanceof String) {
            return new AMQPString((String) value);
        } else {
            throw new IllegalArgumentException("Type not supported for an application property: " + value.getClass());
        }
    }

    protected Object convertApplicationProperty(AMQPType value) {
        if (value instanceof AMQPBoolean) {
            return ((AMQPBoolean) value).getValue() ? Boolean.TRUE : Boolean.FALSE;
        } else if (value instanceof AMQPByte) {
            return ((AMQPByte) value).getValue();
        } else if (value instanceof AMQPShort) {
            return ((AMQPShort) value).getValue();
        } else if (value instanceof AMQPInt) {
            return ((AMQPInt) value).getValue();
        } else if (value instanceof AMQPLong) {
            return ((AMQPLong) value).getValue();
        } else if (value instanceof AMQPFloat) {
            return ((AMQPFloat) value).getValue();
        } else if (value instanceof AMQPDouble) {
            return ((AMQPDouble) value).getValue();
        } else if (value instanceof AMQPBinary) {
            return ((AMQPBinary) value).getValue();
        } else if (value instanceof AMQPString) {
            return ((AMQPString) value).getValue();
        } else {
            throw new IllegalArgumentException("Type not supported for an application property: " + value.getClass());
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

    protected Properties createProperties(AMQPMessage amqpMessage) {
        if (amqpMessage.getProperties() != null) {
            return new SwiftMqProperties(amqpMessage.getProperties());
        } else {
            return null;
        }
    }

    protected Map<String, Object> createApplicationProperties(AMQPMessage amqpMessage) {
        Map<String, Object> applicationProperties;
        if (amqpMessage.getApplicationProperties() != null) {
            Map<AMQPType, AMQPType> amqpApplicationProperties = null;
            try {
                amqpApplicationProperties = amqpMessage.getApplicationProperties().getValue();
            } catch (IOException e) {
                throw new ClientException("Error while reading application properties", e);
            }
            applicationProperties = new LinkedHashMap<>(amqpApplicationProperties.size());
            for (Map.Entry<AMQPType, AMQPType> entry : amqpApplicationProperties.entrySet()) {
                applicationProperties.put(entry.getKey().getValueString(), convertApplicationProperty(entry.getValue()));
            }
        } else {
            applicationProperties = null;
        }
        return applicationProperties;
    }

    protected Message createMessage(byte[] data) {
        AMQPMessage amqpMessage;
        try {
            amqpMessage = new AMQPMessage(data);
        } catch (Exception e) {
            throw new ClientException("Error while decoding AMQP 1.0 message");
        }

        Object body = extractBody(amqpMessage);

        Properties properties = createProperties(amqpMessage);

        Map<String, Object> applicationProperties = createApplicationProperties(amqpMessage);
        return new SwiftMqMessage(amqpMessage, body, properties, applicationProperties);
    }

    private static final class SwiftMqMessage implements Message {

        private final AMQPMessage amqpMessage;
        private final Object body;
        private final Properties properties;
        private final Map<String, Object> applicationProperties;


        private SwiftMqMessage(AMQPMessage amqpMessage, Object body, Properties properties, Map<String, Object> applicationProperties) {
            this.amqpMessage = amqpMessage;
            this.body = body;
            this.properties = properties;
            this.applicationProperties = applicationProperties;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return amqpMessage.getData().get(0).getValue();
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
    }

    private static final class SwiftMqProperties implements Properties {

        private final com.swiftmq.amqp.v100.generated.messaging.message_format.Properties amqpProperties;

        private SwiftMqProperties(com.swiftmq.amqp.v100.generated.messaging.message_format.Properties amqpProperties) {
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
        public Object getUserId() {
            return amqpProperties.getUserId();
        }

        @Override
        public byte[] getUserAsBinary() {
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
            return amqpProperties.getGroupSequence().getValue();
        }

        @Override
        public String getReplyToGroupId() {
            return amqpProperties.getReplyToGroupId().getValue();
        }
    }
}
