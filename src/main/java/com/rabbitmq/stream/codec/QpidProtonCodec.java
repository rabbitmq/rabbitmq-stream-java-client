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


import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.Properties;
import org.apache.qpid.proton.amqp.*;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.MessageAnnotations;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

public class QpidProtonCodec implements Codec {
    private static final Function<String, String> MESSAGE_ANNOTATIONS_STRING_KEY_EXTRACTOR = k -> k;
    private static final Function<Symbol, String> MESSAGE_ANNOTATIONS_SYMBOL_KEY_EXTRACTOR = Symbol::toString;

    private static Map<String, Object> createApplicationProperties(org.apache.qpid.proton.message.Message message) {
        if (message.getApplicationProperties() != null) {
            return createMapFromAmqpMap(MESSAGE_ANNOTATIONS_STRING_KEY_EXTRACTOR, message.getApplicationProperties().getValue());
        } else {
            return null;
        }
    }

    private static Map<String, Object> createMessageAnnotations(org.apache.qpid.proton.message.Message message) {
        if (message.getMessageAnnotations() != null) {
            return createMapFromAmqpMap(MESSAGE_ANNOTATIONS_SYMBOL_KEY_EXTRACTOR, message.getMessageAnnotations().getValue());
        } else {
            return null;
        }
    }

    private static <K> Map<String, Object> createMapFromAmqpMap(Function<K, String> keyMaker, Map<K, Object> amqpMap) {
        Map<String, Object> result;
        if (amqpMap != null) {
            result = new LinkedHashMap<>(amqpMap.size());
            for (Map.Entry<K, Object> entry : amqpMap.entrySet()) {
                result.put(keyMaker.apply(entry.getKey()), convertApplicationProperty(entry.getValue()));
            }
        } else {
            result = null;
        }
        return result;
    }

    private static Object convertApplicationProperty(Object value) {
        if (value instanceof Boolean || value instanceof Byte ||
                value instanceof Short || value instanceof Integer ||
                value instanceof Long || value instanceof Float ||
                value instanceof Double || value instanceof String ||
                value instanceof Character || value instanceof UUID) {
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
        } else {
            throw new IllegalArgumentException("Type not supported for an application property: " + value.getClass());
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
                org.apache.qpid.proton.amqp.messaging.Properties properties = new org.apache.qpid.proton.amqp.messaging.Properties();
                boolean propertiesSet = false;
                if (headers.getMessageId() != null) {
                    if (headers.getMessageId() instanceof String) {
                        properties.setMessageId(headers.getMessageIdAsString());
                    } else if (headers.getMessageId().getClass().isPrimitive() || headers.getMessageId() instanceof Long) {
                        properties.setMessageId(new UnsignedLong(headers.getMessageIdAsLong()));
                    } else if (headers.getMessageId().getClass().isArray()) {
                        properties.setMessageId(new Binary(headers.getMessageIdAsBinary()));
                    } else if (headers.getMessageId() instanceof UUID) {
                        properties.setMessageId(headers.getMessageIdAsUuid());
                    } else {
                        throw new IllegalStateException("Type not supported for message ID:" + properties.getMessageId().getClass());
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
                    } else if (headers.getCorrelationId().getClass().isPrimitive() || headers.getCorrelationId() instanceof Long) {
                        properties.setCorrelationId(new UnsignedLong(headers.getCorrelationIdAsLong()));
                    } else if (headers.getCorrelationId().getClass().isArray()) {
                        properties.setCorrelationId(new Binary(headers.getCorrelationIdAsBinary()));
                    } else if (headers.getCorrelationId() instanceof UUID) {
                        properties.setCorrelationId(headers.getCorrelationIdAsUuid());
                    } else {
                        throw new IllegalStateException("Type not supported for correlation ID:" + properties.getCorrelationId().getClass());
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

            if (message.getApplicationProperties() != null && !message.getApplicationProperties().isEmpty()) {
                Map<String, Object> applicationProperties = new LinkedHashMap<>(message.getApplicationProperties().size());
                for (Map.Entry<String, Object> entry : message.getApplicationProperties().entrySet()) {
                    applicationProperties.put(entry.getKey(), convertToQpidType(entry.getValue()));
                }
                qpidMessage.setApplicationProperties(new ApplicationProperties(applicationProperties));
            }

            if (message.getMessageAnnotations() != null && !message.getMessageAnnotations().isEmpty()) {
                Map<Symbol, Object> messageAnnotations = new LinkedHashMap<>(message.getMessageAnnotations().size());
                for (Map.Entry<String, Object> entry : message.getMessageAnnotations().entrySet()) {
                    messageAnnotations.put(Symbol.getSymbol(entry.getKey()), convertToQpidType(entry.getValue()));
                }
                qpidMessage.setMessageAnnotations(new MessageAnnotations(messageAnnotations));
            }

            if (message.getBodyAsBinary() != null) {
                qpidMessage.setBody(new Data(new Binary(message.getBodyAsBinary())));
            }
        }
        int bufferSize;
        if (qpidMessage.getBody() instanceof Data) {
            bufferSize = ((Data) qpidMessage.getBody()).getValue().getLength() * 2;
        } else {
            bufferSize = 8192;
        }
        ByteArrayWritableBuffer writableBuffer = new ByteArrayWritableBuffer(bufferSize);
        qpidMessage.encode(writableBuffer);
        return new EncodedMessage(writableBuffer.getArrayLength(), writableBuffer.getArray());
    }

    @Override
    public Message decode(byte[] data) {
        org.apache.qpid.proton.message.Message message = org.apache.qpid.proton.message.Message.Factory.create();
        message.decode(data, 0, data.length);
        return new QpidProtonMessage(message, createProperties(message),
                createApplicationProperties(message), createMessageAnnotations(message));
    }

    protected Properties createProperties(org.apache.qpid.proton.message.Message message) {
        if (message.getProperties() != null) {
            return new QpidProtonProperties(message.getProperties());
        } else {
            return null;
        }
    }

    protected Object convertToQpidType(Object value) {
        if (value instanceof Boolean || value instanceof Byte ||
                value instanceof Short || value instanceof Integer ||
                value instanceof Long || value instanceof Float ||
                value instanceof Double || value instanceof String ||
                value instanceof Character || value instanceof UUID ||
                value instanceof Date) {
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
        } else {
            throw new IllegalArgumentException("Type not supported for an application property: " + value.getClass());
        }
    }

    @Override
    public MessageBuilder messageBuilder() {
        return new QpidProtonMessageBuilder();
    }

    private static final class QpidProtonProperties implements Properties {

        private static final long NULL_GROUP_SEQUENCE = -1L;
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
            return properties.getMessageId().toString();
        }

        @Override
        public long getMessageIdAsLong() {
            return ((UnsignedLong) properties.getMessageId()).longValue();
        }

        @Override
        public byte[] getMessageIdAsBinary() {
            return ((Binary) properties.getMessageId()).getArray();
        }

        @Override
        public UUID getMessageIdAsUuid() {
            return (UUID) properties.getMessageId();
        }

        @Override
        public byte[] getUserId() {
            return properties.getUserId().getArray();
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
            return properties.getCorrelationId().toString();
        }

        @Override
        public long getCorrelationIdAsLong() {
            return ((UnsignedLong) properties.getCorrelationId()).longValue();
        }

        @Override
        public byte[] getCorrelationIdAsBinary() {
            return ((Binary) properties.getCorrelationId()).getArray();
        }

        @Override
        public UUID getCorrelationIdAsUuid() {
            return (UUID) properties.getCorrelationId();
        }

        @Override
        public String getContentType() {
            return properties.getContentType().toString();
        }

        @Override
        public String getContentEncoding() {
            return properties.getContentEncoding().toString();
        }

        @Override
        public long getAbsoluteExpiryTime() {
            return properties.getAbsoluteExpiryTime().getTime();
        }

        @Override
        public long getCreationTime() {
            return properties.getCreationTime().getTime();
        }

        @Override
        public String getGroupId() {
            return properties.getGroupId();
        }

        @Override
        public long getGroupSequence() {
            return properties.getGroupSequence() == null ? NULL_GROUP_SEQUENCE : properties.getGroupSequence().longValue();
        }

        @Override
        public String getReplyToGroupId() {
            return properties.getReplyToGroupId();
        }
    }

    private static class QpidProtonMessage implements Message {

        private final org.apache.qpid.proton.message.Message message;
        private final Properties properties;
        private final Map<String, Object> applicationProperties;
        private final Map<String, Object> messageAnnotations;

        private QpidProtonMessage(org.apache.qpid.proton.message.Message message, Properties properties,
                                  Map<String, Object> applicationProperties,
                                  Map<String, Object> messageAnnotations) {
            this.message = message;
            this.properties = properties;
            this.applicationProperties = applicationProperties;
            this.messageAnnotations = messageAnnotations;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return ((Data) message.getBody()).getValue().getArray();
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
    }

    static class QpidProtonAmqpMessageWrapper implements Message {

        private final org.apache.qpid.proton.message.Message message;
        private Properties properties;
        private Map<String, Object> applicationProperties;
        private Map<String, Object> messageAnnotations;

        QpidProtonAmqpMessageWrapper(org.apache.qpid.proton.message.Message message) {
            this.message = message;
        }

        @Override
        public byte[] getBodyAsBinary() {
            return ((Data) message.getBody()).getValue().getArray();
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
    }

    // from https://github.com/apache/activemq/blob/master/activemq-amqp/src/main/java/org/apache/activemq/transport/amqp/message/AmqpWritableBuffer.java
    private class ByteArrayWritableBuffer implements WritableBuffer {

        public final static int DEFAULT_CAPACITY = 4 * 1024;

        byte[] buffer;
        int position;

        /**
         * Creates a new WritableBuffer with default capacity.
         */
        public ByteArrayWritableBuffer() {
            this(DEFAULT_CAPACITY);
        }

        /**
         * Create a new WritableBuffer with the given capacity.
         */
        public ByteArrayWritableBuffer(int capacity) {
            this.buffer = new byte[capacity];
        }

        public byte[] getArray() {
            return buffer;
        }

        public int getArrayLength() {
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
         * Ensures the the buffer has at least the minimumCapacity specified.
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
