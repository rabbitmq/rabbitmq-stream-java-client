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
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.codec.WritableBuffer;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class QpidProtonCodec implements Codec {
    @Override
    public EncodedMessage encode(Message message) {
        if (message instanceof QpidProtonAmqpMessageWrapper) {
            org.apache.qpid.proton.message.Message qpidMessage = ((QpidProtonAmqpMessageWrapper) message).message;
            int bufferSize;
            if (qpidMessage.getBody() instanceof Data) {
                bufferSize = ((Data) qpidMessage.getBody()).getValue().getLength() * 2;
            } else {
                bufferSize = 8192;
            }
            ByteArrayWritableBuffer writableBuffer = new ByteArrayWritableBuffer(bufferSize);
            qpidMessage.encode(writableBuffer);
            return new EncodedMessage(writableBuffer.getArrayLength(), writableBuffer.getArray());
        } else {
            throw new IllegalArgumentException("Message implementation not supported: " + message.getClass().getName());
        }
    }

    @Override
    public Message decode(byte[] data) {
        org.apache.qpid.proton.message.Message message = org.apache.qpid.proton.message.Message.Factory.create();
        message.decode(data, 0, data.length);
        return new QpidProtonMessage(message, createProperties(message), createApplicationProperties(message));
    }

    protected Properties createProperties(org.apache.qpid.proton.message.Message message) {
        if (message.getProperties() != null) {
            return new QpidProtonProperties(message.getProperties());
        } else {
            return null;
        }
    }

    protected Map<String, Object> createApplicationProperties(org.apache.qpid.proton.message.Message message) {
        Map<String, Object> applicationProperties;
        if (message.getApplicationProperties() != null) {
            Map<String, Object> nativeApplicationProperties = message.getApplicationProperties().getValue();
            applicationProperties = new LinkedHashMap<>(nativeApplicationProperties.size());
            for (Map.Entry<String, Object> entry : nativeApplicationProperties.entrySet()) {
                applicationProperties.put(entry.getKey(), convertApplicationProperty(entry.getValue()));
            }
        } else {
            applicationProperties = null;
        }
        return applicationProperties;
    }

    protected Object convertApplicationProperty(Object value) {
        if (value instanceof Boolean || value instanceof Byte ||
                value instanceof Short || value instanceof Integer ||
                value instanceof Long || value instanceof Float ||
                value instanceof Double || value instanceof String ||
                value instanceof Character) {
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
        } else {
            throw new IllegalArgumentException("Type not supported for an application property: " + value.getClass());
        }
    }

    @Override
    public MessageBuilder messageBuilder() {
        return new QpidProtonMessageBuilder();
    }

    private static final class QpidProtonProperties implements Properties {

        private final org.apache.qpid.proton.amqp.messaging.Properties properties;

        private final com.rabbitmq.stream.amqp.UnsignedInteger groupSequence;

        private QpidProtonProperties(org.apache.qpid.proton.amqp.messaging.Properties properties) {
            this.properties = properties;
            if (this.properties.getGroupSequence() != null) {
                this.groupSequence = com.rabbitmq.stream.amqp.UnsignedInteger.valueOf(this.properties.getGroupSequence().intValue());
            } else {
                this.groupSequence = null;
            }
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
        public Object getUserId() {
            return properties.getUserId();
        }

        @Override
        public byte[] getUserAsBinary() {
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
        public com.rabbitmq.stream.amqp.UnsignedInteger getGroupSequence() {
            return this.groupSequence;
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

        private QpidProtonMessage(org.apache.qpid.proton.message.Message message, Properties properties, Map<String, Object> applicationProperties) {
            this.message = message;
            this.properties = properties;
            this.applicationProperties = applicationProperties;
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
    }

    static class QpidProtonAmqpMessageWrapper implements Message {

        private final org.apache.qpid.proton.message.Message message;

        QpidProtonAmqpMessageWrapper(org.apache.qpid.proton.message.Message message) {
            this.message = message;
        }

        @Override
        public byte[] getBodyAsBinary() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object getBody() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Properties getProperties() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, Object> getApplicationProperties() {
            throw new UnsupportedOperationException();
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
