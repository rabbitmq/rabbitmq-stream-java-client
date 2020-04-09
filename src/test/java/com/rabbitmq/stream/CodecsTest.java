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

import com.rabbitmq.stream.amqp.UnsignedByte;
import com.rabbitmq.stream.amqp.UnsignedInteger;
import com.rabbitmq.stream.amqp.UnsignedLong;
import com.rabbitmq.stream.amqp.UnsignedShort;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class CodecsTest {

    static Charset CHARSET = StandardCharsets.UTF_8;

    static UUID TEST_UUID = UUID.randomUUID();

    static Iterable<CodecCouple> codecsCouples() {
        List<Codec> codecs = Arrays.asList(new QpidProtonCodec(), new SwiftMqCodec());
        List<CodecCouple> couples = new ArrayList<>();
        for (Codec serializer : codecs) {
            for (Codec deserializer : codecs) {
                couples.add(new CodecCouple(serializer, deserializer));
            }
        }
        return couples;
    }

    @ParameterizedTest
    @MethodSource("codecsCouples")
    void codecs(CodecCouple codecCouple) {
        Codec serializer = codecCouple.serializer;
        Codec deserializer = codecCouple.deserializer;

        Stream<MessageTestConfiguration> messageOperations = Stream.of(
                test(
                        builder -> builder.properties().messageId(42).messageBuilder(),
                        message -> assertThat(message.getProperties().getMessageIdAsLong()).isEqualTo(42)
                ),
                test(
                        builder -> builder.properties().messageId("foo").messageBuilder(),
                        message -> assertThat(message.getProperties().getMessageIdAsString()).isEqualTo("foo")
                ),
                test(
                        builder -> builder.properties().messageId("bar".getBytes(CHARSET)).messageBuilder(),
                        message -> assertThat(message.getProperties().getMessageIdAsBinary()).isEqualTo("bar".getBytes(CHARSET))
                ),
                // UUID broken in SwiftMQ, https://github.com/iitsoftware/swiftmq-client/issues/36
//                test(
//                        builder -> builder.properties().messageId(TEST_UUID).messageBuilder(),
//                        message -> assertThat(message.getProperties().getMessageIdAsUuid()).isEqualTo(TEST_UUID)
//                ),
                test(
                        builder -> builder.properties().correlationId(42 + 10).messageBuilder(),
                        message -> assertThat(message.getProperties().getCorrelationIdAsLong()).isEqualTo(42 + 10)
                ),
                test(
                        builder -> builder.properties().correlationId("correlation foo").messageBuilder(),
                        message -> assertThat(message.getProperties().getCorrelationIdAsString()).isEqualTo("correlation foo")
                ),
                test(
                        builder -> builder.properties().correlationId("correlation bar".getBytes(CHARSET)).messageBuilder(),
                        message -> assertThat(message.getProperties().getCorrelationIdAsBinary()).isEqualTo("correlation bar".getBytes(CHARSET))
                ),
                // UUID broken in SwiftMQ, https://github.com/iitsoftware/swiftmq-client/issues/36
//                test(
//                        builder -> builder.properties().correlationId(TEST_UUID).messageBuilder(),
//                        message -> assertThat(message.getProperties().getCorrelationIdAsUuid()).isEqualTo(TEST_UUID)
//                )
                test(
                        builder -> builder.properties().groupSequence(10).messageBuilder(),
                        message -> assertThat(message.getProperties().getGroupSequence()).isNotNull()
                                .extracting(v -> v.intValue()).isEqualTo(10)
                ),
                test(
                        builder -> builder.properties().groupSequence((long) Integer.MAX_VALUE + 10).messageBuilder(),
                        message -> assertThat(message.getProperties().getGroupSequence()).isNotNull()
                                .extracting(v -> v.longValue()).isEqualTo((long) Integer.MAX_VALUE + 10)
                )
        );

        String body = "hello";
        String userId = "yoda";
        String to = "the to address";
        String subject = "the subject";
        String replyTo = "the reply to";
        String contentType = "text/plain";
        String contentEncoding = "gzip";
        String groupId = "the group ID";
        String replyToGroupId = "the reply to group ID";
        long now = new Date().getTime();
        messageOperations.forEach(messageTestConfiguration -> {
            Function<MessageBuilder, MessageBuilder> messageOperation = messageTestConfiguration.messageOperation;
            Consumer<Message> messageExpectation = messageTestConfiguration.messageExpectation;
            Message outboundMessage = messageOperation.apply(serializer.messageBuilder())
                    .addData(body.getBytes(CHARSET))
                    .properties()
                    .userId(userId.getBytes(CHARSET))
                    .to(to)
                    .subject(subject)
                    .replyTo(replyTo)
                    .contentType(contentType)
                    .contentEncoding(contentEncoding)
                    .absoluteExpiryTime(now + 1000)
                    .creationTime(now)
                    .groupId(groupId)
                    .replyToGroupId(replyToGroupId)
                    .messageBuilder()
                    .applicationProperties()
                    .entry("byte", (byte) 1)
                    .entry("short", (short) 2)
                    .entry("int", 3)
                    .entry("long", 4l)
                    .entryUnsigned("ubyte", (byte) 1)
                    .entryUnsigned("ushort", (short) 2)
                    .entryUnsigned("uint", 3)
                    .entryUnsigned("ulong", 4l)
                    .entryUnsigned("large.ubyte", (byte) (Byte.MAX_VALUE + 10))
                    .entryUnsigned("large.ushort", (short) (Short.MAX_VALUE + 10))
                    .entryUnsigned("large.uint", Integer.MAX_VALUE + 10)
                    .entryUnsigned("large.ulong", Long.MAX_VALUE + 10)
                    .entry("float", 3.14f)
                    .entry("double", 6.28)
                    .messageBuilder()
                    .build();
            Codec.EncodedMessage encoded = serializer.encode(outboundMessage);

            byte[] encodedData = new byte[encoded.getSize()];
            System.arraycopy(encoded.getData(), 0, encodedData, 0, encoded.getSize());
            Message inboundMessage = deserializer.decode(encodedData);

            messageExpectation.accept(inboundMessage);

            assertThat(new String(inboundMessage.getBodyAsBinary())).isEqualTo(body);

            assertThat(inboundMessage.getProperties().getUserAsBinary()).isEqualTo(userId.getBytes(CHARSET));
            assertThat(inboundMessage.getProperties().getTo()).isEqualTo(to);
            assertThat(inboundMessage.getProperties().getSubject()).isEqualTo(subject);
            assertThat(inboundMessage.getProperties().getReplyTo()).isEqualTo(replyTo);
            assertThat(inboundMessage.getProperties().getContentType()).isEqualTo(contentType);
            assertThat(inboundMessage.getProperties().getContentEncoding()).isEqualTo(contentEncoding);
            assertThat(inboundMessage.getProperties().getAbsoluteExpiryTime()).isEqualTo(now + 1000);
            assertThat(inboundMessage.getProperties().getCreationTime()).isEqualTo(now);
            assertThat(inboundMessage.getProperties().getGroupId()).isEqualTo(groupId);
            assertThat(inboundMessage.getProperties().getReplyToGroupId()).isEqualTo(replyToGroupId);

            assertThat(inboundMessage.getApplicationProperties().get("byte"))
                    .isNotNull().isInstanceOf(Byte.class).isEqualTo(Byte.valueOf((byte) 1));
            assertThat(inboundMessage.getApplicationProperties().get("short"))
                    .isNotNull().isInstanceOf(Short.class).isEqualTo(Short.valueOf((short) 2));
            assertThat(inboundMessage.getApplicationProperties().get("int"))
                    .isNotNull().isInstanceOf(Integer.class).isEqualTo(Integer.valueOf(3));
            assertThat(inboundMessage.getApplicationProperties().get("long"))
                    .isNotNull().isInstanceOf(Long.class).isEqualTo(Long.valueOf(4));

            assertThat(inboundMessage.getApplicationProperties().get("ubyte"))
                    .isNotNull().isInstanceOf(UnsignedByte.class).isEqualTo(UnsignedByte.valueOf((byte) 1));
            assertThat(inboundMessage.getApplicationProperties().get("ushort"))
                    .isNotNull().isInstanceOf(UnsignedShort.class).isEqualTo(UnsignedShort.valueOf((short) 2));
            assertThat(inboundMessage.getApplicationProperties().get("uint"))
                    .isNotNull().isInstanceOf(UnsignedInteger.class).isEqualTo(UnsignedInteger.valueOf(3));
            assertThat(inboundMessage.getApplicationProperties().get("ulong"))
                    .isNotNull().isInstanceOf(UnsignedLong.class).isEqualTo(UnsignedLong.valueOf(4));

            assertThat(inboundMessage.getApplicationProperties().get("large.ubyte"))
                    .isNotNull().isInstanceOf(UnsignedByte.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(UnsignedByte.class))
                    .extracting(v -> v.intValue()).isEqualTo(Byte.MAX_VALUE + 10);
            assertThat(inboundMessage.getApplicationProperties().get("large.ushort"))
                    .isNotNull().isInstanceOf(UnsignedShort.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(UnsignedShort.class))
                    .extracting(v -> v.intValue()).isEqualTo(Short.MAX_VALUE + 10);
            assertThat(inboundMessage.getApplicationProperties().get("large.uint"))
                    .isNotNull().isInstanceOf(UnsignedInteger.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(UnsignedInteger.class))
                    .extracting(v -> v.toString()).isEqualTo(BigInteger.valueOf((long) Integer.MAX_VALUE + 10L).toString());
            assertThat(inboundMessage.getApplicationProperties().get("large.ulong"))
                    .isNotNull().isInstanceOf(UnsignedLong.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(UnsignedLong.class))
                    .extracting(v -> v.toString()).isEqualTo(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.TEN).toString());

            assertThat(inboundMessage.getApplicationProperties().get("float"))
                    .isNotNull().isInstanceOf(Float.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(Float.class))
                    .isEqualTo(Float.valueOf(3.14f));
            assertThat(inboundMessage.getApplicationProperties().get("double"))
                    .isNotNull().isInstanceOf(Double.class)
                    .asInstanceOf(InstanceOfAssertFactories.type(Double.class))
                    .isEqualTo(Double.valueOf(6.28));
        });


    }

    MessageTestConfiguration test(Function<MessageBuilder, MessageBuilder> messageOperation, Consumer<Message> messageExpectation) {
        return new MessageTestConfiguration(messageOperation, messageExpectation);
    }

    static class MessageTestConfiguration {

        final Function<MessageBuilder, MessageBuilder> messageOperation;
        final Consumer<Message> messageExpectation;

        MessageTestConfiguration(Function<MessageBuilder, MessageBuilder> messageOperation, Consumer<Message> messageExpectation) {
            this.messageOperation = messageOperation;
            this.messageExpectation = messageExpectation;
        }
    }

    static class CodecCouple {

        final Codec serializer;
        final Codec deserializer;

        CodecCouple(Codec serializer, Codec deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public String toString() {
            return "serializer=" + serializer.getClass().getSimpleName() +
                    ", deserializer=" + deserializer.getClass().getSimpleName();
        }
    }

}
