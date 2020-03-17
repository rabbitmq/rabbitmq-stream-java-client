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

package com.rabbitmq.stream.amqp;

import com.swiftmq.amqp.v100.generated.messaging.message_format.Data;
import com.swiftmq.amqp.v100.generated.messaging.message_format.MessageIdUlong;
import com.swiftmq.amqp.v100.generated.messaging.message_format.Properties;
import com.swiftmq.amqp.v100.messaging.AMQPMessage;
import com.swiftmq.amqp.v100.types.AMQPSymbol;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.qpid.jms.provider.amqp.message.AmqpReadableBuffer;
import org.apache.qpid.proton.codec.ReadableBuffer;
import org.apache.qpid.proton.message.Message;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

public class SwiftMqTest {

    @Test
    public void writeReadMessage() throws Exception {
        AMQPMessage message = new AMQPMessage();
        Properties properties = new Properties();
        properties.setContentType(new AMQPSymbol("application/json"));
        properties.setCorrelationId(new MessageIdUlong(1));
        message.setProperties(properties);
        message.addData(new Data("hello".getBytes()));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(out);
        message.writeContent(dataOutput);

        AMQPMessage readMessage = new AMQPMessage(out.toByteArray());
        assertThat(readMessage.getProperties().getContentType().getValue()).isEqualTo("application/json");
        assertThat(readMessage.getProperties().getCorrelationId().getValueString()).isEqualTo("1");
        assertThat(readMessage.getData()).hasSize(1);
        assertThat(readMessage.getData().get(0).getValue()).isEqualTo("hello".getBytes());

        Message protonMessage = Message.Factory.create();
        ReadableBuffer readableBuffer = ReadableBuffer.ByteBufferReader.wrap(out.toByteArray());
        protonMessage.decode(readableBuffer);
        assertThat(protonMessage.getContentType()).isEqualTo("application/json");
        assertThat(protonMessage.getCorrelationId().toString()).isEqualTo("1");
        assertThat(protonMessage.getBody()).isInstanceOf(org.apache.qpid.proton.amqp.messaging.Data.class);
        assertThat(((org.apache.qpid.proton.amqp.messaging.Data) protonMessage.getBody()).getValue().getArray()).isEqualTo("hello".getBytes());


        ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes(out.toByteArray(), 0, out.size());
        readableBuffer = new AmqpReadableBuffer(buffer);
        protonMessage.decode(readableBuffer);
        assertThat(protonMessage.getContentType()).isEqualTo("application/json");
        assertThat(protonMessage.getCorrelationId().toString()).isEqualTo("1");
        assertThat(protonMessage.getBody()).isInstanceOf(org.apache.qpid.proton.amqp.messaging.Data.class);
        assertThat(((org.apache.qpid.proton.amqp.messaging.Data) protonMessage.getBody()).getValue().getArray()).isEqualTo("hello".getBytes());
    }

}
