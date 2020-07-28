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

package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;

public class StreamConsumerBuilder implements ConsumerBuilder {

    private final StreamEnvironment environment;

    private String stream;
    private OffsetSpecification offsetSpecification = OffsetSpecification.first();
    private MessageHandler messageHandler;

    public StreamConsumerBuilder(StreamEnvironment environment) {
        this.environment = environment;
    }

    @Override
    public ConsumerBuilder stream(String stream) {
        this.stream = stream;
        return this;
    }

    @Override
    public ConsumerBuilder offset(OffsetSpecification offsetSpecification) {
        this.offsetSpecification = offsetSpecification;
        return this;
    }

    @Override
    public ConsumerBuilder messageHandler(MessageHandler messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    @Override
    public Consumer build() {
        StreamConsumer consumer = new StreamConsumer(stream, offsetSpecification, messageHandler, environment);
        environment.addConsumer(consumer);
        return consumer;
    }
}
