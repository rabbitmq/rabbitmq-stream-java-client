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
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;

import java.util.concurrent.atomic.AtomicBoolean;

public class StreamConsumer implements Consumer {

    private final Runnable closingCallback;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final StreamEnvironment environment;

    StreamConsumer(String stream, OffsetSpecification offsetSpecification, MessageHandler messageHandler, StreamEnvironment environment) {
        this.closingCallback = environment.registerConsumer(stream, offsetSpecification, messageHandler);
        this.environment = environment;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            this.environment.removeConsumer(this);
            closeFromEnvironment();
        }
    }

    void closeFromEnvironment() {
        this.closingCallback.run();
        closed.set(true);
    }

    boolean isOpen() {
        return !this.closed.get();
    }
}
