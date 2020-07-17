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

import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ProducerBuilder;

import java.time.Duration;

class StreamProducerBuilder implements ProducerBuilder {

    private final StreamEnvironment environment;

    private String stream;

    private int batchSize = 100;

    private Duration batchPublishingDelay = Duration.ofMillis(100);

    StreamProducerBuilder(StreamEnvironment environment) {
        this.environment = environment;
    }

    public StreamProducerBuilder stream(String stream) {
        this.stream = stream;
        return this;
    }

    public StreamProducerBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public StreamProducerBuilder batchPublishingDelay(Duration batchPublishingDelay) {
        this.batchPublishingDelay = batchPublishingDelay;
        return this;
    }

    public Producer build() {
        Producer producer = new StreamProducer(stream, batchSize, batchPublishingDelay, environment);
        this.environment.addProducer(producer);
        return producer;
    }
}
