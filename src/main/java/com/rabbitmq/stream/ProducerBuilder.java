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

package com.rabbitmq.stream;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class ProducerBuilder {

    private Client client;

    private String stream;

    private int batchSize = 100;

    private Duration batchPublishingDelay = Duration.ofMillis(100);

    private ScheduledExecutorService scheduledExecutorService;

    public ProducerBuilder client(Client client) {
        this.client = client;
        return this;
    }

    public ProducerBuilder stream(String stream) {
        this.stream = stream;
        return this;
    }

    public ProducerBuilder batchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public ProducerBuilder batchPublishingDelay(Duration batchPublishingDelay) {
        this.batchPublishingDelay = batchPublishingDelay;
        return this;
    }

    public ProducerBuilder scheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
        return this;
    }

    public Producer build() {
        return new StreamProducer(client, stream, batchSize, batchPublishingDelay, scheduledExecutorService);
    }
}
