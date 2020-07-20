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

import com.rabbitmq.stream.*;
import io.netty.buffer.ByteBuf;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

class StreamProducer implements Producer {

    private final Client client;

    private final MessageAccumulator accumulator;

    // FIXME investigate a more optimized data structure to handle pending messages
    private final ConcurrentMap<Long, PendingMessage> unconfirmedMessages = new ConcurrentHashMap<>(10_000, 0.75f, 2);

    private final int batchSize;

    private final String stream;

    private final Client.OutboundEntityWriteCallback writeCallback;

    StreamProducer(String stream, int batchSize, Duration batchPublishingDelay, StreamEnvironment environment) {
        this.client = environment.getClientForPublisher(stream);
        this.stream = stream;
        this.accumulator = new MessageAccumulator(batchSize);
        this.batchSize = batchSize;
        this.writeCallback = new Client.OutboundEntityWriteCallback() {
            @Override
            public int write(ByteBuf bb, Object entity, long publishingId) {
                AccumulatedMessage accumulatedMessage = (AccumulatedMessage) entity;
                unconfirmedMessages.put(publishingId, new PendingMessage(
                        accumulatedMessage.message, accumulatedMessage.confirmationHandler, publishingId
                ));
                return Client.OUTBOUND_MESSAGE_WRITE_CALLBACK.write(bb, accumulatedMessage.encodedMessage, publishingId);
            }

            @Override
            public int fragmentLength(Object entity) {
                return Client.OUTBOUND_MESSAGE_WRITE_CALLBACK.fragmentLength(((AccumulatedMessage) entity).encodedMessage);
            }
        };
        if (!batchPublishingDelay.isNegative() && !batchPublishingDelay.isZero()) {
            AtomicReference<Runnable> taskReference = new AtomicReference<>();
            Runnable task = () -> {
                synchronized (StreamProducer.this) {
                    publishBatch();
                }
                environment.getScheduledExecutorService().schedule(taskReference.get(), batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
            };
            taskReference.set(task);
            environment.getScheduledExecutorService().schedule(task, batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
        // FIXME deal with publish error
        this.client.addPublisherConfirmListener(publishingId -> {
            PendingMessage pendingMessage = unconfirmedMessages.remove(publishingId);
            if (pendingMessage != null) {
                pendingMessage.confirmationHandler.handle(new ConfirmationStatus(pendingMessage.message, true, null));
            }
        });
    }

    @Override
    public MessageBuilder messageBuilder() {
        return client.messageBuilder();
    }

    @Override
    public void send(Message message, ConfirmationHandler confirmationHandler) {
        Codec.EncodedMessage encodedMessage = this.client.encode(this.stream, message);
        AccumulatedMessage accumulatedMessage = new AccumulatedMessage(message, confirmationHandler, encodedMessage);

        if (!accumulator.add(accumulatedMessage)) {
            synchronized (this) {
                publishBatch();
                accumulator.add(accumulatedMessage);
            }
        }
    }

    @Override
    public void close() {
        // FIXME remove from environment
        // FIXME remove publish confirm callback from environment
    }

    private void publishBatch() {
        if (!accumulator.messages.isEmpty()) {
            List<Object> messages = new ArrayList<>(this.batchSize);
            int batchCount = 0;
            while (batchCount != this.batchSize) {
                AccumulatedMessage accMessage = accumulator.messages.poll();
                if (accMessage == null) {
                    break;
                }
                messages.add(accMessage);
                batchCount++;
            }
            client.publishInternal(this.stream, messages, this.writeCallback);
        }
    }

    static class AccumulatedMessage {

        private final Message message;
        private final ConfirmationHandler confirmationHandler;
        private final Codec.EncodedMessage encodedMessage;

        private AccumulatedMessage(Message message, ConfirmationHandler confirmationHandler, Codec.EncodedMessage encodedMessage) {
            this.message = message;
            this.confirmationHandler = confirmationHandler;
            this.encodedMessage = encodedMessage;
        }
    }

    static class PendingMessage {

        private final Message message;
        private final ConfirmationHandler confirmationHandler;
        private final long publishingId;

        private PendingMessage(Message message, ConfirmationHandler confirmationHandler, long publishingId) {
            this.message = message;
            this.confirmationHandler = confirmationHandler;
            this.publishingId = publishingId;
        }
    }
}
