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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.rabbitmq.stream.Constants.CODE_MESSAGE_ENQUEUEING_FAILED;

class StreamProducer implements Producer {

    private final Client client;

    private final MessageAccumulator accumulator;

    // FIXME investigate a more optimized data structure to handle pending messages
    private final ConcurrentMap<Long, ConfirmationCallback> unconfirmedMessages = new ConcurrentHashMap<>(10_000, 0.75f, 2);

    private final int batchSize;

    private final String stream;

    private final Client.OutboundEntityWriteCallback writeCallback;

    private final Semaphore unconfirmedMessagesSemaphore;

    StreamProducer(String stream,
                   int subEntrySize, int batchSize, Duration batchPublishingDelay,
                   int maxUnconfirmedMessages,
                   StreamEnvironment environment) {
        this.client = environment.getClientForPublisher(stream);
        this.stream = stream;
        final Client.OutboundEntityWriteCallback delegateWriteCallback;
        if (subEntrySize <= 1) {
            this.accumulator = new SimpleMessageAccumulator(batchSize, environment.codec(), client.maxFrameSize(), this.stream);
            delegateWriteCallback = Client.OUTBOUND_MESSAGE_WRITE_CALLBACK;
        } else {
            this.accumulator = new SubEntryMessageAccumulator(subEntrySize, batchSize, environment.codec(), client.maxFrameSize(), this.stream);
            delegateWriteCallback = Client.OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK;
        }

        this.unconfirmedMessagesSemaphore = new Semaphore(maxUnconfirmedMessages, true);

        this.writeCallback = new Client.OutboundEntityWriteCallback() {
            @Override
            public int write(ByteBuf bb, Object entity, long publishingId) {
                MessageAccumulator.AccumulatedEntity accumulatedEntity = (MessageAccumulator.AccumulatedEntity) entity;
                unconfirmedMessages.put(publishingId, accumulatedEntity.confirmationCallback());
                return delegateWriteCallback.write(bb, accumulatedEntity.encodedEntity(), publishingId);
            }

            @Override
            public int fragmentLength(Object entity) {
                return delegateWriteCallback.fragmentLength(((MessageAccumulator.AccumulatedEntity) entity).encodedEntity());
            }
        };
        if (!batchPublishingDelay.isNegative() && !batchPublishingDelay.isZero()) {
            AtomicReference<Runnable> taskReference = new AtomicReference<>();
            Runnable task = () -> {
                synchronized (this) {
                    publishBatch();
                }
                environment.getScheduledExecutorService().schedule(taskReference.get(), batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
            };
            taskReference.set(task);
            environment.getScheduledExecutorService().schedule(task, batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
        }
        this.batchSize = batchSize;
        this.client.addPublishConfirmListener(publishingId -> {
            ConfirmationCallback confirmationCallback = unconfirmedMessages.remove(publishingId);
            if (confirmationCallback != null) {
                int confirmedCount = confirmationCallback.handle(true, Constants.RESPONSE_CODE_OK);
                this.unconfirmedMessagesSemaphore.release(confirmedCount);
            } else {
                unconfirmedMessagesSemaphore.release();
            }
        });
        this.client.addPublishErrorListener((publishingId, errorCode) -> {
            ConfirmationCallback confirmationCallback = unconfirmedMessages.remove(publishingId);
            unconfirmedMessagesSemaphore.release();
            if (confirmationCallback != null) {
                int nackedCount = confirmationCallback.handle(false, errorCode);
                this.unconfirmedMessagesSemaphore.release(nackedCount);
            } else {
                unconfirmedMessagesSemaphore.release();
            }
        });
    }

    @Override
    public MessageBuilder messageBuilder() {
        return client.messageBuilder();
    }

    @Override
    public void send(Message message, ConfirmationHandler confirmationHandler) {
        try {
            if (unconfirmedMessagesSemaphore.tryAcquire(10, TimeUnit.SECONDS)) {
                if (accumulator.add(message, confirmationHandler)) {
                    synchronized (this) {
                        publishBatch();
                    }
                }
            } else {
                confirmationHandler.handle(new ConfirmationStatus(message, false, CODE_MESSAGE_ENQUEUEING_FAILED));
            }
        } catch (InterruptedException e) {
            throw new StreamException("Interrupted while waiting to accumulate outbound message", e);
        }
    }

    @Override
    public void close() {
        // FIXME remove from environment
        // FIXME remove publish confirm callback from environment
    }

    private void publishBatch() {
        if (!accumulator.isEmpty()) {
            List<Object> messages = new ArrayList<>(this.batchSize);
            int batchCount = 0;
            while (batchCount != this.batchSize) {
                Object accMessage = accumulator.get();
                if (accMessage == null) {
                    break;
                }
                messages.add(accMessage);
                batchCount++;
            }
            client.publishInternal(this.stream, messages, this.writeCallback);
        }
    }

    interface ConfirmationCallback {

        int handle(boolean confirmed, short code);

    }

}
