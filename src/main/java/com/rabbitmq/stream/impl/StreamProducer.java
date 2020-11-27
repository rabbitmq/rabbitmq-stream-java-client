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

import static com.rabbitmq.stream.Constants.*;
import static com.rabbitmq.stream.impl.Utils.formatConstant;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.impl.Client.Response;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProducer implements Producer {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProducer.class);

  private final MessageAccumulator accumulator;
  // FIXME investigate a more optimized data structure to handle pending messages
  private final ConcurrentMap<Long, ConfirmationCallback> unconfirmedMessages;
  private final int batchSize;
  private final String stream;
  private final Client.OutboundEntityWriteCallback writeCallback;
  private final Semaphore unconfirmedMessagesSemaphore;
  private final Runnable closingCallback;
  private final StreamEnvironment environment;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final int maxUnconfirmedMessages;
  private volatile Client client;
  private volatile byte publisherId;
  private volatile Status status;
  private final LongSupplier publishSequenceSupplier =
      new LongSupplier() {
        private final AtomicLong publishSequence = new AtomicLong(0);

        @Override
        public long getAsLong() {
          return publishSequence.getAndIncrement();
        }
      };

  StreamProducer(
      String stream,
      int subEntrySize,
      int batchSize,
      Duration batchPublishingDelay,
      int maxUnconfirmedMessages,
      StreamEnvironment environment) {
    this.environment = environment;
    this.stream = stream;
    this.closingCallback = environment.registerProducer(this, this.stream);
    final Client.OutboundEntityWriteCallback delegateWriteCallback;
    if (subEntrySize <= 1) {
      this.accumulator =
          new SimpleMessageAccumulator(batchSize, environment.codec(), client.maxFrameSize());
      delegateWriteCallback = Client.OUTBOUND_MESSAGE_WRITE_CALLBACK;
    } else {
      this.accumulator =
          new SubEntryMessageAccumulator(
              subEntrySize, batchSize, environment.codec(), client.maxFrameSize());
      delegateWriteCallback = Client.OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK;
    }

    this.maxUnconfirmedMessages = maxUnconfirmedMessages;
    this.unconfirmedMessagesSemaphore = new Semaphore(maxUnconfirmedMessages, true);
    this.unconfirmedMessages = new ConcurrentHashMap<>(this.maxUnconfirmedMessages, 0.75f, 2);

    this.writeCallback =
        new Client.OutboundEntityWriteCallback() {
          @Override
          public int write(ByteBuf bb, Object entity, long publishingId) {
            MessageAccumulator.AccumulatedEntity accumulatedEntity =
                (MessageAccumulator.AccumulatedEntity) entity;
            unconfirmedMessages.put(publishingId, accumulatedEntity.confirmationCallback());
            return delegateWriteCallback.write(bb, accumulatedEntity.encodedEntity(), publishingId);
          }

          @Override
          public int fragmentLength(Object entity) {
            return delegateWriteCallback.fragmentLength(
                ((MessageAccumulator.AccumulatedEntity) entity).encodedEntity());
          }
        };
    if (!batchPublishingDelay.isNegative() && !batchPublishingDelay.isZero()) {
      AtomicReference<Runnable> taskReference = new AtomicReference<>();
      Runnable task =
          () -> {
            if (canSend()) {
              synchronized (this) {
                publishBatch();
              }
            }
            if (status != Status.CLOSED) {
              environment
                  .scheduledExecutorService()
                  .schedule(
                      taskReference.get(), batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
            }
          };
      taskReference.set(task);
      environment
          .scheduledExecutorService()
          .schedule(task, batchPublishingDelay.toMillis(), TimeUnit.MILLISECONDS);
    }
    this.batchSize = batchSize;
    this.status = Status.RUNNING;
  }

  void confirm(long publishingId) {
    ConfirmationCallback confirmationCallback = this.unconfirmedMessages.remove(publishingId);
    if (confirmationCallback != null) {
      int confirmedCount = confirmationCallback.handle(true, Constants.RESPONSE_CODE_OK);
      this.unconfirmedMessagesSemaphore.release(confirmedCount);
    } else {
      this.unconfirmedMessagesSemaphore.release();
    }
  }

  void error(long publishingId, short errorCode) {
    ConfirmationCallback confirmationCallback = unconfirmedMessages.remove(publishingId);
    if (confirmationCallback != null) {
      int nackedCount = confirmationCallback.handle(false, errorCode);
      this.unconfirmedMessagesSemaphore.release(nackedCount);
    } else {
      unconfirmedMessagesSemaphore.release();
    }
  }

  @Override
  public MessageBuilder messageBuilder() {
    return client.messageBuilder();
  }

  @Override
  public void send(Message message, ConfirmationHandler confirmationHandler) {
    try {
      if (canSend()) {
        if (unconfirmedMessagesSemaphore.tryAcquire(10, TimeUnit.SECONDS)) {
          if (canSend()) {
            if (accumulator.add(message, confirmationHandler)) {
              synchronized (this) {
                publishBatch();
              }
            }
          } else {
            failPublishing(message, confirmationHandler);
          }
        } else {
          confirmationHandler.handle(
              new ConfirmationStatus(message, false, CODE_MESSAGE_ENQUEUEING_FAILED));
        }
      } else {
        failPublishing(message, confirmationHandler);
      }
    } catch (InterruptedException e) {
      throw new StreamException("Interrupted while waiting to accumulate outbound message", e);
    }
  }

  private void failPublishing(Message message, ConfirmationHandler confirmationHandler) {
    if (this.status == Status.NOT_AVAILABLE) {
      confirmationHandler.handle(
          new ConfirmationStatus(message, false, CODE_PRODUCER_NOT_AVAILABLE));
    } else if (this.status == Status.CLOSED) {
      confirmationHandler.handle(new ConfirmationStatus(message, false, CODE_PRODUCER_CLOSED));
    } else {
      throw new IllegalStateException("Cannot publish while status is " + this.status);
    }
  }

  private boolean canSend() {
    return this.status == Status.RUNNING;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      Response response = this.client.deletePublisher(this.publisherId);
      if (!response.isOk()) {
        LOGGER.info(
            "Could not delete publisher {} on producer closing: {}",
            this.publisherId,
            formatConstant(response.getResponseCode()));
      }
      this.environment.removeProducer(this);
      closeFromEnvironment();
    }
  }

  void closeFromEnvironment() {
    this.closingCallback.run();
    this.closed.set(true);
    this.status = Status.CLOSED;
  }

  void closeAfterStreamDeletion() {
    if (closed.compareAndSet(false, true)) {
      this.environment.removeProducer(this);
      this.status = Status.CLOSED;
    }
  }

  private void publishBatch() {
    if (canSend() && !accumulator.isEmpty()) {
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
      client.publishInternal(
          this.publisherId, messages, this.writeCallback, this.publishSequenceSupplier);
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void unavailable() {
    this.status = Status.NOT_AVAILABLE;
    synchronized (this) {
      this.unconfirmedMessages
          .keySet()
          .forEach(publishingId -> error(publishingId, CODE_PRODUCER_NOT_AVAILABLE));
      if (!accumulator.isEmpty()) {
        MessageAccumulator.AccumulatedEntity accumulatedEntity;
        while ((accumulatedEntity = accumulator.get()) != null) {
          accumulatedEntity.confirmationCallback().handle(false, CODE_PRODUCER_NOT_AVAILABLE);
        }
      }
    }
  }

  void running() {
    synchronized (this) {
      if (unconfirmedMessagesSemaphore.availablePermits() != maxUnconfirmedMessages) {
        unconfirmedMessagesSemaphore.release(
            maxUnconfirmedMessages - unconfirmedMessagesSemaphore.availablePermits());
      }
    }
    this.status = Status.RUNNING;
  }

  synchronized void setClient(Client client) {
    this.client = client;
  }

  synchronized void setPublisherId(byte publisherId) {
    this.publisherId = publisherId;
  }

  Status status() {
    return this.status;
  }

  enum Status {
    RUNNING,
    NOT_AVAILABLE,
    CLOSED
  }

  interface ConfirmationCallback {

    int handle(boolean confirmed, short code);
  }
}
