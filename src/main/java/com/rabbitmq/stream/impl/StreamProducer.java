// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
import com.rabbitmq.stream.impl.MessageAccumulator.AccumulatedEntity;
import io.netty.buffer.ByteBuf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProducer implements Producer {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProducer.class);

  private final MessageAccumulator accumulator;
  // FIXME investigate a more optimized data structure to handle pending messages
  private final ConcurrentMap<Long, AccumulatedEntity> unconfirmedMessages;
  private final int batchSize;
  private final String name;
  private final String stream;
  private final Client.OutboundEntityWriteCallback writeCallback;
  private final Semaphore unconfirmedMessagesSemaphore;
  private final Runnable closingCallback;
  private final StreamEnvironment environment;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final int maxUnconfirmedMessages;
  private final Codec codec;
  private volatile Client client;
  private volatile byte publisherId;
  private volatile Status status;
  private final ToLongFunction<Object> publishSequenceFunction =
      entity -> ((AccumulatedEntity) entity).publishindId();

  StreamProducer(
      String name,
      String stream,
      int subEntrySize,
      int batchSize,
      Duration batchPublishingDelay,
      int maxUnconfirmedMessages,
      StreamEnvironment environment) {
    this.environment = environment;
    this.name = name;
    this.stream = stream;
    this.closingCallback = environment.registerProducer(this, name, this.stream);
    final Client.OutboundEntityWriteCallback delegateWriteCallback;
    AtomicLong publishingSequence = new AtomicLong(0);
    ToLongFunction<Message> accumulatorPublishSequenceFunction =
        msg -> {
          if (msg.hasPublishingId()) {
            return msg.getPublishingId();
          } else {
            return publishingSequence.getAndIncrement();
          }
        };
    if (subEntrySize <= 1) {
      this.accumulator =
          new SimpleMessageAccumulator(
              batchSize,
              environment.codec(),
              client.maxFrameSize(),
              accumulatorPublishSequenceFunction);
      delegateWriteCallback = Client.OUTBOUND_MESSAGE_WRITE_CALLBACK;
    } else {
      this.accumulator =
          new SubEntryMessageAccumulator(
              subEntrySize,
              batchSize,
              environment.codec(),
              client.maxFrameSize(),
              accumulatorPublishSequenceFunction);
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
            unconfirmedMessages.put(publishingId, accumulatedEntity);
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
              synchronized (StreamProducer.this) {
                publishBatch(true);
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
    this.codec = environment.codec();
    this.status = Status.RUNNING;
  }

  void confirm(long publishingId) {
    AccumulatedEntity accumulatedEntity = this.unconfirmedMessages.remove(publishingId);
    if (accumulatedEntity != null) {
      int confirmedCount =
          accumulatedEntity.confirmationCallback().handle(true, Constants.RESPONSE_CODE_OK);
      this.unconfirmedMessagesSemaphore.release(confirmedCount);
    } else {
      this.unconfirmedMessagesSemaphore.release();
    }
  }

  void error(long publishingId, short errorCode) {
    AccumulatedEntity accumulatedEntity = unconfirmedMessages.remove(publishingId);
    if (accumulatedEntity != null) {
      int nackedCount = accumulatedEntity.confirmationCallback().handle(false, errorCode);
      this.unconfirmedMessagesSemaphore.release(nackedCount);
    } else {
      unconfirmedMessagesSemaphore.release();
    }
  }

  @Override
  public MessageBuilder messageBuilder() {
    return codec.messageBuilder();
  }

  @Override
  public long getLastPublishingId() {
    if (this.name != null && !this.name.isEmpty()) {
      if (canSend()) {
        try {
          return this.client.queryPublisherSequence(this.name, this.stream);
        } catch (Exception e) {
          throw new IllegalStateException(
              "Error while trying to query last publishing ID for "
                  + "producer "
                  + this.name
                  + " on stream "
                  + stream);
        }
      } else {
        throw new IllegalStateException("The producer has no connection");
      }
    } else {
      throw new IllegalStateException("The producer has no name");
    }
  }

  @Override
  public void send(Message message, ConfirmationHandler confirmationHandler) {
    try {
      if (canSend()) {
        if (unconfirmedMessagesSemaphore.tryAcquire(10, TimeUnit.SECONDS)) {
          if (canSend()) {
            if (accumulator.add(message, confirmationHandler)) {
              synchronized (this) {
                publishBatch(true);
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
      confirmationHandler.handle(
          new ConfirmationStatus(message, false, CODE_PRODUCER_NOT_AVAILABLE));
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

  private void publishBatch(boolean stateCheck) {
    if ((!stateCheck || canSend()) && !accumulator.isEmpty()) {
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
          this.publisherId, messages, this.writeCallback, this.publishSequenceFunction);
    }
  }

  boolean isOpen() {
    return !this.closed.get();
  }

  void unavailable() {
    this.status = Status.NOT_AVAILABLE;
  }

  void running() {
    synchronized (this) {
      LOGGER.debug(
          "Re-publishing {} unconfirmed message(s) and {} accumulated message(s)",
          this.unconfirmedMessages.size(),
          this.accumulator.size());
      if (!this.unconfirmedMessages.isEmpty()) {
        Map<Long, AccumulatedEntity> messagesToResend = new TreeMap<>(this.unconfirmedMessages);
        this.unconfirmedMessages.clear();
        Iterator<Entry<Long, AccumulatedEntity>> resendIterator =
            messagesToResend.entrySet().iterator();
        while (resendIterator.hasNext()) {
          List<Object> messages = new ArrayList<>(this.batchSize);
          int batchCount = 0;
          while (batchCount != this.batchSize) {
            Object accMessage = resendIterator.hasNext() ? resendIterator.next().getValue() : null;
            if (accMessage == null) {
              break;
            }
            messages.add(accMessage);
            batchCount++;
          }
          client.publishInternal(
              this.publisherId, messages, this.writeCallback, this.publishSequenceFunction);
        }
      }
      publishBatch(false);
      if (unconfirmedMessagesSemaphore.availablePermits() != maxUnconfirmedMessages) {
        unconfirmedMessagesSemaphore.release(
            maxUnconfirmedMessages - unconfirmedMessagesSemaphore.availablePermits());
        if (!unconfirmedMessagesSemaphore.tryAcquire(this.unconfirmedMessages.size())) {
          LOGGER.debug(
              "Could not acquire {} permit(s) for message republishing",
              this.unconfirmedMessages.size());
        }
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
