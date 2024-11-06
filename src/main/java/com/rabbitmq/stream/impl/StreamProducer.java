// Copyright (c) 2020-2023 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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
import static com.rabbitmq.stream.impl.Utils.namedRunnable;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.impl.Client.Response;
import com.rabbitmq.stream.impl.ProducerUtils.AccumulatedEntity;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBuf;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamProducer implements Producer {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamProducer.class);
  private static final ConfirmationHandler NO_OP_CONFIRMATION_HANDLER = confirmationStatus -> {};
  private final long id;
  private final MessageAccumulator accumulator;
  private final ToLongFunction<Message> accumulatorPublishSequenceFunction;
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
  private final ToLongFunction<Object> publishSequenceFunction =
      entity -> ((AccumulatedEntity) entity).publishingId();
  private final long enqueueTimeoutMs;
  private final boolean blockOnMaxUnconfirmed;
  private final boolean retryOnRecovery;
  private volatile Client client;
  private volatile byte publisherId;
  private volatile Status status;
  private volatile ScheduledFuture<?> confirmTimeoutFuture;
  private final short publishVersion;

  @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
  StreamProducer(
      String name,
      String stream,
      int subEntrySize,
      int batchSize,
      boolean dynamicBatch,
      Compression compression,
      Duration batchPublishingDelay,
      int maxUnconfirmedMessages,
      Duration confirmTimeout,
      Duration enqueueTimeout,
      boolean retryOnRecovery,
      Function<Message, String> filterValueExtractor,
      StreamEnvironment environment) {
    if (filterValueExtractor != null && !environment.filteringSupported()) {
      throw new IllegalArgumentException(
          "Filtering is not supported by the broker "
              + "(requires RabbitMQ 3.13+ and stream_filtering feature flag activated");
    }
    this.id = ID_SEQUENCE.getAndIncrement();
    this.environment = environment;
    this.name = name;
    this.stream = stream;
    this.enqueueTimeoutMs = enqueueTimeout.toMillis();
    this.retryOnRecovery = retryOnRecovery;
    this.blockOnMaxUnconfirmed = enqueueTimeout.isZero();
    this.closingCallback = environment.registerProducer(this, name, this.stream);
    final Client.OutboundEntityWriteCallback delegateWriteCallback;
    AtomicLong publishingSequence = new AtomicLong(computeFirstValueOfPublishingSequence());
    this.accumulatorPublishSequenceFunction =
        msg -> {
          if (msg.hasPublishingId()) {
            return msg.getPublishingId();
          } else {
            return publishingSequence.getAndIncrement();
          }
        };

    if (subEntrySize <= 1) {
      if (filterValueExtractor == null) {
        delegateWriteCallback = Client.OUTBOUND_MESSAGE_WRITE_CALLBACK;
      } else {
        delegateWriteCallback = OUTBOUND_MSG_FILTER_VALUE_WRITE_CALLBACK;
      }
    } else {
      delegateWriteCallback = Client.OUTBOUND_MESSAGE_BATCH_WRITE_CALLBACK;
    }

    this.maxUnconfirmedMessages = maxUnconfirmedMessages;
    this.unconfirmedMessagesSemaphore = new Semaphore(maxUnconfirmedMessages, true);
    this.unconfirmedMessages = new ConcurrentHashMap<>(this.maxUnconfirmedMessages, 0.75f, 2);

    if (filterValueExtractor == null) {
      this.publishVersion = VERSION_1;
      this.writeCallback =
          new Client.OutboundEntityWriteCallback() {
            @Override
            public int write(ByteBuf bb, Object entity, long publishingId) {
              AccumulatedEntity accumulatedEntity = (AccumulatedEntity) entity;
              unconfirmedMessages.put(publishingId, accumulatedEntity);
              return delegateWriteCallback.write(
                  bb, accumulatedEntity.encodedEntity(), publishingId);
            }

            @Override
            public int fragmentLength(Object entity) {
              return delegateWriteCallback.fragmentLength(
                  ((AccumulatedEntity) entity).encodedEntity());
            }
          };
    } else {
      this.publishVersion = VERSION_2;
      this.writeCallback =
          new Client.OutboundEntityWriteCallback() {
            @Override
            public int write(ByteBuf bb, Object entity, long publishingId) {
              AccumulatedEntity accumulatedEntity = (AccumulatedEntity) entity;
              unconfirmedMessages.put(publishingId, accumulatedEntity);
              return delegateWriteCallback.write(bb, accumulatedEntity, publishingId);
            }

            @Override
            public int fragmentLength(Object entity) {
              return delegateWriteCallback.fragmentLength(entity);
            }
          };
    }

    CompressionCodec compressionCodec = null;
    if (compression != null) {
      compressionCodec = environment.compressionCodecFactory().get(compression);
    }
    this.accumulator =
        ProducerUtils.createMessageAccumulator(
            dynamicBatch,
            subEntrySize,
            batchSize,
            compressionCodec,
            environment.codec(),
            environment.byteBufAllocator(),
            client.maxFrameSize(),
            accumulatorPublishSequenceFunction,
            filterValueExtractor,
            environment.clock(),
            stream,
            environment.observationCollector(),
            this);

    boolean backgroundBatchPublishingTaskRequired =
        !dynamicBatch && batchPublishingDelay.toMillis() > 0;
    LOGGER.debug(
        "Background batch publishing task required? {}", backgroundBatchPublishingTaskRequired);
    if (backgroundBatchPublishingTaskRequired) {
      AtomicReference<Runnable> taskReference = new AtomicReference<>();
      Runnable task =
          () -> {
            if (canSend()) {
              this.accumulator.flush(false);
            }
            if (status != Status.CLOSED) {
              environment
                  .scheduledExecutorService()
                  .schedule(
                      namedRunnable(
                          taskReference.get(),
                          "Background batch publishing task for publisher %d on stream '%s'",
                          this.id,
                          this.stream),
                      batchPublishingDelay.toMillis(),
                      TimeUnit.MILLISECONDS);
            }
          };
      taskReference.set(task);
      environment
          .scheduledExecutorService()
          .schedule(
              namedRunnable(
                  task,
                  "Background batch publishing task for publisher %d on stream '%s'",
                  this.id,
                  this.stream),
              batchPublishingDelay.toMillis(),
              TimeUnit.MILLISECONDS);
    }
    this.batchSize = batchSize;
    this.codec = environment.codec();
    if (!confirmTimeout.isZero()) {
      AtomicReference<Runnable> taskReference = new AtomicReference<>();
      Runnable confirmTimeoutTask = confirmTimeoutTask(confirmTimeout);
      Runnable wrapperTask =
          () -> {
            try {
              confirmTimeoutTask.run();
            } catch (Exception e) {
              LOGGER.info("Error while executing confirm timeout check task: {}", e.getCause());
            }

            if (this.status != Status.CLOSED) {
              this.confirmTimeoutFuture =
                  this.environment
                      .scheduledExecutorService()
                      .schedule(
                          namedRunnable(
                              taskReference.get(),
                              "Background confirm timeout task for producer %d on stream %s",
                              this.id,
                              this.stream),
                          confirmTimeout.toMillis(),
                          TimeUnit.MILLISECONDS);
            }
          };
      taskReference.set(wrapperTask);
      this.confirmTimeoutFuture =
          this.environment
              .scheduledExecutorService()
              .schedule(
                  namedRunnable(
                      taskReference.get(),
                      "Background confirm timeout task for producer %d on stream %s",
                      this.id,
                      this.stream),
                  confirmTimeout.toMillis(),
                  TimeUnit.MILLISECONDS);
    }
    this.status = Status.RUNNING;
  }

  private Runnable confirmTimeoutTask(Duration confirmTimeout) {
    return () -> {
      long limit = this.environment.clock().time() - confirmTimeout.toNanos();
      SortedMap<Long, AccumulatedEntity> unconfirmedSnapshot =
          new TreeMap<>(this.unconfirmedMessages);
      int count = 0;
      for (Entry<Long, AccumulatedEntity> unconfirmedEntry : unconfirmedSnapshot.entrySet()) {
        if (unconfirmedEntry.getValue().time() < limit) {
          if (Thread.currentThread().isInterrupted()) {
            return;
          }
          error(unconfirmedEntry.getKey(), Constants.CODE_PUBLISH_CONFIRM_TIMEOUT);
          count++;
        } else {
          // everything else is after, we can stop
          break;
        }
      }
      if (count > 0) {
        LOGGER.debug(
            "{} outbound message(s) had reached the confirm timeout (limit {}) "
                + "for producer {} on stream '{}', application notified with callback",
            count,
            limit,
            this.id,
            this.stream);
      }
    };
  }

  private long computeFirstValueOfPublishingSequence() {
    if (name == null || name.isEmpty()) {
      return 0;
    } else {
      long lastPublishingId = this.client.queryPublisherSequence(this.name, this.stream);
      if (lastPublishingId == 0) {
        return 0;
      } else {
        return lastPublishingId + 1;
      }
    }
  }

  // visible for testing
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

  // for testing
  int unconfirmedCount() {
    return this.unconfirmedMessages.size();
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
    checkNotClosed();
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
    if (confirmationHandler == null) {
      confirmationHandler = NO_OP_CONFIRMATION_HANDLER;
    }
    try {
      if (canSend()) {
        if (this.blockOnMaxUnconfirmed) {
          unconfirmedMessagesSemaphore.acquire();
          doSend(message, confirmationHandler);
        } else {
          if (unconfirmedMessagesSemaphore.tryAcquire(
              this.enqueueTimeoutMs, TimeUnit.MILLISECONDS)) {
            doSend(message, confirmationHandler);
          } else {
            confirmationHandler.handle(
                new ConfirmationStatus(message, false, CODE_MESSAGE_ENQUEUEING_FAILED));
          }
        }
      } else {
        failPublishing(message, confirmationHandler);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StreamException("Interrupted while waiting to accumulate outbound message", e);
    }
  }

  private void doSend(Message message, ConfirmationHandler confirmationHandler) {
    if (canSend()) {
      this.accumulator.add(message, confirmationHandler);
    } else {
      failPublishing(message, confirmationHandler);
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

  boolean canSend() {
    return this.status == Status.RUNNING;
  }

  @Override
  public void close() {
    if (this.closed.compareAndSet(false, true)) {
      if (this.status == Status.RUNNING && this.client != null) {
        LOGGER.debug("Deleting producer {}", this.publisherId);
        Response response = this.client.deletePublisher(this.publisherId);
        if (!response.isOk()) {
          LOGGER.info(
              "Could not delete publisher {} on producer closing: {}",
              this.publisherId,
              formatConstant(response.getResponseCode()));
        }
      } else {
        LOGGER.debug(
            "No need to delete producer {}, it is currently unavailable", this.publisherId);
      }
      this.environment.removeProducer(this);
      closeFromEnvironment();
    }
  }

  void closeFromEnvironment() {
    this.closingCallback.run();
    cancelConfirmTimeoutTask();
    this.closed.set(true);
    this.status = Status.CLOSED;
    LOGGER.debug("Closed publisher {} successfully", this.publisherId);
  }

  void closeAfterStreamDeletion(short code) {
    if (closed.compareAndSet(false, true)) {
      if (!this.unconfirmedMessages.isEmpty()) {
        Iterator<Entry<Long, AccumulatedEntity>> iterator =
            unconfirmedMessages.entrySet().iterator();
        while (iterator.hasNext()) {
          AccumulatedEntity entry = iterator.next().getValue();
          int confirmedCount = entry.confirmationCallback().handle(false, code);
          this.unconfirmedMessagesSemaphore.release(confirmedCount);
          iterator.remove();
        }
      }
      cancelConfirmTimeoutTask();
      this.environment.removeProducer(this);
      this.status = Status.CLOSED;
    }
  }

  private void cancelConfirmTimeoutTask() {
    if (this.confirmTimeoutFuture != null) {
      this.confirmTimeoutFuture.cancel(true);
    }
  }

  void publishInternal(List<Object> messages) {
    client.publishInternal(
        this.publishVersion,
        this.publisherId,
        messages,
        this.writeCallback,
        this.publishSequenceFunction);
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
          "Recovering producer with {} unconfirmed message(s) and {} accumulated message(s)",
          this.unconfirmedMessages.size(),
          this.accumulator.size());
      if (this.retryOnRecovery) {
        LOGGER.debug("Re-publishing {} unconfirmed message(s)", this.unconfirmedMessages.size());
        if (!this.unconfirmedMessages.isEmpty()) {
          Map<Long, AccumulatedEntity> messagesToResend = new TreeMap<>(this.unconfirmedMessages);
          this.unconfirmedMessages.clear();
          Iterator<Entry<Long, AccumulatedEntity>> resendIterator =
              messagesToResend.entrySet().iterator();
          while (resendIterator.hasNext()) {
            List<Object> messages = new ArrayList<>(this.batchSize);
            int batchCount = 0;
            while (batchCount != this.batchSize) {
              Object accMessage =
                  resendIterator.hasNext() ? resendIterator.next().getValue() : null;
              if (accMessage == null) {
                break;
              }
              messages.add(accMessage);
              batchCount++;
            }
            client.publishInternal(
                this.publishVersion,
                this.publisherId,
                messages,
                this.writeCallback,
                this.publishSequenceFunction);
          }
        }
      } else {
        LOGGER.debug(
            "Skipping republishing of {} unconfirmed messages", this.unconfirmedMessages.size());
        Map<Long, AccumulatedEntity> messagesToFail = new TreeMap<>(this.unconfirmedMessages);
        this.unconfirmedMessages.clear();
        for (AccumulatedEntity accumulatedEntity : messagesToFail.values()) {
          try {
            int permits =
                accumulatedEntity
                    .confirmationCallback()
                    .handle(false, CODE_PUBLISH_CONFIRM_TIMEOUT);
            this.unconfirmedMessagesSemaphore.release(permits);
          } catch (Exception e) {
            LOGGER.debug("Error while nack-ing outbound message: {}", e.getMessage());
            this.unconfirmedMessagesSemaphore.release(1);
          }
        }
      }
      this.accumulator.flush(true);
      int toRelease = maxUnconfirmedMessages - unconfirmedMessagesSemaphore.availablePermits();
      if (toRelease > 0) {
        unconfirmedMessagesSemaphore.release(toRelease);
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StreamProducer that = (StreamProducer) o;
    return id == that.id && stream.equals(that.stream);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, stream);
  }

  @Override
  public String toString() {
    Client client = this.client;
    return "{ "
        + "\"id\" : "
        + id
        + ","
        + "\"stream\" : \""
        + stream
        + "\","
        + "\"publishing_client\" : "
        + (client == null ? "null" : ("\"" + client.connectionName() + "\""))
        + "}";
  }

  private void checkNotClosed() {
    if (this.closed.get()) {
      throw new IllegalStateException("This producer instance has been closed");
    }
  }

  private static final Client.OutboundEntityWriteCallback OUTBOUND_MSG_FILTER_VALUE_WRITE_CALLBACK =
      new OutboundMessageFilterValueWriterCallback();

  private static final class OutboundMessageFilterValueWriterCallback
      implements Client.OutboundEntityWriteCallback {

    @Override
    public int write(ByteBuf bb, Object entity, long publishingId) {
      AccumulatedEntity accumulatedEntity = (AccumulatedEntity) entity;
      String filterValue = accumulatedEntity.filterValue();
      if (filterValue == null) {
        bb.writeShort(-1);
      } else {
        bb.writeShort(filterValue.length());
        bb.writeBytes(filterValue.getBytes(StandardCharsets.UTF_8));
      }
      Codec.EncodedMessage messageToPublish =
          (Codec.EncodedMessage) accumulatedEntity.encodedEntity();
      bb.writeInt(messageToPublish.getSize());
      bb.writeBytes(messageToPublish.getData(), 0, messageToPublish.getSize());
      return 1;
    }

    @Override
    public int fragmentLength(Object entity) {
      AccumulatedEntity accumulatedEntity = (AccumulatedEntity) entity;
      Codec.EncodedMessage message = (Codec.EncodedMessage) accumulatedEntity.encodedEntity();
      String filterValue = accumulatedEntity.filterValue();
      if (filterValue == null) {
        return 8 + 2 + 4 + message.getSize();
      } else {
        return 8 + 2 + accumulatedEntity.filterValue().length() + 4 + message.getSize();
      }
    }
  }
}
