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

import com.rabbitmq.stream.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.ToLongFunction;

class SimpleMessageAccumulator implements MessageAccumulator {

  private static final Function<Message, String> NULL_FILTER_VALUE_EXTRACTOR = m -> null;

  protected final BlockingQueue<ProducerUtils.AccumulatedEntity> messages;
  protected final Clock clock;
  private final int capacity;
  protected final Codec codec;
  private final int maxFrameSize;
  private final ToLongFunction<Message> publishSequenceFunction;
  private final Function<Message, String> filterValueExtractor;
  final String stream;
  final ObservationCollector<Object> observationCollector;
  private final StreamProducer producer;

  @SuppressWarnings("unchecked")
  SimpleMessageAccumulator(
      int capacity,
      Codec codec,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Function<Message, String> filterValueExtractor,
      Clock clock,
      String stream,
      ObservationCollector<?> observationCollector,
      StreamProducer producer) {
    this.capacity = capacity;
    this.messages = new LinkedBlockingQueue<>(capacity);
    this.codec = codec;
    this.maxFrameSize = maxFrameSize;
    this.publishSequenceFunction = publishSequenceFunction;
    this.filterValueExtractor =
        filterValueExtractor == null ? NULL_FILTER_VALUE_EXTRACTOR : filterValueExtractor;
    this.clock = clock;
    this.stream = stream;
    this.observationCollector = (ObservationCollector<Object>) observationCollector;
    this.producer = producer;
  }

  public void add(Message message, ConfirmationHandler confirmationHandler) {
    Object observationContext = this.observationCollector.prePublish(this.stream, message);
    Codec.EncodedMessage encodedMessage = this.codec.encode(message);
    Client.checkMessageFitsInFrame(this.maxFrameSize, encodedMessage);
    long publishingId = this.publishSequenceFunction.applyAsLong(message);
    try {
      boolean offered =
          messages.offer(
              new ProducerUtils.SimpleAccumulatedEntity(
                  clock.time(),
                  publishingId,
                  this.filterValueExtractor.apply(message),
                  encodedMessage,
                  new ProducerUtils.SimpleConfirmationCallback(message, confirmationHandler),
                  observationContext),
              60,
              TimeUnit.SECONDS);
      if (!offered) {
        throw new StreamException("Could not accumulate outbound message");
      }
    } catch (InterruptedException e) {
      throw new StreamException("Error while accumulating outbound message", e);
    }
    if (this.messages.size() == this.capacity) {
      synchronized (this.producer) {
        publishBatch(true);
      }
    }
  }

  ProducerUtils.AccumulatedEntity get() {
    ProducerUtils.AccumulatedEntity entity = this.messages.poll();
    if (entity != null) {
      this.observationCollector.published(
          entity.observationContext(), entity.confirmationCallback().message());
    }
    return entity;
  }

  @Override
  public int size() {
    return messages.size();
  }

  @Override
  public void flush(boolean force) {
    boolean stateCheck = !force;
    synchronized (this.producer) {
      publishBatch(stateCheck);
    }
    //    System.out.println(sent.get());
  }

  AtomicInteger sent = new AtomicInteger();

  private void publishBatch(boolean stateCheck) {
    if ((!stateCheck || this.producer.canSend()) && !this.messages.isEmpty()) {
      List<Object> entities = new ArrayList<>(this.capacity);
      int batchCount = 0;
      while (batchCount != this.capacity) {
        ProducerUtils.AccumulatedEntity entity = this.get();
        if (entity == null) {
          break;
        }
        entities.add(entity);
        batchCount++;
      }
      this.sent.addAndGet(entities.size());
      producer.publishInternal(entities);
    }
  }
}
