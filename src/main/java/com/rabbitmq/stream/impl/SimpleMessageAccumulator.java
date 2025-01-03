// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
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
import com.rabbitmq.stream.impl.ProducerUtils.AccumulatedEntity;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;

class SimpleMessageAccumulator implements MessageAccumulator {

  protected final BlockingQueue<AccumulatedEntity> messages;
  private final int capacity;
  final ObservationCollector<Object> observationCollector;
  private final StreamProducer producer;
  final ProducerUtils.MessageAccumulatorHelper helper;

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
    this.helper =
        new ProducerUtils.MessageAccumulatorHelper(
            codec,
            maxFrameSize,
            publishSequenceFunction,
            filterValueExtractor,
            clock,
            stream,
            observationCollector);
    this.capacity = capacity;
    this.messages = new LinkedBlockingQueue<>(this.capacity);
    this.observationCollector = (ObservationCollector<Object>) observationCollector;
    this.producer = producer;
  }

  public void add(Message message, ConfirmationHandler confirmationHandler) {
    AccumulatedEntity entity = this.helper.entity(message, confirmationHandler);
    try {
      boolean offered = messages.offer(entity, 60, TimeUnit.SECONDS);
      if (!offered) {
        throw new StreamException("Could not accumulate outbound message");
      }
    } catch (InterruptedException e) {
      throw new StreamException("Error while accumulating outbound message", e);
    }
    if (this.messages.size() == this.capacity) {
      publishBatch(true);
    }
  }

  AccumulatedEntity get() {
    AccumulatedEntity entity = this.messages.poll();
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
    publishBatch(stateCheck);
  }

  private void publishBatch(boolean stateCheck) {
    this.producer.lock();
    try {
      if ((!stateCheck || this.producer.canSend()) && !this.messages.isEmpty()) {
        List<Object> entities = new ArrayList<>(this.capacity);
        int batchCount = 0;
        while (batchCount != this.capacity) {
          AccumulatedEntity entity = this.get();
          if (entity == null) {
            break;
          }
          entities.add(entity);
          batchCount++;
        }
        producer.publishInternal(entities);
      }
    } finally {
      this.producer.unlock();
    }
  }

  @Override
  public void close() {}
}
