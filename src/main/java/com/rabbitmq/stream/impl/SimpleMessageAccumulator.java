// Copyright (c) 2020-2023 VMware, Inc. or its affiliates.  All rights reserved.
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.ToLongFunction;

class SimpleMessageAccumulator implements MessageAccumulator {

  private static final Function<Message, String> NULL_FILTER_VALUE_EXTRACTOR = m -> null;

  protected final BlockingQueue<AccumulatedEntity> messages;
  protected final Clock clock;
  private final int capacity;
  protected final Codec codec;
  private final int maxFrameSize;
  private final ToLongFunction<Message> publishSequenceFunction;
  private final Function<Message, String> filterValueExtractor;
  final String stream;
  final ObservationCollector<Object> observationCollector;

  @SuppressWarnings("unchecked")
  SimpleMessageAccumulator(
      int capacity,
      Codec codec,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Function<Message, String> filterValueExtractor,
      Clock clock,
      String stream,
      ObservationCollector<?> observationCollector) {
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
  }

  public boolean add(Message message, ConfirmationHandler confirmationHandler) {
    Object observationContext = this.observationCollector.prePublish(this.stream, message);
    Codec.EncodedMessage encodedMessage = this.codec.encode(message);
    Client.checkMessageFitsInFrame(this.maxFrameSize, encodedMessage);
    long publishingId = this.publishSequenceFunction.applyAsLong(message);
    try {
      boolean offered =
          messages.offer(
              new SimpleAccumulatedEntity(
                  clock.time(),
                  publishingId,
                  this.filterValueExtractor.apply(message),
                  encodedMessage,
                  new SimpleConfirmationCallback(message, confirmationHandler),
                  observationContext),
              60,
              TimeUnit.SECONDS);
      if (!offered) {
        throw new StreamException("Could not accumulate outbound message");
      }
    } catch (InterruptedException e) {
      throw new StreamException("Error while accumulating outbound message", e);
    }
    return this.messages.size() == this.capacity;
  }

  @Override
  public AccumulatedEntity get() {
    AccumulatedEntity entity = this.messages.poll();
    if (entity != null) {
      this.observationCollector.published(
          entity.observationContext(), entity.confirmationCallback().message());
    }
    return entity;
  }

  @Override
  public boolean isEmpty() {
    return messages.isEmpty();
  }

  @Override
  public int size() {
    return messages.size();
  }

  private static final class SimpleAccumulatedEntity implements AccumulatedEntity {

    private final long time;
    private final long publishingId;
    private final String filterValue;
    private final Codec.EncodedMessage encodedMessage;
    private final StreamProducer.ConfirmationCallback confirmationCallback;
    private final Object observationContext;

    private SimpleAccumulatedEntity(
        long time,
        long publishingId,
        String filterValue,
        Codec.EncodedMessage encodedMessage,
        StreamProducer.ConfirmationCallback confirmationCallback,
        Object observationContext) {
      this.time = time;
      this.publishingId = publishingId;
      this.encodedMessage = encodedMessage;
      this.filterValue = filterValue;
      this.confirmationCallback = confirmationCallback;
      this.observationContext = observationContext;
    }

    @Override
    public long publishingId() {
      return publishingId;
    }

    @Override
    public String filterValue() {
      return filterValue;
    }

    @Override
    public Object encodedEntity() {
      return encodedMessage;
    }

    @Override
    public long time() {
      return time;
    }

    @Override
    public StreamProducer.ConfirmationCallback confirmationCallback() {
      return confirmationCallback;
    }

    @Override
    public Object observationContext() {
      return this.observationContext;
    }
  }

  private static final class SimpleConfirmationCallback
      implements StreamProducer.ConfirmationCallback {

    private final Message message;
    private final ConfirmationHandler confirmationHandler;

    private SimpleConfirmationCallback(Message message, ConfirmationHandler confirmationHandler) {
      this.message = message;
      this.confirmationHandler = confirmationHandler;
    }

    @Override
    public int handle(boolean confirmed, short code) {
      confirmationHandler.handle(new ConfirmationStatus(message, confirmed, code));
      return 1;
    }

    @Override
    public Message message() {
      return this.message;
    }
  }
}
