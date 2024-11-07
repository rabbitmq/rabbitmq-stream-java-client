// Copyright (c) 2024 Broadcom. All Rights Reserved.
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
import com.rabbitmq.stream.compression.CompressionCodec;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

final class ProducerUtils {

  private ProducerUtils() {}

  static MessageAccumulator createMessageAccumulator(
      boolean dynamicBatch,
      int subEntrySize,
      int batchSize,
      CompressionCodec compressionCodec,
      Codec codec,
      ByteBufAllocator byteBufAllocator,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Function<Message, String> filterValueExtractor,
      Clock clock,
      String stream,
      ObservationCollector<?> observationCollector,
      StreamProducer producer) {
    if (dynamicBatch) {
      return new DynamicBatchMessageAccumulator(
          subEntrySize,
          batchSize,
          codec,
          maxFrameSize,
          publishSequenceFunction,
          filterValueExtractor,
          clock,
          stream,
          compressionCodec,
          byteBufAllocator,
          observationCollector,
          producer);
    } else {
      if (subEntrySize <= 1) {
        return new SimpleMessageAccumulator(
            batchSize,
            codec,
            maxFrameSize,
            publishSequenceFunction,
            filterValueExtractor,
            clock,
            stream,
            observationCollector,
            producer);
      } else {
        return new SubEntryMessageAccumulator(
            subEntrySize,
            batchSize,
            compressionCodec,
            codec,
            byteBufAllocator,
            maxFrameSize,
            publishSequenceFunction,
            clock,
            stream,
            observationCollector,
            producer);
      }
    }
  }

  interface ConfirmationCallback {

    int handle(boolean confirmed, short code);

    Message message();
  }

  interface AccumulatedEntity {

    long time();

    long publishingId();

    String filterValue();

    Object encodedEntity();

    ConfirmationCallback confirmationCallback();

    Object observationContext();
  }

  static final class SimpleConfirmationCallback implements ConfirmationCallback {

    private final Message message;
    private final ConfirmationHandler confirmationHandler;

    SimpleConfirmationCallback(Message message, ConfirmationHandler confirmationHandler) {
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

  static final class SimpleAccumulatedEntity implements AccumulatedEntity {

    private final long time;
    private final long publishingId;
    private final String filterValue;
    private final Codec.EncodedMessage encodedMessage;
    private final ConfirmationCallback confirmationCallback;
    private final Object observationContext;

    SimpleAccumulatedEntity(
        long time,
        long publishingId,
        String filterValue,
        Codec.EncodedMessage encodedMessage,
        ConfirmationCallback confirmationCallback,
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
    public ConfirmationCallback confirmationCallback() {
      return confirmationCallback;
    }

    @Override
    public Object observationContext() {
      return this.observationContext;
    }
  }

  static final class CompositeConfirmationCallback implements ConfirmationCallback {

    private final List<ConfirmationCallback> callbacks;

    CompositeConfirmationCallback(List<ConfirmationCallback> callbacks) {
      this.callbacks = callbacks;
    }

    private void add(ConfirmationCallback confirmationCallback) {
      this.callbacks.add(confirmationCallback);
    }

    @Override
    public int handle(boolean confirmed, short code) {
      for (ConfirmationCallback callback : callbacks) {
        callback.handle(confirmed, code);
      }
      return callbacks.size();
    }

    @Override
    public Message message() {
      throw new UnsupportedOperationException(
          "composite confirmation callback does not contain just one message");
    }
  }

  static final class Batch implements AccumulatedEntity {

    final Client.EncodedMessageBatch encodedMessageBatch;
    private final CompositeConfirmationCallback confirmationCallback;
    volatile long publishingId;
    volatile long time;

    Batch(
        Client.EncodedMessageBatch encodedMessageBatch,
        CompositeConfirmationCallback confirmationCallback) {
      this.encodedMessageBatch = encodedMessageBatch;
      this.confirmationCallback = confirmationCallback;
    }

    void add(Codec.EncodedMessage encodedMessage, ConfirmationCallback confirmationCallback) {
      this.encodedMessageBatch.add(encodedMessage);
      this.confirmationCallback.add(confirmationCallback);
    }

    boolean isEmpty() {
      return this.confirmationCallback.callbacks.isEmpty();
    }

    @Override
    public long publishingId() {
      return publishingId;
    }

    @Override
    public String filterValue() {
      return null;
    }

    @Override
    public Object encodedEntity() {
      return encodedMessageBatch;
    }

    @Override
    public long time() {
      return time;
    }

    @Override
    public ConfirmationCallback confirmationCallback() {
      return confirmationCallback;
    }

    @Override
    public Object observationContext() {
      throw new UnsupportedOperationException(
          "batch entity does not contain only one observation context");
    }
  }

  static final class MessageAccumulatorHelper {

    private static final Function<Message, String> NULL_FILTER_VALUE_EXTRACTOR = m -> null;

    private final ObservationCollector<Object> observationCollector;
    private final ToLongFunction<Message> publishSequenceFunction;
    private final String stream;
    private final Codec codec;
    private final int maxFrameSize;
    private final Clock clock;
    private final Function<Message, String> filterValueExtractor;

    @SuppressWarnings("unchecked")
    MessageAccumulatorHelper(
        Codec codec,
        int maxFrameSize,
        ToLongFunction<Message> publishSequenceFunction,
        Function<Message, String> filterValueExtractor,
        Clock clock,
        String stream,
        ObservationCollector<?> observationCollector) {
      this.publishSequenceFunction = publishSequenceFunction;
      this.codec = codec;
      this.clock = clock;
      this.maxFrameSize = maxFrameSize;
      this.filterValueExtractor =
          filterValueExtractor == null ? NULL_FILTER_VALUE_EXTRACTOR : filterValueExtractor;
      this.observationCollector = (ObservationCollector<Object>) observationCollector;
      this.stream = stream;
    }

    AccumulatedEntity entity(Message message, ConfirmationHandler confirmationHandler) {
      Object observationContext = this.observationCollector.prePublish(this.stream, message);
      Codec.EncodedMessage encodedMessage = this.codec.encode(message);
      Client.checkMessageFitsInFrame(this.maxFrameSize, encodedMessage);
      long publishingId = this.publishSequenceFunction.applyAsLong(message);
      return new ProducerUtils.SimpleAccumulatedEntity(
          this.clock.time(),
          publishingId,
          this.filterValueExtractor.apply(message),
          this.codec.encode(message),
          new ProducerUtils.SimpleConfirmationCallback(message, confirmationHandler),
          observationContext);
    }

    Batch batch(
        ByteBufAllocator bba,
        byte compressionCode,
        CompressionCodec compressionCodec,
        int subEntrySize) {
      return new ProducerUtils.Batch(
          Client.EncodedMessageBatch.create(bba, compressionCode, compressionCodec, subEntrySize),
          new ProducerUtils.CompositeConfirmationCallback(new ArrayList<>(subEntrySize)));
    }
  }
}
