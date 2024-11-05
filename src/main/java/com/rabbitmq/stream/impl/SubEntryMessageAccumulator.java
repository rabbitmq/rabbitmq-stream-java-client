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

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Codec.EncodedMessage;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.ObservationCollector;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.impl.Client.EncodedMessageBatch;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

class SubEntryMessageAccumulator extends SimpleMessageAccumulator {

  private final int subEntrySize;
  private final CompressionCodec compressionCodec;
  private final ByteBufAllocator byteBufAllocator;
  private final byte compression;

  public SubEntryMessageAccumulator(
      int subEntrySize,
      int batchSize,
      CompressionCodec compressionCodec,
      Codec codec,
      ByteBufAllocator byteBufAllocator,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Clock clock,
      String stream,
      ObservationCollector observationCollector) {
    super(
        subEntrySize * batchSize,
        codec,
        maxFrameSize,
        publishSequenceFunction,
        null,
        clock,
        stream,
        observationCollector);
    this.subEntrySize = subEntrySize;
    this.compressionCodec = compressionCodec;
    this.compression = compressionCodec == null ? Compression.NONE.code() : compressionCodec.code();
    this.byteBufAllocator = byteBufAllocator;
  }

  private Batch createBatch() {
    return new Batch(
        EncodedMessageBatch.create(
            byteBufAllocator, compression, compressionCodec, this.subEntrySize),
        new CompositeConfirmationCallback(new ArrayList<>(this.subEntrySize)));
  }

  @Override
  public AccumulatedEntity get() {
    if (this.messages.isEmpty()) {
      return null;
    }
    int count = 0;
    Batch batch = createBatch();
    AccumulatedEntity lastMessageInBatch = null;
    while (count != this.subEntrySize) {
      AccumulatedEntity message = messages.poll();
      if (message == null) {
        break;
      }
      this.observationCollector.published(
          message.observationContext(), message.confirmationCallback().message());
      lastMessageInBatch = message;
      batch.add((EncodedMessage) message.encodedEntity(), message.confirmationCallback());
      count++;
    }
    if (batch.isEmpty()) {
      return null;
    } else {
      batch.time = lastMessageInBatch.time();
      batch.publishingId = lastMessageInBatch.publishingId();
      batch.encodedMessageBatch.close();
      return batch;
    }
  }

  static class Batch implements AccumulatedEntity {

    final EncodedMessageBatch encodedMessageBatch;
    private final CompositeConfirmationCallback confirmationCallback;
    volatile long publishingId;
    volatile long time;

    Batch(
        EncodedMessageBatch encodedMessageBatch,
        CompositeConfirmationCallback confirmationCallback) {
      this.encodedMessageBatch = encodedMessageBatch;
      this.confirmationCallback = confirmationCallback;
    }

    void add(
        Codec.EncodedMessage encodedMessage,
        StreamProducer.ConfirmationCallback confirmationCallback) {
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
    public StreamProducer.ConfirmationCallback confirmationCallback() {
      return confirmationCallback;
    }

    @Override
    public Object observationContext() {
      throw new UnsupportedOperationException(
          "batch entity does not contain only one observation context");
    }
  }

  static class CompositeConfirmationCallback implements StreamProducer.ConfirmationCallback {

    private final List<StreamProducer.ConfirmationCallback> callbacks;

    CompositeConfirmationCallback(List<StreamProducer.ConfirmationCallback> callbacks) {
      this.callbacks = callbacks;
    }

    private void add(StreamProducer.ConfirmationCallback confirmationCallback) {
      this.callbacks.add(confirmationCallback);
    }

    @Override
    public int handle(boolean confirmed, short code) {
      for (StreamProducer.ConfirmationCallback callback : callbacks) {
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
}
