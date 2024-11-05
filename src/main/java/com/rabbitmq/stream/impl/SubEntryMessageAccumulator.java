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
import java.util.function.ToLongFunction;

final class SubEntryMessageAccumulator extends SimpleMessageAccumulator {

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
      ObservationCollector<?> observationCollector,
      StreamProducer producer) {
    super(
        subEntrySize * batchSize,
        codec,
        maxFrameSize,
        publishSequenceFunction,
        null,
        clock,
        stream,
        observationCollector,
        producer);
    this.subEntrySize = subEntrySize;
    this.compressionCodec = compressionCodec;
    this.compression = compressionCodec == null ? Compression.NONE.code() : compressionCodec.code();
    this.byteBufAllocator = byteBufAllocator;
  }

  private ProducerUtils.Batch createBatch() {
    return new ProducerUtils.Batch(
        EncodedMessageBatch.create(
            byteBufAllocator, compression, compressionCodec, this.subEntrySize),
        new ProducerUtils.CompositeConfirmationCallback(new ArrayList<>(this.subEntrySize)));
  }

  @Override
  protected ProducerUtils.AccumulatedEntity get() {
    if (this.messages.isEmpty()) {
      return null;
    }
    int count = 0;
    ProducerUtils.Batch batch = createBatch();
    ProducerUtils.AccumulatedEntity lastMessageInBatch = null;
    while (count != this.subEntrySize) {
      ProducerUtils.AccumulatedEntity message = messages.poll();
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
}
