// Copyright (c) 2020-2026 Broadcom. All Rights Reserved.
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
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.ObservationCollector;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import io.netty.buffer.ByteBufAllocator;
import java.util.Collections;
import java.util.function.ToLongFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class SubEntryMessageAccumulator extends SimpleMessageAccumulator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubEntryMessageAccumulator.class);

  private final int subEntrySize;
  private final CompressionCodec compressionCodec;
  private final ByteBufAllocator byteBufAllocator;
  private final byte compressionCode;

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
        subEntrySize,
        compressionCodec,
        publishSequenceFunction,
        null,
        clock,
        stream,
        observationCollector,
        producer);
    this.subEntrySize = subEntrySize;
    this.compressionCodec = compressionCodec;
    this.compressionCode =
        compressionCodec == null ? Compression.NONE.code() : compressionCodec.code();
    this.byteBufAllocator = byteBufAllocator;
  }

  private ProducerUtils.Batch createBatch() {
    return this.helper.batch(
        this.byteBufAllocator, this.compressionCode, this.compressionCodec, this.subEntrySize);
  }

  @Override
  protected ProducerUtils.AccumulatedEntity get() {
    while (!this.messages.isEmpty()) {
      ProducerUtils.Batch batch = this.createBatch();
      ProducerUtils.AccumulatedEntity lastMessageInBatch = null;
      while (true) {
        // only the publishing thread polls, under the producer lock
        ProducerUtils.AccumulatedEntity message = messages.peek();
        if (message == null
            || (!batch.isEmpty()
                && !this.helper.canAddToBatch(batch, (EncodedMessage) message.encodedEntity()))) {
          break;
        }
        messages.poll();
        this.observationCollector.published(
            message.observationContext(), message.confirmationCallback().message());
        lastMessageInBatch = message;
        batch.add((EncodedMessage) message.encodedEntity(), message.confirmationCallback());
      }
      if (batch.isEmpty()) {
        return null;
      }
      batch.time = lastMessageInBatch.time();
      batch.publishingId = lastMessageInBatch.publishingId();
      try {
        batch.encodedMessageBatch.close();
        return batch;
      } catch (Exception e) {
        LOGGER.warn("Error while closing message sub-entry batch: {}", e.getMessage());
        this.producer.errorBeforePublish(
            Collections.singletonList(batch), Constants.CODE_MESSAGE_ENQUEUEING_FAILED);
      }
    }
    return null;
  }
}
