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

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.ObservationCollector;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.compression.CompressionCodec;
import com.rabbitmq.stream.impl.ProducerUtils.AccumulatedEntity;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

final class DynamicBatchMessageAccumulator implements MessageAccumulator {

  private static final Function<Message, String> NULL_FILTER_VALUE_EXTRACTOR = m -> null;

  private final DynamicBatch<Object> dynamicBatch;
  private final ObservationCollector<Object> observationCollector;
  private final ToLongFunction<Message> publishSequenceFunction;
  private final String stream;
  private final StreamProducer producer;
  private final Codec codec;
  private final int maxFrameSize;
  private final Clock clock;
  private final Function<Message, String> filterValueExtractor;

  @SuppressWarnings("unchecked")
  DynamicBatchMessageAccumulator(
      int subEntrySize,
      int batchSize,
      Codec codec,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Function<Message, String> filterValueExtractor,
      Clock clock,
      String stream,
      CompressionCodec compressionCodec,
      ByteBufAllocator byteBufAllocator,
      ObservationCollector<?> observationCollector,
      StreamProducer producer) {
    this.producer = producer;
    this.stream = stream;
    this.publishSequenceFunction = publishSequenceFunction;
    this.observationCollector = (ObservationCollector<Object>) observationCollector;
    this.codec = codec;
    this.clock = clock;
    this.maxFrameSize = maxFrameSize;
    this.filterValueExtractor =
        filterValueExtractor == null ? NULL_FILTER_VALUE_EXTRACTOR : filterValueExtractor;
    if (subEntrySize <= 1) {
      this.dynamicBatch = new DynamicBatch<>(this::publish, batchSize);
    } else {
      byte compressionCode =
          compressionCodec == null ? Compression.NONE.code() : compressionCodec.code();
      this.dynamicBatch =
          new DynamicBatch<>(
              items -> {
                if (this.producer.canSend()) {
                  List<Object> subBatches = new ArrayList<>();
                  int count = 0;
                  ProducerUtils.Batch batch =
                      new ProducerUtils.Batch(
                          Client.EncodedMessageBatch.create(
                              byteBufAllocator, compressionCode, compressionCodec, subEntrySize),
                          new ProducerUtils.CompositeConfirmationCallback(
                              new ArrayList<>(subEntrySize)));
                  AccumulatedEntity lastMessageInBatch = null;
                  for (Object msg : items) {
                    AccumulatedEntity message = (AccumulatedEntity) msg;
                    this.observationCollector.published(
                        message.observationContext(), message.confirmationCallback().message());
                    lastMessageInBatch = message;
                    batch.add(
                        (Codec.EncodedMessage) message.encodedEntity(),
                        message.confirmationCallback());
                    count++;
                    if (count == subEntrySize) {
                      batch.time = lastMessageInBatch.time();
                      batch.publishingId = lastMessageInBatch.publishingId();
                      batch.encodedMessageBatch.close();
                      subBatches.add(batch);
                      lastMessageInBatch = null;
                      batch =
                          new ProducerUtils.Batch(
                              Client.EncodedMessageBatch.create(
                                  byteBufAllocator,
                                  compressionCode,
                                  compressionCodec,
                                  subEntrySize),
                              new ProducerUtils.CompositeConfirmationCallback(
                                  new ArrayList<>(subEntrySize)));
                      count = 0;
                    }
                  }

                  if (!batch.isEmpty() && count < subEntrySize) {
                    batch.time = lastMessageInBatch.time();
                    batch.publishingId = lastMessageInBatch.publishingId();
                    batch.encodedMessageBatch.close();
                    subBatches.add(batch);
                  }

                  return this.publish(subBatches);
                } else {
                  return false;
                }
              },
              batchSize * subEntrySize);
    }
  }

  @Override
  public void add(Message message, ConfirmationHandler confirmationHandler) {
    Object observationContext = this.observationCollector.prePublish(this.stream, message);
    Codec.EncodedMessage encodedMessage = this.codec.encode(message);
    Client.checkMessageFitsInFrame(this.maxFrameSize, encodedMessage);
    long publishingId = this.publishSequenceFunction.applyAsLong(message);
    this.dynamicBatch.add(
        new ProducerUtils.SimpleAccumulatedEntity(
            this.clock.time(),
            publishingId,
            this.filterValueExtractor.apply(message),
            this.codec.encode(message),
            new ProducerUtils.SimpleConfirmationCallback(message, confirmationHandler),
            observationContext));
  }

  @Override
  public int size() {
    // TODO compute dynamic batch message accumulator pending message count
    return 0;
  }

  @Override
  public void flush(boolean force) {}

  private boolean publish(List<Object> entities) {
    if (this.producer.canSend()) {
      this.producer.publishInternal(entities);
      return true;
    } else {
      return false;
    }
  }
}
