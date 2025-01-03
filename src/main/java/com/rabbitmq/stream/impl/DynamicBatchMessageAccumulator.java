// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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

  private final DynamicBatch<Object> dynamicBatch;
  private final ObservationCollector<Object> observationCollector;
  private final StreamProducer producer;
  private final ProducerUtils.MessageAccumulatorHelper helper;

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
    this.helper =
        new ProducerUtils.MessageAccumulatorHelper(
            codec,
            maxFrameSize,
            publishSequenceFunction,
            filterValueExtractor,
            clock,
            stream,
            observationCollector);
    this.producer = producer;
    this.observationCollector = (ObservationCollector<Object>) observationCollector;
    boolean shouldObserve = !this.observationCollector.isNoop();
    if (subEntrySize <= 1) {
      this.dynamicBatch =
          new DynamicBatch<>(
              items -> {
                boolean result = this.publish(items);
                if (result && shouldObserve) {
                  items.forEach(
                      i -> {
                        AccumulatedEntity entity = (AccumulatedEntity) i;
                        this.observationCollector.published(
                            entity.observationContext(), entity.confirmationCallback().message());
                      });
                }
                return result;
              },
              batchSize);
    } else {
      byte compressionCode =
          compressionCodec == null ? Compression.NONE.code() : compressionCodec.code();
      this.dynamicBatch =
          new DynamicBatch<>(
              items -> {
                List<Object> subBatches = new ArrayList<>();
                int count = 0;
                ProducerUtils.Batch batch =
                    this.helper.batch(
                        byteBufAllocator, compressionCode, compressionCodec, subEntrySize);
                AccumulatedEntity lastMessageInBatch = null;
                for (Object msg : items) {
                  AccumulatedEntity message = (AccumulatedEntity) msg;
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
                        this.helper.batch(
                            byteBufAllocator, compressionCode, compressionCodec, subEntrySize);
                    count = 0;
                  }
                }

                if (!batch.isEmpty() && count < subEntrySize) {
                  batch.time = lastMessageInBatch.time();
                  batch.publishingId = lastMessageInBatch.publishingId();
                  batch.encodedMessageBatch.close();
                  subBatches.add(batch);
                }
                boolean result = this.publish(subBatches);
                if (result && shouldObserve) {
                  for (Object msg : items) {
                    AccumulatedEntity message = (AccumulatedEntity) msg;
                    this.observationCollector.published(
                        message.observationContext(), message.confirmationCallback().message());
                  }
                }
                return result;
              },
              batchSize * subEntrySize);
    }
  }

  @Override
  public void add(Message message, ConfirmationHandler confirmationHandler) {
    this.dynamicBatch.add(helper.entity(message, confirmationHandler));
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

  @Override
  public void close() {
    this.dynamicBatch.close();
  }
}
