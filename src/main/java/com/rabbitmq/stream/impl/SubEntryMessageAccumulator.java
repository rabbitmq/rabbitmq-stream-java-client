package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Codec.EncodedMessage;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.compression.Compression;
import com.rabbitmq.stream.impl.Client.EncodedMessageBatch;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

class SubEntryMessageAccumulator extends SimpleMessageAccumulator {

  private final int subEntrySize;

  public SubEntryMessageAccumulator(
      int subEntrySize,
      int batchSize,
      Codec codec,
      int maxFrameSize,
      ToLongFunction<Message> publishSequenceFunction,
      Clock clock) {
    super(subEntrySize * batchSize, codec, maxFrameSize, publishSequenceFunction, clock);
    this.subEntrySize = subEntrySize;
  }

  private Batch createBatch() {
    return new Batch(
        EncodedMessageBatch.create(Compression.NONE, new ArrayList<>()),
        new CompositeConfirmationCallback(new ArrayList<>()));
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
      lastMessageInBatch = message;
      batch.add((EncodedMessage) message.encodedEntity(), message.confirmationCallback());
      count++;
    }
    if (batch.isEmpty()) {
      return null;
    } else {
      batch.time = lastMessageInBatch.time();
      batch.publishingId = lastMessageInBatch.publishindId();
      batch.encodedMessageBatch.close();
      return batch;
    }
  }

  private static class Batch implements AccumulatedEntity {

    private final EncodedMessageBatch encodedMessageBatch;
    private final CompositeConfirmationCallback confirmationCallback;
    private volatile long publishingId;
    private volatile long time;

    private Batch(
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
    public long publishindId() {
      return publishingId;
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
  }

  private static class CompositeConfirmationCallback
      implements StreamProducer.ConfirmationCallback {

    private final List<StreamProducer.ConfirmationCallback> callbacks;

    private CompositeConfirmationCallback(List<StreamProducer.ConfirmationCallback> callbacks) {
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
  }
}
