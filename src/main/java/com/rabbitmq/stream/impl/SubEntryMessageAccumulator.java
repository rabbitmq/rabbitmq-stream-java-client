package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Codec.EncodedMessage;
import java.util.ArrayList;
import java.util.List;

class SubEntryMessageAccumulator extends SimpleMessageAccumulator {

  private final int subEntrySize;

  public SubEntryMessageAccumulator(
      int subEntrySize, int batchSize, Codec codec, int maxFrameSize) {
    super(subEntrySize * batchSize, codec, maxFrameSize);
    this.subEntrySize = subEntrySize;
  }

  private Batch createBatch() {
    return new Batch(
        new Client.EncodedMessageBatch(MessageBatch.Compression.NONE, new ArrayList<>()),
        new CompositeConfirmationCallback(new ArrayList<>()));
  }

  @Override
  public AccumulatedEntity get() {
    if (this.messages.isEmpty()) {
      return null;
    }
    int count = 0;
    Batch batch = createBatch();
    while (count != this.subEntrySize) {
      AccumulatedEntity message = messages.poll();
      if (message == null) {
        break;
      }
      batch.add((EncodedMessage) message.encodedEntity(), message.confirmationCallback());
      count++;
    }
    return batch.isEmpty() ? null : batch;
  }

  private static class Batch implements AccumulatedEntity {

    private final Client.EncodedMessageBatch encodedMessageBatch;
    private final CompositeConfirmationCallback confirmationCallback;

    private Batch(
        Client.EncodedMessageBatch encodedMessageBatch,
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
    public Object encodedEntity() {
      return encodedMessageBatch;
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
