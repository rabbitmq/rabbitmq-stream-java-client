package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.ConfirmationHandler;
import com.rabbitmq.stream.ConfirmationStatus;
import com.rabbitmq.stream.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SubEntryMessageAccumulator implements MessageAccumulator {

    private final int subEntrySize, batchSize;

    private final BlockingQueue<Batch> batches;

    private final Codec codec;

    private final int maxFrameSize;
    private final String stream;
    private volatile Batch currentBatch;

    public SubEntryMessageAccumulator(int subEntrySize, int batchSize, Codec codec, int maxFrameSize, String stream) {
        this.batchSize = batchSize;
        this.batches = new LinkedBlockingQueue<>(batchSize);
        this.subEntrySize = subEntrySize;
        this.codec = codec;
        this.maxFrameSize = maxFrameSize;
        this.currentBatch = createBatch();
        this.stream = stream;
    }

    private Batch createBatch() {
        return new Batch(
                new Client.EncodedMessageBatch(MessageBatch.Compression.NONE, new ArrayList<>(subEntrySize)),
                new CompositeConfirmationCallback(new ArrayList<>(subEntrySize))
        );
    }

    @Override
    public synchronized boolean add(Message message, ConfirmationHandler confirmationHandler) {
        Codec.EncodedMessage encodedMessage = this.codec.encode(message);
        Client.checkMessageFitsInFrame(this.maxFrameSize, stream, encodedMessage);
        this.currentBatch.add(encodedMessage, new SimpleConfirmationCallback(message, confirmationHandler));
        if (this.currentBatch.count.get() == this.subEntrySize) {
            // FIXME make sure batch fits in frame
            this.batches.add(this.currentBatch);
            this.currentBatch = createBatch();
        }
        return this.batches.size() == this.batchSize;
    }

    @Override
    public AccumulatedEntity get() {
        Batch batch = batches.poll();
        if (batch == null) {
            if (this.currentBatch.isEmpty()) {
                return null;
            } else {
                Batch toReturn = this.currentBatch;
                this.currentBatch = createBatch();
                return toReturn;
            }
        } else {
            return batch;
        }
    }

    @Override
    public boolean isEmpty() {
        return batches.isEmpty() && this.currentBatch.isEmpty();
    }

    private static class Batch implements AccumulatedEntity {

        private final Client.EncodedMessageBatch encodedMessageBatch;
        private final CompositeConfirmationCallback confirmationCallback;
        private final AtomicInteger count = new AtomicInteger(0);

        private Batch(Client.EncodedMessageBatch encodedMessageBatch, CompositeConfirmationCallback confirmationCallback) {
            this.encodedMessageBatch = encodedMessageBatch;
            this.confirmationCallback = confirmationCallback;
        }

        void add(Codec.EncodedMessage encodedMessage, StreamProducer.ConfirmationCallback confirmationCallback) {
            this.encodedMessageBatch.add(encodedMessage);
            this.confirmationCallback.add(confirmationCallback);
            count.incrementAndGet();
        }

        private boolean isEmpty() {
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

    private static class MessageConfirmationHandler {

        private final Message message;
        private final ConfirmationHandler confirmationHandler;

        private MessageConfirmationHandler(Message message, ConfirmationHandler confirmationHandler) {
            this.message = message;
            this.confirmationHandler = confirmationHandler;
        }
    }

    private static class CompositeConfirmationCallback implements StreamProducer.ConfirmationCallback {

        private final List<StreamProducer.ConfirmationCallback> callbacks;

        private CompositeConfirmationCallback(List<StreamProducer.ConfirmationCallback> callbacks) {
            this.callbacks = callbacks;
        }

        private void add(StreamProducer.ConfirmationCallback confirmationCallback) {
            this.callbacks.add(confirmationCallback);
        }

        @Override
        public void handle(boolean confirmed, short code) {
            for (StreamProducer.ConfirmationCallback callback : callbacks) {
                callback.handle(confirmed, code);
            }
        }
    }

    private static final class SimpleConfirmationCallback implements StreamProducer.ConfirmationCallback {

        private final Message message;
        private final ConfirmationHandler confirmationHandler;

        private SimpleConfirmationCallback(Message message, ConfirmationHandler confirmationHandler) {
            this.message = message;
            this.confirmationHandler = confirmationHandler;
        }

        @Override
        public void handle(boolean confirmed, short code) {
            confirmationHandler.handle(new ConfirmationStatus(message, confirmed, code));
        }
    }

}
