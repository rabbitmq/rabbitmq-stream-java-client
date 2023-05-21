package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

/**
 * Exposes callbacks to handle events from a particular {@link Client},
 * with specific names for methods and no {@link Client} parameter.
 */
public interface ClientDataHandler extends
        Client.PublishConfirmListener,
        Client.PublishErrorListener,
        Client.ChunkListener,
        Client.MessageListener,
        Client.CreditNotification,
        Client.ConsumerUpdateListener,
        Client.ShutdownListener,
        Client.MetadataListener {

    @Override
    default void handle(byte publisherId, long publishingId) {
        this.handlePublishConfirm(publisherId, publishingId);
    }

    default void handlePublishConfirm(byte publisherId, long publishingId) {
        // No-op by default
    }

    @Override
    default void handle(byte publisherId, long publishingId, short errorCode) {
        this.handlePublishError(publisherId, publishingId, errorCode);
    }

    default void handlePublishError(byte publisherId, long publishingId, short errorCode) {
        // No-op by default
    }

    @Override
    default void handle(Client client, byte subscriptionId, long offset, long messageCount, long dataSize) {
        this.handleChunk(subscriptionId, offset, messageCount, dataSize);
    }

    default void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        // No-op by default
    }

    @Override
    default void handle(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        this.handleMessage(subscriptionId, offset, chunkTimestamp, committedChunkId, message);
    }

    default void handleMessage(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        // No-op by default
    }

    @Override
    default void handle(byte subscriptionId, short responseCode) {
        this.handleCreditNotification(subscriptionId, responseCode);
    }

    default void handleCreditNotification(byte subscriptionId, short responseCode) {
        // No-op by default
    }

    @Override
    default OffsetSpecification handle(Client client, byte subscriptionId, boolean active) {
        this.handleConsumerUpdate(subscriptionId, active);
        return null;
    }

    default void handleConsumerUpdate(byte subscriptionId, boolean active) {
        // No-op by default
    }

    @Override
    default void handle(Client.ShutdownContext shutdownContext) {
        this.handleShutdown(shutdownContext);
    }

    default void handleShutdown(Client.ShutdownContext shutdownContext) {
        // No-op by default
    }

    @Override
    default void handle(String stream, short code) {
        this.handleMetadata(stream, code);
    }

    default void handleMetadata(String stream, short code) {
        // No-op by default
    }

}
