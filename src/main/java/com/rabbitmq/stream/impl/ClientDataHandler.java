package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.OffsetSpecification;

import java.util.Map;

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

    /**
     * Callback for handling a new stream subscription.
     *
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     * @param stream The name of the stream being subscribed to
     * @param offsetSpecification The offset specification for this new subscription
     * @param subscriptionProperties The subscription properties for this new subscription
     */
    default void handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    ) {
        // No-op by default
    }

    /**
     * Callback for handling a stream unsubscription.
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     */
    default void handleUnsubscribe(byte subscriptionId) {
        // No-op by default
    }

}
