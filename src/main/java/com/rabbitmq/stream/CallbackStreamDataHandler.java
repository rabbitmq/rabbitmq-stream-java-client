package com.rabbitmq.stream;

import java.util.Map;

/**
 * Exposes callbacks to handle events from a particular Stream connection,
 * with specific names for methods and no connection-oriented parameter.
 */
public interface CallbackStreamDataHandler {

    default void handlePublishConfirm(byte publisherId, long publishingId) {
        // No-op by default
    }

    default void handlePublishError(byte publisherId, long publishingId, short errorCode) {
        // No-op by default
    }

    default void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        // No-op by default
    }

    default void handleMessage(byte subscriptionId, long offset, long chunkTimestamp, long committedChunkId, Message message) {
        // No-op by default
    }

    default void handleCreditNotification(byte subscriptionId, short responseCode) {
        // No-op by default
    }

    default void handleConsumerUpdate(byte subscriptionId, boolean active) {
        // No-op by default
    }

    default void handleMetadata(String stream, short code) {
        // No-op by default
    }

    /**
     * Callback for handling a stream subscription.
     *
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     * @param stream The name of the stream being subscribed to
     * @param offsetSpecification The offset specification for this new subscription
     * @param subscriptionProperties The subscription properties for this new subscription
     * @param isInitialSubscription Whether this subscription is an initial subscription
     *                              or a recovery for an existing subscription
     */
    default void handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties,
            boolean isInitialSubscription
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
