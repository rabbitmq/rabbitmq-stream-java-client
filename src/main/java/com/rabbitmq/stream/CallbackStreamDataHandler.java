package com.rabbitmq.stream;

/**
 * Exposes callbacks to handle events from a particular Stream subscription,
 * with specific names for methods and no connection-oriented parameter.
 */
public interface CallbackStreamDataHandler {

    default void handleChunk(long offset, long messageCount, long dataSize) {
        // No-op by default
    }

    default void handleMessage(long offset, long chunkTimestamp, long committedChunkId, Message message) {
        // No-op by default
    }

    default void handleCreditNotification(short responseCode) {
        // No-op by default
    }

    default void handleConsumerUpdate(boolean active) {
        // No-op by default
    }

    default void handleMetadata(short code) {
        // No-op by default
    }

    /**
     * Callback for handling a stream subscription.
     *
     * @param offsetSpecification The offset specification for this new subscription
     * @param isInitialSubscription Whether this subscription is an initial subscription
     *                              or a recovery for an existing subscription
     */
    default void handleSubscribe(
            OffsetSpecification offsetSpecification,
            boolean isInitialSubscription
    ) {
        // No-op by default
    }

    /**
     * Callback for handling a stream unsubscription.
     */
    default void handleUnsubscribe() {
        if(this instanceof AutoCloseable) {
            try {
                ((AutoCloseable) this).close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
