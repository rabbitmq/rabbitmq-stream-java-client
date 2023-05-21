package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

import java.util.Map;

/**
 * A built and configured flow control strategy for consumers.
 * Implementations may freely implement reactions to the various client callbacks.
 * When defined by each implementation, it may internally call {@link Client#credit} to ask for credits.
 */
public interface ConsumerFlowControlStrategy extends ClientDataHandler, AutoCloseable {

    /**
     * Callback for handling a new stream subscription.
     * Called right before the subscription is sent to the actual client.
     *
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     * @param stream The name of the stream being subscribed to
     * @param offsetSpecification The offset specification for this new subscription
     * @param subscriptionProperties The subscription properties for this new subscription
     * @return The initial credits that should be granted to this new subscription
     */
    int handleSubscribe(
        byte subscriptionId,
        String stream,
        OffsetSpecification offsetSpecification,
        Map<String, String> subscriptionProperties
    );

    /**
     * Callback for handling a stream unsubscription.
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     */
    default void handleUnsubscribe(byte subscriptionId) {
        // No-op by default
    }

    @Override
    default void handleShutdown(Client.ShutdownContext shutdownContext) {
        this.close();
    }

    @Override
    default void close() {
        // Override with cleanup logic, if applicable
    }

}
