package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.impl.ClientDataHandler;

import java.util.Map;

/**
 * A built and configured flow control strategy for consumers.
 * Implementations may freely implement reactions to the various client callbacks.
 * When defined by each implementation, it may internally call {@link CreditAsker#credit} to ask for credits.
 */
// TODO: Decouple from ClientDataHandler. Maybe create an adapter pattern for handling this, or something.
public interface ConsumerFlowControlStrategy extends ClientDataHandler {

    /**
     * Callback for handling a new stream subscription.
     * Called right before the subscription is sent to the actual client.
     * <p>
     * Either this variant or {@link ClientDataHandler#handleSubscribe(byte, String, OffsetSpecification, Map)} should be called, NOT both.
     * </p>
     *
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     * @param stream The name of the stream being subscribed to
     * @param offsetSpecification The offset specification for this new subscription
     * @param subscriptionProperties The subscription properties for this new subscription
     * @return The initial credits that should be granted to this new subscription
     */
    int handleSubscribeReturningInitialCredits(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    );

    @Override
    default void handleSubscribe(byte subscriptionId, String stream, OffsetSpecification offsetSpecification, Map<String, String> subscriptionProperties) {
        handleSubscribeReturningInitialCredits(
                subscriptionId,
                stream,
                offsetSpecification,
                subscriptionProperties
        );
    }

}
