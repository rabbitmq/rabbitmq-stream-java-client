package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.CallbackStreamDataHandler;
import com.rabbitmq.stream.OffsetSpecification;

import java.util.Map;

/**
 * A built and configured flow control strategy for consumers.
 * Implementations may freely implement reactions to the various client callbacks.
 * When defined by each implementation, it may internally call {@link CreditAsker#credit} to ask for credits.
 * One instance of this is expected to be built for each separate subscription.
 * A {@link com.rabbitmq.stream.Consumer} may have multiple subscriptions, and thus multiple instances of this.
 */
public interface ConsumerFlowControlStrategy extends CallbackStreamDataHandler {

    /**
     * Callback for handling a stream subscription.
     * Called right before the subscription is sent to the broker.
     * <p>
     * Either this variant or {@link CallbackStreamDataHandler#handleSubscribe} should be called, NOT both.
     * </p>
     *
     * @param subscriptionId The subscriptionId as specified by the Stream Protocol
     * @param stream The name of the stream being subscribed to
     * @param offsetSpecification The offset specification for this new subscription
     * @param subscriptionProperties The subscription properties for this new subscription
     * @param isInitialSubscription Whether this subscription is an initial subscription
     *                              or a recovery for an existing subscription
     * @return The initial credits that should be granted to this new subscription
     */
    int handleSubscribeReturningInitialCredits(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties,
            boolean isInitialSubscription
    );

    @Override
    default void handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties,
            boolean isInitialSubscription) {
        handleSubscribeReturningInitialCredits(
                subscriptionId,
                stream,
                offsetSpecification,
                subscriptionProperties,
                isInitialSubscription
        );
    }

}
