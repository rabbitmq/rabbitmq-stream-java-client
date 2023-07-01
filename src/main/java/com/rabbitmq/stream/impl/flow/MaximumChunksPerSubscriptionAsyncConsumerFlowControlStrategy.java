package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilder;
import com.rabbitmq.stream.flow.CreditAsker;
import com.rabbitmq.stream.flow.MessageHandlingListener;
import com.rabbitmq.stream.impl.ConsumerStatisticRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.IntUnaryOperator;

/**
 * A flow control strategy that enforces a maximum amount of Inflight chunks per registered subscription.
 * Based on {@link MessageHandlingListener message acknowledgement}, asking for the maximum number of chunks possible, given the limit.
 */
public class MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy extends AbstractStatisticRecordingConsumerFlowControlStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy.class);

    private final int maximumSimultaneousChunksPerSubscription;

    public MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy(
            String identifier,
            CreditAsker creditAsker,
            int maximumSimultaneousChunksPerSubscription
    ) {
        super(identifier, creditAsker);
        if(maximumSimultaneousChunksPerSubscription <= 0) {
            throw new IllegalArgumentException(
                "maximumSimultaneousChunksPerSubscription must be greater than 0. Was: " + maximumSimultaneousChunksPerSubscription
            );
        }
        this.maximumSimultaneousChunksPerSubscription = maximumSimultaneousChunksPerSubscription;
    }

    @Override
    public int handleSubscribeReturningInitialCredits(
            OffsetSpecification offsetSpecification,
            boolean isInitialSubscription) {
        this.handleSubscribe(
            offsetSpecification,
            isInitialSubscription
        );
        return registerCredits(getCreditRegistererFunction(), false);
    }

    @Override
    protected void afterMarkHandledStateChanged(
            MessageHandler.Context messageContext,
            ConsumerStatisticRecorder.AggregatedMessageStatistics messageStatistics) {
        registerCredits(getCreditRegistererFunction(), true);
    }

    private IntUnaryOperator getCreditRegistererFunction() {
        return pendingChunks -> {
            int inProcessingChunks = extractInProcessingChunks();
            return Math.max(0, this.maximumSimultaneousChunksPerSubscription - (pendingChunks + inProcessingChunks));
        };
    }

    private int extractInProcessingChunks() {
        int inProcessingChunks;
        ConsumerStatisticRecorder.SubscriptionStatistics subscriptionStats = this.consumerStatisticRecorder
                .getSubscriptionStatistics();
        if(subscriptionStats == null) {
            LOGGER.warn("Subscription data not found while calculating credits to ask! Identifier: {}", this.getIdentifier());
            inProcessingChunks = 0;
        } else {
            inProcessingChunks = subscriptionStats.getUnprocessedChunksByOffset().size();
        }
        return inProcessingChunks;
    }

    public static Builder builder(ConsumerBuilder consumerBuilder) {
        return new Builder(consumerBuilder);
    }

    public static class Builder implements ConsumerFlowControlStrategyBuilder<MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy>, ConsumerBuilder.ConsumerBuilderAccessor {

        private final ConsumerBuilder consumerBuilder;

        private int maximumInflightChunksPerSubscription = 1;

        public Builder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy build(String identifier, CreditAsker creditAsker) {
            return new MaximumChunksPerSubscriptionAsyncConsumerFlowControlStrategy(
                    identifier,
                    creditAsker,
                    this.maximumInflightChunksPerSubscription
            );
        }

        @Override
        public ConsumerBuilder builder() {
            return this.consumerBuilder;
        }

        public Builder maximumInflightChunksPerSubscription(int maximumInflightChunksPerSubscription) {
            this.maximumInflightChunksPerSubscription = maximumInflightChunksPerSubscription;
            return this;
        }

    }

}
