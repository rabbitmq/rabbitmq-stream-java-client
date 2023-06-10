package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.CreditAsker;
import com.rabbitmq.stream.flow.MessageHandlingAware;
import com.rabbitmq.stream.impl.ConsumerStatisticRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

/**
 * A flow control strategy that enforces a maximum amount of Inflight chunks per registered subscription.
 * Based on {@link MessageHandlingAware message acknowledgement}, asking for the maximum number of chunks possible, given the limit.
 */
public class MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy extends AbstractStatisticRecordingConsumerFlowControlStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy.class);

    private final int maximumSimultaneousChunksPerSubscription;

    public MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy(
            Supplier<CreditAsker> creditAskerSupplier,
            int maximumSimultaneousChunksPerSubscription
    ) {
        super(creditAskerSupplier);
        if(maximumSimultaneousChunksPerSubscription <= 0) {
            throw new IllegalArgumentException(
                "maximumSimultaneousChunksPerSubscription must be greater than 0. Was: " + maximumSimultaneousChunksPerSubscription
            );
        }
        this.maximumSimultaneousChunksPerSubscription = maximumSimultaneousChunksPerSubscription;
    }

    @Override
    public int handleSubscribeReturningInitialCredits(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties) {
        this.handleSubscribe(
            subscriptionId,
            stream,
            offsetSpecification,
            subscriptionProperties
        );
        return registerCredits(subscriptionId, getCreditAsker(subscriptionId), false);
    }

    @Override
    protected void afterMarkHandledStateChanged(
            MessageHandler.Context messageContext,
            ConsumerStatisticRecorder.AggregatedMessageStatistics messageStatistics) {
        byte subscriptionId = messageStatistics.getSubscriptionStatistics().getSubscriptionId();
        registerCredits(subscriptionId, getCreditAsker(subscriptionId), true);
    }

    private IntUnaryOperator getCreditAsker(byte subscriptionId) {
        return pendingChunks -> {
            int inProcessingChunks = extractInProcessingChunks(subscriptionId);
            return Math.max(0, this.maximumSimultaneousChunksPerSubscription - (pendingChunks + inProcessingChunks));
        };
    }

    private int extractInProcessingChunks(byte subscriptionId) {
        int inProcessingChunks;
        ConsumerStatisticRecorder.SubscriptionStatistics subscriptionStats = this.consumerStatisticRecorder
                .getSubscriptionStatisticsMap()
                .get(subscriptionId);
        if(subscriptionStats == null) {
            LOGGER.warn("Subscription data not found while calculating credits to ask! subscriptionId: {}", subscriptionId);
            inProcessingChunks = 0;
        } else {
            inProcessingChunks = subscriptionStats.getUnprocessedChunksByOffset().size();
        }
        return inProcessingChunks;
    }

}
