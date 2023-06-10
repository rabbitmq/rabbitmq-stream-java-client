package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.AbstractConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.CreditAsker;
import com.rabbitmq.stream.flow.MessageHandlingAware;
import com.rabbitmq.stream.impl.ConsumerStatisticRecorder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

/**
 * Abstract class that calls an instance of {@link ConsumerStatisticRecorder} and exposes it to child implementations
 * that may use its statistics to control flow as they see fit.
 */
public abstract class AbstractStatisticRecordingConsumerFlowControlStrategy
        extends AbstractConsumerFlowControlStrategy
        implements MessageHandlingAware {

    protected final ConsumerStatisticRecorder consumerStatisticRecorder = new ConsumerStatisticRecorder();

    protected AbstractStatisticRecordingConsumerFlowControlStrategy(Supplier<CreditAsker> creditAskerSupplier) {
        super(creditAskerSupplier);
    }

    /**
     * Note for implementors: This method MUST be called from the implementation of
     * {@link ConsumerFlowControlStrategy#handleSubscribeReturningInitialCredits},
     * otherwise statistics will not be registered!
     * <br/><br/>
     * {@inheritDoc}
     */
    @Override
    public void handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    ) {
        super.handleSubscribe(subscriptionId, stream, offsetSpecification, subscriptionProperties);
        this.consumerStatisticRecorder.handleSubscribe(
                subscriptionId,
                stream,
                offsetSpecification,
                subscriptionProperties
        );
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        super.handleChunk(subscriptionId, offset, messageCount, dataSize);
        this.consumerStatisticRecorder.handleChunk(
            subscriptionId,
            offset,
            messageCount,
            dataSize
        );
    }

    @Override
    public void handleMessage(
            byte subscriptionId,
            long offset,
            long chunkTimestamp,
            long committedChunkId,
            Message message
    ) {
        super.handleMessage(subscriptionId, offset, chunkTimestamp, committedChunkId, message);
        this.consumerStatisticRecorder.handleMessage(
                subscriptionId,
                offset,
                chunkTimestamp,
                committedChunkId,
                message
        );
    }

    @Override
    public void handleCreditNotification(byte subscriptionId, short responseCode) {
        super.handleCreditNotification(subscriptionId, responseCode);
        this.consumerStatisticRecorder.handleCreditNotification(subscriptionId, responseCode);
    }

    @Override
    public void handleUnsubscribe(byte subscriptionId) {
        super.handleUnsubscribe(subscriptionId);
        this.consumerStatisticRecorder.handleUnsubscribe(subscriptionId);
    }

    protected int registerCredits(byte subscriptionId, IntUnaryOperator askedToAsk, boolean askForCredits) {
        AtomicInteger outerCreditsToAsk = new AtomicInteger();
        ConsumerStatisticRecorder.SubscriptionStatistics subscriptionStatistics = this.consumerStatisticRecorder
                .getSubscriptionStatisticsMap()
                .get(subscriptionId);
        subscriptionStatistics.getPendingChunks().updateAndGet(credits -> {
            int creditsToAsk = askedToAsk.applyAsInt(credits);
            outerCreditsToAsk.set(creditsToAsk);
            return credits + creditsToAsk;
        });
        int finalCreditsToAsk = outerCreditsToAsk.get();
        if(askForCredits && finalCreditsToAsk > 0) {
            mandatoryClient().credit(subscriptionId, finalCreditsToAsk);
        }
        return finalCreditsToAsk;
    }

    @Override
    public boolean markHandled(MessageHandler.Context messageContext) {
        ConsumerStatisticRecorder.AggregatedMessageStatistics messageStatistics = this.consumerStatisticRecorder
                .retrieveStatistics(messageContext);
        if(messageStatistics == null) {
            return false;
        }
        boolean markedAsHandled = this.consumerStatisticRecorder.markHandled(messageStatistics);
        if(!markedAsHandled) {
            return false;
        }
        afterMarkHandledStateChanged(messageContext, messageStatistics);
        return true;
    }

    protected void afterMarkHandledStateChanged(
            MessageHandler.Context messageContext,
            ConsumerStatisticRecorder.AggregatedMessageStatistics messageStatistics) {
        // Default no-op callback
    }
}
