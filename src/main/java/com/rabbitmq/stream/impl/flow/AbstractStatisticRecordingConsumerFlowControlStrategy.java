package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.AbstractConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.AsyncConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.CreditAsker;
import com.rabbitmq.stream.impl.ConsumerStatisticRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

/**
 * Abstract class that calls an instance of {@link ConsumerStatisticRecorder} and exposes it to child implementations
 * that may use its statistics to control flow as they see fit.
 */
public abstract class AbstractStatisticRecordingConsumerFlowControlStrategy
        extends AbstractConsumerFlowControlStrategy
        implements AsyncConsumerFlowControlStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractStatisticRecordingConsumerFlowControlStrategy.class);

    protected final ConsumerStatisticRecorder consumerStatisticRecorder;

    protected AbstractStatisticRecordingConsumerFlowControlStrategy(String identifier, CreditAsker creditAsker) {
        super(identifier, creditAsker);
        this.consumerStatisticRecorder = new ConsumerStatisticRecorder(identifier);
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
            OffsetSpecification offsetSpecification,
            boolean isInitialSubscription
    ) {
        this.consumerStatisticRecorder.handleSubscribe(
                offsetSpecification,
                isInitialSubscription
        );
    }

    @Override
    public void handleChunk(long offset, long messageCount, long dataSize) {
        super.handleChunk(offset, messageCount, dataSize);
        this.consumerStatisticRecorder.handleChunk(
            offset,
            messageCount,
            dataSize
        );
    }

    @Override
    public void handleMessage(
            long offset,
            long chunkTimestamp,
            long committedChunkId,
            Message message
    ) {
        super.handleMessage(offset, chunkTimestamp, committedChunkId, message);
        this.consumerStatisticRecorder.handleMessage(
                offset,
                chunkTimestamp,
                committedChunkId,
                message
        );
    }

    @Override
    public void handleCreditNotification(short responseCode) {
        super.handleCreditNotification(responseCode);
        this.consumerStatisticRecorder.handleCreditNotification(responseCode);
    }

    @Override
    public void handleUnsubscribe() {
        super.handleUnsubscribe();
        this.consumerStatisticRecorder.handleUnsubscribe();
    }

    protected int registerCredits(IntUnaryOperator askedToAsk, boolean askForCredits) {
        AtomicInteger outerCreditsToAsk = new AtomicInteger();
        ConsumerStatisticRecorder.SubscriptionStatistics subscriptionStatistics = this.consumerStatisticRecorder
                .getSubscriptionStatistics();
        if(subscriptionStatistics == null) {
            LOGGER.warn("Lost subscription, returning no credits. askForCredits={}", askForCredits);
            return 0;
        }
        subscriptionStatistics.getPendingChunks().updateAndGet(credits -> {
            int creditsToAsk = askedToAsk.applyAsInt(credits);
            outerCreditsToAsk.set(creditsToAsk);
            return credits + creditsToAsk;
        });
        int finalCreditsToAsk = outerCreditsToAsk.get();
        if(askForCredits && finalCreditsToAsk > 0) {
            LOGGER.debug("Asking for {} credits", finalCreditsToAsk);
            getCreditAsker().credit(finalCreditsToAsk);
        }
        LOGGER.debug("Returning {} credits with askForCredits={}", finalCreditsToAsk, askForCredits);
        return finalCreditsToAsk;
    }

    @Override
    public boolean markHandled(MessageHandler.Context messageContext) {
        ConsumerStatisticRecorder.AggregatedMessageStatistics messageStatistics = this.consumerStatisticRecorder
                .retrieveStatistics(messageContext);
        if(messageStatistics == null) {
            LOGGER.warn("Message statistics not found for offset {} on stream '{}'", messageContext.offset(), messageContext.stream());
            return false;
        }
        boolean markedAsHandled = this.consumerStatisticRecorder.markHandled(messageStatistics);
        if(!markedAsHandled) {
            LOGGER.warn("Message not marked as handled for offset {} on stream '{}'", messageContext.offset(), messageContext.stream());
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
