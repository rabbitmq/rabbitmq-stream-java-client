package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.AbstractConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.CreditAsker;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The flow control strategy that was always applied before the flow control strategy mechanism existed in the codebase.
 * Requests a set amount of credits after each chunk arrives.
 */
public class LegacyConsumerFlowControlStrategy extends AbstractConsumerFlowControlStrategy {

    private final int initialCredits;
    private final int additionalCredits;

    public LegacyConsumerFlowControlStrategy(Supplier<CreditAsker> creditAskerSupplier, int initialCredits, int additionalCredits) {
        super(creditAskerSupplier);
        this.initialCredits = initialCredits;
        this.additionalCredits = additionalCredits;
    }

    @Override
    public int handleSubscribeReturningInitialCredits(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    ) {
        return this.initialCredits;
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        mandatoryCreditAsker().credit(subscriptionId, this.additionalCredits);
    }

}
