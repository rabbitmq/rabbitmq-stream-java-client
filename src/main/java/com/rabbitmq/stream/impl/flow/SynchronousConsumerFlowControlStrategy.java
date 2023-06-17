package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.flow.AbstractConsumerFlowControlStrategy;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilder;
import com.rabbitmq.stream.flow.CreditAsker;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The default flow control strategy.
 * Requests a set amount of credits after each chunk arrives.
 * Ideal for usage when the message is consumed synchronously inside the message handler,
 * which is the case for most Consumers.
 */
public class SynchronousConsumerFlowControlStrategy extends AbstractConsumerFlowControlStrategy {

    private final int initialCredits;
    private final int additionalCredits;

    public SynchronousConsumerFlowControlStrategy(Supplier<CreditAsker> creditAskerSupplier, int initialCredits, int additionalCredits) {
        super(creditAskerSupplier);
        this.initialCredits = initialCredits;
        this.additionalCredits = additionalCredits;
    }

    @Override
    public int handleSubscribeReturningInitialCredits(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties,
            boolean isInitialSubscription
    ) {
        return this.initialCredits;
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        mandatoryCreditAsker().credit(subscriptionId, this.additionalCredits);
    }

    public static SynchronousConsumerFlowControlStrategy.Builder builder(ConsumerBuilder consumerBuilder) {
        return new SynchronousConsumerFlowControlStrategy.Builder(consumerBuilder);
    }

    public static class Builder implements ConsumerFlowControlStrategyBuilder<SynchronousConsumerFlowControlStrategy> {

        private final ConsumerBuilder consumerBuilder;

        private int initialCredits = 1;

        private int additionalCredits = 1;

        public Builder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public SynchronousConsumerFlowControlStrategy build(Supplier<CreditAsker> creditAskerSupplier) {
            return new SynchronousConsumerFlowControlStrategy(creditAskerSupplier, this.initialCredits, this.additionalCredits);
        }

        @Override
        public ConsumerBuilder builder() {
            return this.consumerBuilder;
        }

        public Builder additionalCredits(int additionalCredits) {
            this.additionalCredits = additionalCredits;
            return this;
        }

        public Builder initialCredits(int initialCredits) {
            this.initialCredits = initialCredits;
            return this;
        }

    }

}
