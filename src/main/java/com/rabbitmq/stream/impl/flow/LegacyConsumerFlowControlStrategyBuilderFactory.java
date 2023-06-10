package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilder;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilderFactory;
import com.rabbitmq.stream.flow.CreditAsker;

import java.util.function.Supplier;

public class LegacyConsumerFlowControlStrategyBuilderFactory implements ConsumerFlowControlStrategyBuilderFactory<LegacyConsumerFlowControlStrategy, LegacyConsumerFlowControlStrategyBuilderFactory.Builder> {

    public static final LegacyConsumerFlowControlStrategyBuilderFactory INSTANCE = new LegacyConsumerFlowControlStrategyBuilderFactory();

    @Override
    public Builder builder(ConsumerBuilder consumerBuilder) {
        return new Builder(consumerBuilder);
    }

    public static class Builder implements ConsumerFlowControlStrategyBuilder<LegacyConsumerFlowControlStrategy> {

        private final ConsumerBuilder consumerBuilder;

        private int initialCredits = 1;

        private int additionalCredits = 1;

        public Builder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public LegacyConsumerFlowControlStrategy build(Supplier<CreditAsker> creditAskerSupplier) {
            return new LegacyConsumerFlowControlStrategy(creditAskerSupplier, this.initialCredits, this.additionalCredits);
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
