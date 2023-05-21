package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

import java.util.function.Supplier;

public class LegacyFlowControlStrategyBuilderFactory implements ConsumerFlowControlStrategyBuilderFactory<LegacyFlowControlStrategy, LegacyFlowControlStrategyBuilderFactory.LegacyFlowControlStrategyBuilder> {

    public static final LegacyFlowControlStrategyBuilderFactory INSTANCE = new LegacyFlowControlStrategyBuilderFactory();

    @Override
    public LegacyFlowControlStrategyBuilder builder(ConsumerBuilder consumerBuilder) {
        return new LegacyFlowControlStrategyBuilder(consumerBuilder);
    }

    public static class LegacyFlowControlStrategyBuilder implements ConsumerFlowControlStrategyBuilder<LegacyFlowControlStrategy> {

        private final ConsumerBuilder consumerBuilder;

        private int initialCredits = 1;

        private int additionalCredits = 1;

        public LegacyFlowControlStrategyBuilder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public LegacyFlowControlStrategy build(Supplier<Client> clientSupplier) {
            return new LegacyFlowControlStrategy(clientSupplier, this.initialCredits, this.additionalCredits);
        }

        @Override
        public ConsumerBuilder builder() {
            return this.consumerBuilder;
        }

        public LegacyFlowControlStrategyBuilder additionalCredits(int additionalCredits) {
            this.additionalCredits = additionalCredits;
            return this;
        }

        public LegacyFlowControlStrategyBuilder initialCredits(int initialCredits) {
            this.initialCredits = initialCredits;
            return this;
        }
    }

}
