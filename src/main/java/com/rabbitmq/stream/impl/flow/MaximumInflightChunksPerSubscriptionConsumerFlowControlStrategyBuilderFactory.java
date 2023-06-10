package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilder;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilderFactory;
import com.rabbitmq.stream.flow.CreditAsker;

import java.util.function.Supplier;

public class MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory implements ConsumerFlowControlStrategyBuilderFactory<MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy, MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory.Builder> {

    public static final MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory INSTANCE = new MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory();

    @Override
    public Builder builder(ConsumerBuilder consumerBuilder) {
        return new Builder(consumerBuilder);
    }

    public static class Builder implements ConsumerFlowControlStrategyBuilder<MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy> {

        private final ConsumerBuilder consumerBuilder;

        private int maximumInflightChunksPerSubscription = 1;

        public Builder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy build(Supplier<CreditAsker> creditAskerSupplier) {
            return new MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy(creditAskerSupplier, this.maximumInflightChunksPerSubscription);
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
