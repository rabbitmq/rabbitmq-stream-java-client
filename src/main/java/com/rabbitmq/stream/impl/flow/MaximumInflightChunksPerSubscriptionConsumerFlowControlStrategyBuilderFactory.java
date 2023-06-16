package com.rabbitmq.stream.impl.flow;

import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilder;
import com.rabbitmq.stream.flow.ConsumerFlowControlStrategyBuilderFactory;
import com.rabbitmq.stream.flow.CreditAsker;
import com.rabbitmq.stream.flow.MessageHandlingAware;
import com.rabbitmq.stream.flow.MessageHandlingListenerAware;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Supplier;

public class MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory implements ConsumerFlowControlStrategyBuilderFactory<MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy, MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory.Builder> {

    public static final MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory INSTANCE = new MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategyBuilderFactory();

    @Override
    public Builder builder(ConsumerBuilder consumerBuilder) {
        return new Builder(consumerBuilder);
    }

    public static class Builder implements ConsumerFlowControlStrategyBuilder<MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy>, MessageHandlingListenerAware {

        private final ConsumerBuilder consumerBuilder;

        private int maximumInflightChunksPerSubscription = 1;

        private final Set<MessageHandlingAware> instances = Collections.newSetFromMap(new WeakHashMap<>());

        public Builder(ConsumerBuilder consumerBuilder) {
            this.consumerBuilder = consumerBuilder;
        }

        @Override
        public MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy build(Supplier<CreditAsker> creditAskerSupplier) {
            MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy built = new MaximumInflightChunksPerSubscriptionConsumerFlowControlStrategy(
                    creditAskerSupplier,
                    this.maximumInflightChunksPerSubscription
            );
            instances.add(built);
            return built;
        }

        @Override
        public ConsumerBuilder builder() {
            return this.consumerBuilder;
        }

        public Builder maximumInflightChunksPerSubscription(int maximumInflightChunksPerSubscription) {
            this.maximumInflightChunksPerSubscription = maximumInflightChunksPerSubscription;
            return this;
        }

        @Override
        public MessageHandlingAware messageHandlingListener() {
            return this::messageHandlingMulticaster;
        }

        private boolean messageHandlingMulticaster(MessageHandler.Context context) {
            boolean changed = false;
            for(MessageHandlingAware instance : instances) {
                changed = changed || instance.markHandled(context);
            }
            return changed;
        }
    }

}
