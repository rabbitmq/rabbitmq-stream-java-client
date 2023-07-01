package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.ConsumerBuilder;

/**
 * Fluent builder for a {@link ConsumerFlowControlStrategyBuilderFactory}.
 * One instance of this is set per {@link com.rabbitmq.stream.Consumer}.
 * A {@link com.rabbitmq.stream.Consumer} may have multiple subscriptions, and thus multiple instances built by this.
 *
 * @param <T> the type of {@link ConsumerFlowControlStrategy} to be built
 */
public interface ConsumerFlowControlStrategyBuilder<T extends ConsumerFlowControlStrategy> extends ConsumerBuilder.ConsumerBuilderAccessor {
    /**
     * Builds the actual FlowControlStrategy instance, for the Client with which it interoperates
     *
     * @param identifier A {@link String} to uniquely identify the built instance and/or its subscription.
     * @param creditAsker {@link CreditAsker} for asking for credits.
     * @return {@link T} the built {@link ConsumerFlowControlStrategy}
     */
    T build(String identifier, CreditAsker creditAsker);
}
