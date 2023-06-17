package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.ConsumerBuilder;

/**
 * A strategy for regulating consumer flow when consuming from a RabbitMQ Stream.
 * @param <T> the type of {@link ConsumerFlowControlStrategy} to be built
 * @param <C> the type of fluent builder exposed by this factory. Must subclass {@link ConsumerFlowControlStrategyBuilder}
 */
@FunctionalInterface
public interface ConsumerFlowControlStrategyBuilderFactory<T extends ConsumerFlowControlStrategy, C extends ConsumerFlowControlStrategyBuilder<T>> {
    /**
     * Accessor for configuration builder with settings specific to each implementing strategy
     * @return {@link C} the specific consumer flow control strategy configuration builder
     */
    C builder(ConsumerBuilder consumerBuilder);
}
