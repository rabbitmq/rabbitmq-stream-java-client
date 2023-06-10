package com.rabbitmq.stream.flow;

import com.rabbitmq.stream.ConsumerBuilder;

import java.util.function.Supplier;

/**
 * Fluent builder for a {@link ConsumerFlowControlStrategyBuilderFactory}.
 *
 * @param <T> the type of {@link ConsumerFlowControlStrategy} to be built
 */
public interface ConsumerFlowControlStrategyBuilder<T extends ConsumerFlowControlStrategy> extends ConsumerBuilder.ConsumerBuilderAccessor {
    /**
     * Builds the actual FlowControlStrategy instance, for the Client with which it interoperates
     *
     * @param creditAskerSupplier {@link Supplier<CreditAsker>} for retrieving the instance (which may be lazily initialized).
     * @return {@link T} the built {@link ConsumerFlowControlStrategy}
     */
    T build(Supplier<CreditAsker> creditAskerSupplier);
}
