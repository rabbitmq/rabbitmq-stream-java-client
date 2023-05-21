package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

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
     * @param clientSupplier {@link Supplier <Client>} for retrieving the {@link Client}.
     *                       Is not a {@link Client} instance because the {@link Client} may be lazily initialized.
     * @return the FlowControlStrategy
     */
    T build(Supplier<Client> clientSupplier);
}
