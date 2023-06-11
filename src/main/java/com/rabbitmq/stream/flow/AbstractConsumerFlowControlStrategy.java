package com.rabbitmq.stream.flow;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Abstract class for Consumer Flow Control Strategies which keeps a cached lazily-initialized
 * {@link CreditAsker} ready for retrieval by its inheritors.
 */
public abstract class AbstractConsumerFlowControlStrategy implements ConsumerFlowControlStrategy {

    private final Supplier<CreditAsker> creditAskerSupplier;
    private volatile CreditAsker creditAsker;


    protected AbstractConsumerFlowControlStrategy(Supplier<CreditAsker> creditAskerSupplier) {
        this.creditAskerSupplier = Objects.requireNonNull(creditAskerSupplier, "creditAskerSupplier");
    }

    protected CreditAsker mandatoryCreditAsker() {
        CreditAsker localSupplied = this.creditAsker;
        if(localSupplied != null) {
            return localSupplied;
        }
        localSupplied = creditAskerSupplier.get();
        if(localSupplied == null) {
            throw new IllegalStateException("Requested client, but client is not yet available! Supplier: " + this.creditAskerSupplier);
        }
        this.creditAsker = localSupplied;
        return localSupplied;
    }

}
