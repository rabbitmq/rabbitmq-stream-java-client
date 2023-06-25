package com.rabbitmq.stream.flow;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Abstract class for Consumer Flow Control Strategies which keeps a cached lazily-initialized
 * {@link CreditAsker} ready for retrieval by its inheritors.
 */
public abstract class AbstractConsumerFlowControlStrategy implements ConsumerFlowControlStrategy {

    private final Supplier<CreditAsker> creditAskerSupplier;
    private volatile CreditAsker lastRetrievedCreditAsker;

    protected AbstractConsumerFlowControlStrategy(Supplier<CreditAsker> creditAskerSupplier) {
        this.creditAskerSupplier = Objects.requireNonNull(creditAskerSupplier, "creditAskerSupplier");
    }

    protected CreditAsker mandatoryCreditAsker() {
        CreditAsker localSupplied = creditAskerSupplier.get();
        if(localSupplied == null) {
            throw new IllegalStateException("Requested CreditAsker but it's not yet available! Supplier: " + this.creditAskerSupplier);
        }
        this.lastRetrievedCreditAsker = localSupplied;
        return localSupplied;
    }

    public CreditAsker getLastRetrievedCreditAsker() {
        return lastRetrievedCreditAsker;
    }

    public Supplier<CreditAsker> getCreditAskerSupplier() {
        return creditAskerSupplier;
    }

    @Override
    public String toString() {
        return "AbstractConsumerFlowControlStrategy{" +
                "creditAskerSupplier=" + creditAskerSupplier +
                ", lastRetrievedCreditAsker=" + lastRetrievedCreditAsker +
                '}';
    }

}
