package com.rabbitmq.stream.flow;

import java.util.Objects;

/**
 * Abstract class for Consumer Flow Control Strategies which keeps a
 * {@link CreditAsker creditAsker} field ready for retrieval by its inheritors.
 */
public abstract class AbstractConsumerFlowControlStrategy implements ConsumerFlowControlStrategy {

    private final String identifier;
    private final CreditAsker creditAsker;

    protected AbstractConsumerFlowControlStrategy(String identifier, CreditAsker creditAsker) {
        this.identifier = identifier;
        this.creditAsker = Objects.requireNonNull(creditAsker, "creditAsker");
    }

    public CreditAsker getCreditAsker() {
        return creditAsker;
    }

    public String getIdentifier() {
        return identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AbstractConsumerFlowControlStrategy that = (AbstractConsumerFlowControlStrategy) o;
        return Objects.equals(identifier, that.identifier) && Objects.equals(creditAsker, that.creditAsker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(identifier, creditAsker);
    }

    @Override
    public String toString() {
        return "AbstractConsumerFlowControlStrategy{" +
                "identifier='" + identifier + '\'' +
                ", creditAsker=" + creditAsker +
                '}';
    }
}
