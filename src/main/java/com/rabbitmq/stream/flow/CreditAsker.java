package com.rabbitmq.stream.flow;

@FunctionalInterface
public interface CreditAsker {

    /**
     * Asks for credits for a given subscription.
     * @param credits How many credits to ask for
     * @throws IllegalArgumentException If credits are below 0 or above {@link Short#MAX_VALUE}
     */
    void credit(int credits);

}
