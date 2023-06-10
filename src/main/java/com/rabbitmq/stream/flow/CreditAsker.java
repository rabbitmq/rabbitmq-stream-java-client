package com.rabbitmq.stream.flow;

public interface CreditAsker {

    /**
     * Asks for credits for a given subscription.
     * @param subscriptionId the subscription ID
     * @param credit how many credits to ask for
     * @throws IllegalArgumentException if credits are below 0 or above {@link Short#MAX_VALUE}
     */
    void credit(byte subscriptionId, int credit);

}
