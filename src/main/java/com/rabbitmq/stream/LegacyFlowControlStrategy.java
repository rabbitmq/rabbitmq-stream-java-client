package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The flow control strategy that was always applied before the flow control strategy mechanism existed in the codebase.
 * Requests a set amount of credits after each chunk arrives.
 */
public class LegacyFlowControlStrategy extends AbstractFlowControlStrategy {

    private final int initialCredits;
    private final int additionalCredits;

    public LegacyFlowControlStrategy(Supplier<Client> clientSupplier) {
        this(clientSupplier, 1);
    }

    public LegacyFlowControlStrategy(Supplier<Client> clientSupplier, int initialCredits) {
        this(clientSupplier, initialCredits, 1);
    }

    public LegacyFlowControlStrategy(Supplier<Client> clientSupplier, int initialCredits, int additionalCredits) {
        super(clientSupplier);
        this.initialCredits = initialCredits;
        this.additionalCredits = additionalCredits;
    }

    @Override
    public int handleSubscribe(
            byte subscriptionId,
            String stream,
            OffsetSpecification offsetSpecification,
            Map<String, String> subscriptionProperties
    ) {
        return this.initialCredits;
    }

    @Override
    public void handleChunk(byte subscriptionId, long offset, long messageCount, long dataSize) {
        mandatoryClient().credit(subscriptionId, this.additionalCredits);
    }

}
