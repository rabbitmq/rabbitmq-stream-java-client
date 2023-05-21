package com.rabbitmq.stream;

import com.rabbitmq.stream.impl.Client;

import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractFlowControlStrategy implements ConsumerFlowControlStrategy {

    private final Supplier<Client> clientSupplier;
    private volatile Client client;

    protected AbstractFlowControlStrategy(Supplier<Client> clientSupplier) {
        this.clientSupplier = Objects.requireNonNull(clientSupplier, "clientSupplier");
    }

    protected Client mandatoryClient() {
        Client localClient = this.client;
        if(localClient != null) {
            return localClient;
        }
        localClient = clientSupplier.get();
        if(localClient == null) {
            throw new IllegalStateException("Requested client, but client is not yet available! Supplier: " + this.clientSupplier);
        }
        this.client = localClient;
        return localClient;
    }

}
