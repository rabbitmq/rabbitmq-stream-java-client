package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class ProducersCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);

    private final StreamEnvironment environment;
    private final Map<String, ClientProducersManager> clientProducerManagers = new ConcurrentHashMap<>();
    private final AtomicLong globalPublisherIdSequence = new AtomicLong(0);
    private final Map<Long, ProducerTracker> producerRegistry = new ConcurrentHashMap<>();
    private final Function<Client.ClientParameters, Client> clientFactory;
    private final Client.ClientParameters clientParametersPrototype;

    ProducersCoordinator(StreamEnvironment environment) {
        this(environment, cp -> new Client(cp));
    }

    ProducersCoordinator(StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
        this.environment = environment;
        this.clientFactory = clientFactory;
        this.clientParametersPrototype = this.environment.clientParametersCopy();
    }

    Runnable registerProducer(StreamProducer producer, String stream) {
        ClientProducersManager clientForPublisher = getClient(stream);
        long globalProducerId = globalPublisherIdSequence.getAndIncrement();
        ProducerTracker producerState = new ProducerTracker(globalProducerId, producer);
        clientForPublisher.register(producerState);
        producerRegistry.put(globalProducerId, producerState);

        return () -> {
            producerState.clientProducersManager.unregister(producerState);
            producerRegistry.remove(globalProducerId);
        };
    }

    private ClientProducersManager getClient(String stream) {
        Map<String, Client.StreamMetadata> metadata = this.environment.locator().metadata(stream);
        if (metadata.size() == 0 || metadata.get(stream) == null) {
            throw new StreamDoesNotExistException("Stream does not exist: " + stream);
        }

        Client.StreamMetadata streamMetadata = metadata.get(stream);
        if (!streamMetadata.isResponseOk()) {
            if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
                throw new StreamDoesNotExistException("Stream does not exist: " + stream);
            } else {
                throw new IllegalArgumentException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
            }
        }

        Client.Broker leader = streamMetadata.getLeader();
        if (leader == null) {
            throw new IllegalStateException("Not leader available for stream " + stream);
        }
        LOGGER.debug("Using client on {}:{} to publish to {}", leader.getHost(), leader.getPort(), stream);

        // FIXME make sure this is a reasonable key for brokers
        String key = leader.getHost() + ":" + leader.getPort();

        return clientProducerManagers.computeIfAbsent(key, s -> {
            // FIXME add shutdown listener to client for publisher
            // this should notify the affected publishers/consumers
            return new ClientProducersManager(clientFactory, clientParametersPrototype.duplicate()
                    .host(leader.getHost())
                    .port(leader.getPort()));
        });
    }

    void close() {
        for (ClientProducersManager clientProducersManager : clientProducerManagers.values()) {
            try {
                clientProducersManager.client.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing producer connection: {}", e.getMessage());
            }
        }
    }

    private static class ProducerTracker {

        private final long id;
        private final StreamProducer producer;
        private volatile byte publisherId;
        private volatile ClientProducersManager clientProducersManager;

        private ProducerTracker(long id, StreamProducer producer) {
            this.id = id;
            this.producer = producer;
        }
    }

    private static class ClientProducersManager {

        private static final int MAX_PUBLISHERS_PER_CLIENT = 256;

        private final Map<Byte, ProducerTracker> producers = new ConcurrentHashMap<>(MAX_PUBLISHERS_PER_CLIENT);

        private final AtomicInteger producerSequence = new AtomicInteger(0);

        private final Client client;

        private ClientProducersManager(Function<Client.ClientParameters, Client> cf, Client.ClientParameters clientParameters) {
            // FIXME handle client disconnection
            // FIXME handle stream unavailability
            this.client = cf.apply(clientParameters
                    .publishConfirmListener((publisherId, publishingId) -> {
                        ProducerTracker producerTracker = producers.get(publisherId);
                        if (producerTracker == null) {
                            LOGGER.warn("Received publish confirm for unknown producer: {}");
                        } else {
                            producerTracker.producer.confirm(publishingId);
                        }
                    })
                    .publishErrorListener((publisherId, publishingId, errorCode) -> {
                        ProducerTracker producerTracker = producers.get(publisherId);
                        if (producerTracker == null) {
                            LOGGER.warn("Received publish error for unknown producer: {}");
                        } else {
                            producerTracker.producer.error(publishingId, errorCode);
                        }
                    })
                    .clientProperty("name", "rabbitmq-stream-producer"));
        }

        private void register(ProducerTracker producerTracker) {
            producerTracker.publisherId = (byte) producerSequence.incrementAndGet();
            producerTracker.producer.publisherId = producerTracker.publisherId;
            producerTracker.producer.client = this.client;
            producerTracker.clientProducersManager = this;
            producers.put(producerTracker.publisherId, producerTracker);
        }

        private void unregister(ProducerTracker producerTracker) {
            producers.remove(producerTracker.publisherId);
        }

    }

}
