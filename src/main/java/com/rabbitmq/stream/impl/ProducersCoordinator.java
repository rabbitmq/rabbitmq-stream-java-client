// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Stream Java client library, is dual-licensed under the
// Mozilla Public License 2.0 ("MPL"), and the Apache License version 2 ("ASL").
// For the MPL, please see LICENSE-MPL-RabbitMQ. For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

package com.rabbitmq.stream.impl;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.StreamDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

class ProducersCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducersCoordinator.class);

    private final StreamEnvironment environment;
    private final Map<String, ClientProducersManager> clientProducerManagers = new ConcurrentHashMap<>();
    private final AtomicLong globalPublisherIdSequence = new AtomicLong(0);
    private final Map<Long, ProducerTracker> producerRegistry = new ConcurrentHashMap<>();
    private final Function<Client.ClientParameters, Client> clientFactory;

    ProducersCoordinator(StreamEnvironment environment) {
        this(environment, cp -> new Client(cp));
    }

    ProducersCoordinator(StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
        this.environment = environment;
        this.clientFactory = clientFactory;
        this.environment.clientParametersCopy();
    }

    Runnable registerProducer(StreamProducer producer, String stream) {
        ClientProducersManager clientForPublisher = getClient(stream);
        long globalProducerId = globalPublisherIdSequence.getAndIncrement();
        ProducerTracker producerTracker = new ProducerTracker(globalProducerId, stream, producer);
        clientForPublisher.register(producerTracker);
        producerRegistry.put(globalProducerId, producerTracker);

        return () -> {
            producerTracker.clientProducersManager.unregister(producerTracker);
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
                throw new IllegalStateException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
            }
        }

        Client.Broker leader = streamMetadata.getLeader();
        if (leader == null) {
            throw new IllegalStateException("Not leader available for stream " + stream);
        }
        LOGGER.debug("Using client on {}:{} to publish to {}", leader.getHost(), leader.getPort(), stream);

        // FIXME make sure this is a reasonable key for brokers
        String key = leader.getHost() + ":" + leader.getPort();

        return clientProducerManagers.computeIfAbsent(key, s -> new ClientProducersManager(clientFactory, environment.clientParametersCopy()
                .host(leader.getHost())
                .port(leader.getPort())));
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
        private final String stream;
        private final StreamProducer producer;
        private volatile byte publisherId;
        private volatile ClientProducersManager clientProducersManager;

        private ProducerTracker(long id, String stream, StreamProducer producer) {
            this.id = id;
            this.stream = stream;
            this.producer = producer;
        }
    }

    private class ClientProducersManager {

        private static final int MAX_PUBLISHERS_PER_CLIENT = 256;

        private final Map<Byte, ProducerTracker> producers = new ConcurrentHashMap<>(MAX_PUBLISHERS_PER_CLIENT);
        private final Map<String, Set<ProducerTracker>> streamToProducerTrackers = new ConcurrentHashMap<>();

        private final AtomicInteger producerSequence = new AtomicInteger(0);

        private final Client client;

        private ClientProducersManager(Function<Client.ClientParameters, Client> cf, Client.ClientParameters clientParameters) {
            // FIXME handle client disconnection
            // FIXME handle stream unavailability
            AtomicReference<Client> ref = new AtomicReference<>();
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
                    .shutdownListener(shutdownContext -> {
                        clientProducerManagers.values().remove(this);
                        if (shutdownContext.isShutdownUnexpected()) {
                            producers.forEach((publishingId, tracker) -> tracker.producer.unavailable());
                            // execute in thread pool to free the IO thread
                            environment.scheduledExecutorService().execute(() -> {
                                streamToProducerTrackers.entrySet().forEach(entry -> {
                                    String stream = entry.getKey();
                                    Set<ProducerTracker> trackers = entry.getValue();

                                    // FIXME deal with errors (no locator or timeout to look up a leader)
                                    // producers should be closed
                                    AsyncRetry.asyncRetry(() -> getClient(stream))
                                            .description("Candidate lookup to publish to " + stream)
                                            .scheduler(environment.scheduledExecutorService())
                                            .retry(ex -> !(ex instanceof StreamDoesNotExistException))
                                            .delayPolicy(attempt -> environment.recoveryBackOffDelayPolicy().delay(attempt))
                                            .build()
                                            .thenAccept(producersManager -> trackers.forEach(tracker -> {
                                                producersManager.register(tracker);
                                                tracker.producer.running();
                                            })).exceptionally(ex -> {
                                        for (ProducerTracker tracker : trackers) {
                                            try {
                                                tracker.producer.closeAfterStreamDeletion();
                                                producerRegistry.remove(tracker.id);
                                            } catch (Exception e) {
                                                LOGGER.debug("Error while closing producer", e.getMessage());
                                            }
                                        }
                                        return null;
                                    });
                                });
                            });
                        }
                    })
                    .clientProperty("name", "rabbitmq-stream-producer"));
            ref.set(this.client);
        }

        private synchronized void register(ProducerTracker producerTracker) {
            producerTracker.publisherId = (byte) producerSequence.incrementAndGet();
            producerTracker.producer.setPublisherId(producerTracker.publisherId);
            producerTracker.producer.setClient(this.client);
            producerTracker.clientProducersManager = this;
            producers.put(producerTracker.publisherId, producerTracker);
            streamToProducerTrackers.computeIfAbsent(producerTracker.stream, s -> ConcurrentHashMap.newKeySet())
                    .add(producerTracker);
        }

        private synchronized void unregister(ProducerTracker producerTracker) {
            producers.remove(producerTracker.publisherId);
            streamToProducerTrackers.compute(producerTracker.stream, (s, producerTrackersForThisStream) -> {
                if (s == null || producerTrackersForThisStream == null) {
                    // should not happen
                    return null;
                } else {
                    producerTrackersForThisStream.remove(producerTracker);
                    return producerTrackersForThisStream.isEmpty() ? null : producerTrackersForThisStream;
                }
            });
        }

        // FIXME close client

    }

}
