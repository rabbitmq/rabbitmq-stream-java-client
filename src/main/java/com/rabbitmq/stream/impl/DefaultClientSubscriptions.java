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

import com.rabbitmq.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class DefaultClientSubscriptions implements ClientSubscriptions {

    static final Duration METADATA_UPDATE_DEFAULT_INITIAL_DELAY = Duration.ofSeconds(5);
    static final Duration METADATA_UPDATE_DEFAULT_RETRY_DELAY = Duration.ofSeconds(1);
    static final Duration METADATA_UPDATE_DEFAULT_RETRY_TIMEOUT = Duration.ofSeconds(60);
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientSubscriptions.class);
    private final Random random = new Random();
    private final AtomicLong globalSubscriptionIdSequence = new AtomicLong(0);
    private final StreamEnvironment environment;
    private final Map<String, SubscriptionState> clientSubscriptionStates = new ConcurrentHashMap<>();
    private final Map<Long, StreamSubscription> streamSubscriptionRegistry = new ConcurrentHashMap<>();
    private final Function<Client.ClientParameters, Client> clientFactory;
    volatile Duration metadataUpdateInitialDelay = METADATA_UPDATE_DEFAULT_INITIAL_DELAY;
    volatile Duration metadataUpdateRetryDelay = METADATA_UPDATE_DEFAULT_RETRY_DELAY;
    volatile Duration metadataUpdateRetryTimeout = METADATA_UPDATE_DEFAULT_RETRY_TIMEOUT;


    DefaultClientSubscriptions(StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
        this.environment = environment;
        this.clientFactory = clientFactory;
    }

    DefaultClientSubscriptions(StreamEnvironment environment) {
        this(environment, cp -> new Client(cp));
    }

    private static String keyForClientSubscriptionState(Client.Broker broker) {
        // FIXME make sure this is a reasonable key for brokers
        return broker.getHost() + ":" + broker.getPort();
    }

    @Override
    public long subscribe(StreamConsumer consumer, String stream, OffsetSpecification offsetSpecification, MessageHandler messageHandler) {
        // FIXME fail immediately if there's no locator (can provide a supplier that does not retry)
        List<Client.Broker> candidates = findBrokersForStream(stream);
        Client.Broker newNode = pickBroker(candidates);

        long streamSubscriptionId = globalSubscriptionIdSequence.getAndIncrement();
        // create stream subscription to track final and changing state of this very subscription
        // we should keep this instance when we move the subscription from a client to another one
        StreamSubscription streamSubscription = new StreamSubscription(streamSubscriptionId, consumer, stream, messageHandler);

        String key = keyForClientSubscriptionState(newNode);

        SubscriptionState subscriptionState = clientSubscriptionStates.computeIfAbsent(key, s -> new SubscriptionState(environment
                .clientParametersCopy()
                .host(newNode.getHost())
                .port(newNode.getPort())
        ));

        subscriptionState.add(streamSubscription, offsetSpecification);
        streamSubscriptionRegistry.put(streamSubscription.id, streamSubscription);
        return streamSubscriptionId;
    }

    private Client locator() {
        return environment.locator();
    }

    // package protected for testing
    List<Client.Broker> findBrokersForStream(String stream) {
        // FIXME make sure locator is not null (retry)
        Map<String, Client.StreamMetadata> metadata = locator().metadata(stream);
        if (metadata.size() == 0 || metadata.get(stream) == null) {
            // this is not supposed to happen
            throw new StreamDoesNotExistException(stream);
        }

        Client.StreamMetadata streamMetadata = metadata.get(stream);
        if (!streamMetadata.isResponseOk()) {
            if (streamMetadata.getResponseCode() == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
                throw new StreamDoesNotExistException(stream);
            } else {
                throw new IllegalStateException("Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
            }
        }

        List<Client.Broker> replicas = streamMetadata.getReplicas();
        if ((replicas == null || replicas.isEmpty()) && streamMetadata.getLeader() == null) {
            throw new IllegalStateException("No node available to consume from stream " + stream);
        }

        List<Client.Broker> brokers;
        if (replicas == null || replicas.isEmpty()) {
            brokers = Collections.singletonList(streamMetadata.getLeader());
            LOGGER.debug("Consuming from {} on leader node {}", stream, streamMetadata.getLeader());
        } else {
            LOGGER.debug("Replicas for consuming from {}: {}", stream, replicas);
            brokers = new ArrayList<>(replicas);
        }

        LOGGER.debug("Candidates to consume from {}: {}", stream, brokers);

        return brokers;
    }

    private Client.Broker pickBroker(List<Client.Broker> brokers) {
        if (brokers.isEmpty()) {
            return null;
        } else if (brokers.size() == 1) {
            return brokers.get(0);
        } else {
            return brokers.get(random.nextInt(brokers.size()));
        }
    }

    @Override
    public void unsubscribe(long id) {
        // TODO consider closing the corresponding subscriptions state if there's no more subscription in it
        StreamSubscription streamSubscription = this.streamSubscriptionRegistry.get(id);
        if (streamSubscription != null) {
            streamSubscription.cancel();
        }
    }

    private static class StreamSubscription {

        private final long id;
        private final String stream;
        private final MessageHandler messageHandler;
        private final StreamConsumer consumer;
        private volatile long offset;
        private volatile int subscriptionIdInClient;
        private volatile SubscriptionState subscriptionState;

        private StreamSubscription(long id, StreamConsumer consumer, String stream, MessageHandler messageHandler) {
            this.id = id;
            this.consumer = consumer;
            this.stream = stream;
            this.messageHandler = messageHandler;
        }

        private void cancel() {
            this.subscriptionState.remove(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            StreamSubscription that = (StreamSubscription) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

    private class SubscriptionState {

        private final Client client;
        private final AtomicInteger subscriptionSequence = new AtomicInteger();
        private final Map<Integer, StreamSubscription> streamSubscriptions = new ConcurrentHashMap<>();
        private final Map<String, Set<StreamSubscription>> streamToStreamSubscriptions = new ConcurrentHashMap<>();

        private SubscriptionState(Client.ClientParameters clientParameters) {
            this.client = clientFactory.apply(clientParameters
                    .chunkListener((client, subscriptionId, offset, messageCount, dataSize) -> client.credit(subscriptionId, 1))
                    .creditNotification((subscriptionId, responseCode) -> LOGGER.debug("Received credit notification for subscription {}: {}", subscriptionId, responseCode))
                    .messageListener((subscriptionId, offset, message) -> {
                        StreamSubscription streamSubscription = streamSubscriptions.get(subscriptionId);
                        if (streamSubscription != null) {
                            streamSubscription.offset = offset;
                            streamSubscription.messageHandler.handle(offset, message);
                            // FIXME set offset here as well, best effort to avoid duplicates
                        } else {
                            LOGGER.warn("Could not find stream subscription {}", subscriptionId);
                        }
                    })
                    .metadataListener((stream, code) -> {
                        LOGGER.debug("Received metadata notification for {}, stream is likely to have become unavailable",
                                stream);
                        Set<StreamSubscription> affectedSubscriptions = streamToStreamSubscriptions.remove(stream);
                        if (affectedSubscriptions != null && !affectedSubscriptions.isEmpty()) {
                            // scheduling consumer re-assignment, to give the system some time to recover
                            environment.scheduledExecutorService().schedule(() -> {

                                if (affectedSubscriptions != null) {
                                    LOGGER.debug("Trying to move {} subscription(s) (stream {})", affectedSubscriptions.size(), stream);
                                    for (StreamSubscription affectedSubscription : affectedSubscriptions) {
                                        streamSubscriptions.remove(affectedSubscription.subscriptionIdInClient);
                                    }

                                    Runnable consumersClosingCallback = () -> {
                                        for (StreamSubscription affectedSubscription : affectedSubscriptions) {
                                            try {
                                                affectedSubscription.consumer.closeAfterStreamDeletion();
                                                streamSubscriptionRegistry.remove(affectedSubscription.id);
                                            } catch (Exception e) {
                                                LOGGER.debug("Error while closing consumer", e.getMessage());
                                            }
                                        }
                                    };

                                    AsyncRetry.asyncRetry(() -> findBrokersForStream(stream))
                                            .description("Candidate lookup to consume from " + stream)
                                            .scheduler(environment.scheduledExecutorService())
                                            .retry(ex -> !(ex instanceof StreamDoesNotExistException))
                                            .delay(metadataUpdateRetryDelay)
                                            .timeout(metadataUpdateRetryTimeout)
                                            .build()
                                            .thenAccept(candidates -> {
                                                if (candidates == null) {
                                                    consumersClosingCallback.run();
                                                } else {
                                                    for (StreamSubscription affectedSubscription : affectedSubscriptions) {
                                                        try {
                                                            Client.Broker broker = pickBroker(candidates);
                                                            LOGGER.debug("Using {} to resume consuming from {}", broker, stream);
                                                            String key = keyForClientSubscriptionState(broker);
                                                            // FIXME in case the broker is no longer there, we may have to deal with an error here
                                                            // we could renew the list of candidates for the stream
                                                            SubscriptionState subscriptionState = clientSubscriptionStates.computeIfAbsent(key, s -> new SubscriptionState(environment
                                                                    .clientParametersCopy()
                                                                    .host(broker.getHost())
                                                                    .port(broker.getPort())
                                                            ));
                                                            if (affectedSubscription.consumer.isOpen()) {
                                                                synchronized (affectedSubscription.consumer) {
                                                                    if (affectedSubscription.consumer.isOpen()) {
                                                                        subscriptionState.add(affectedSubscription, OffsetSpecification.offset(affectedSubscription.offset));
                                                                    }
                                                                }
                                                            }
                                                        } catch (Exception e) {
                                                            LOGGER.warn("Error while re-assigning subscription from stream {}", stream, e.getMessage());
                                                        }
                                                    }
                                                }
                                            }).exceptionally(ex -> {
                                        consumersClosingCallback.run();
                                        return null;
                                    });
                                }
                            }, metadataUpdateInitialDelay.toMillis(), TimeUnit.MILLISECONDS);
                        }
                    }));
        }

        void add(StreamSubscription streamSubscription, OffsetSpecification offsetSpecification) {
            int subscriptionId = subscriptionSequence.getAndIncrement();
            LOGGER.debug("Subscribing to {}", streamSubscription.stream);
            try {
                // updating data structures before subscribing
                // (to make sure they are up-to-date in case message would arrive super fast)
                streamSubscription.subscriptionIdInClient = subscriptionId;
                streamSubscription.subscriptionState = this;
                streamSubscriptions.put(subscriptionId, streamSubscription);
                streamToStreamSubscriptions.computeIfAbsent(streamSubscription.stream, s -> ConcurrentHashMap.newKeySet())
                        .add(streamSubscription);
                // FIXME consider using fewer initial credits
                Client.Response subscribeResponse = client.subscribe(subscriptionId, streamSubscription.stream, offsetSpecification, 10);
                if (!subscribeResponse.isOk()) {
                    String message = "Subscription to stream " + streamSubscription.stream + " failed with code " + subscribeResponse.getResponseCode();
                    LOGGER.debug(message);
                    throw new StreamException(message);
                }
            } catch (RuntimeException e) {
                streamSubscription.subscriptionIdInClient = -1;
                streamSubscription.subscriptionState = null;
                streamSubscriptions.remove(subscriptionId);
                streamToStreamSubscriptions.computeIfAbsent(streamSubscription.stream, s -> ConcurrentHashMap.newKeySet())
                        .remove(streamSubscription);
                throw e;
            }

            LOGGER.debug("Subscribed to {}", streamSubscription.stream);
        }

        public void remove(StreamSubscription streamSubscription) {
            // TODO consider closing this state if there's no more subscription
            int subscriptionIdInClient = streamSubscription.subscriptionIdInClient;
            Client.Response unsubscribeResponse = client.unsubscribe(subscriptionIdInClient);
            if (!unsubscribeResponse.isOk()) {
                LOGGER.warn("Unexpected response code when unsubscribing from {}: {} (subscription ID {})",
                        streamSubscription.stream, unsubscribeResponse.getResponseCode(), subscriptionIdInClient);
            }
            streamSubscriptions.remove(subscriptionIdInClient);
            streamToStreamSubscriptions.compute(streamSubscription.stream, (stream, subscriptionsForThisStream) -> {
                if (subscriptionsForThisStream == null || subscriptionsForThisStream.isEmpty()) {
                    // should not happen
                    return null;
                } else {
                    subscriptionsForThisStream.remove(streamSubscription);
                    return subscriptionsForThisStream.isEmpty() ? null : subscriptionsForThisStream;
                }
            });
        }
    }
}
