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

class DefaultClientSubscriptions implements ClientSubscriptions {

    static final Duration DELAY_AFTER_METADATA_UPDATE = Duration.ofSeconds(5);
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientSubscriptions.class);
    private final Random random = new Random();
    private final AtomicLong globalSubscriptionIdSequence = new AtomicLong(0);
    private final StreamEnvironment environment;
    private final Map<String, SubscriptionState> clientSubscriptionStates = new ConcurrentHashMap<>();
    private final Map<Long, StreamSubscription> streamSubscriptionRegistry = new ConcurrentHashMap<>();

    DefaultClientSubscriptions(StreamEnvironment environment) {
        this.environment = environment;
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
            this.client = new Client(clientParameters
                    .chunkListener((client, subscriptionId, offset, messageCount, dataSize) -> client.credit(subscriptionId, 1))
                    .creditNotification((subscriptionId, responseCode) -> LOGGER.debug("Received notification for subscription {}: {}", subscriptionId, responseCode))
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

                                    // choose new node
                                    List<Client.Broker> candidates = null;
                                    Duration delayInterval = Duration.ofSeconds(1);
                                    Duration timeout = Duration.ofSeconds(60);
                                    int waited = 0;
                                    while (waited < timeout.toMillis()) {
                                        try {
                                            // FIXME keep trying if there's no locator (can provide a supplier that does retry)
                                            candidates = findBrokersForStream(stream);
                                        } catch (StreamDoesNotExistException e) {
                                            consumersClosingCallback.run();
                                            return;
                                        } catch (Exception e) {
                                            LOGGER.debug("Error while looking up candidate nodes to consume from {}: {}",
                                                    stream, e.getMessage());
                                        } finally {
                                            if (candidates == null || candidates.isEmpty()) {
                                                try {
                                                    Thread.sleep(delayInterval.toMillis());
                                                    waited += delayInterval.toMillis();
                                                } catch (InterruptedException e) {
                                                    Thread.currentThread().interrupt();
                                                    return;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    }

                                    if (candidates == null || candidates.isEmpty()) {
                                        consumersClosingCallback.run();
                                    } else {
                                        for (StreamSubscription affectedSubscription : affectedSubscriptions) {
                                            Client.Broker broker = pickBroker(candidates);
                                            LOGGER.debug("Using {} to resume consuming from {}", broker, stream);
                                            String key = keyForClientSubscriptionState(broker);
                                            // FIXME in case the broker is no longer there, we may have to deal with an error here
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
                                        }
                                    }

                                }
                            }, DELAY_AFTER_METADATA_UPDATE.toMillis(), TimeUnit.MILLISECONDS);
                        }

                    }));
        }

        void add(StreamSubscription streamSubscription, OffsetSpecification offsetSpecification) {
            int subscriptionId = subscriptionSequence.getAndIncrement();
            LOGGER.debug("Subscribing to {}", streamSubscription.stream);
            try {
                Client.Response subscribeResponse = client.subscribe(subscriptionId, streamSubscription.stream, offsetSpecification, 10);
                if (!subscribeResponse.isOk()) {
                    String message = "Subscription to stream " + streamSubscription.stream + " failed with code " + subscribeResponse.getResponseCode();
                    LOGGER.debug(message);
                    throw new StreamException(message);
                }
            } catch (RuntimeException e) {
                e.printStackTrace();
                throw e;
            }

            streamSubscription.subscriptionIdInClient = subscriptionId;
            streamSubscription.subscriptionState = this;
            streamSubscriptions.put(subscriptionId, streamSubscription);
            streamToStreamSubscriptions.computeIfAbsent(streamSubscription.stream, s -> ConcurrentHashMap.newKeySet())
                    .add(streamSubscription);
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
