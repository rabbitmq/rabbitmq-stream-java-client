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

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumersCoordinator {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumersCoordinator.class);
  private final Random random = new Random();
  private final AtomicLong globalSubscriptionIdSequence = new AtomicLong(0);
  private final StreamEnvironment environment;
  private final Map<String, ManagerPool> pools = new ConcurrentHashMap<>();
  private final Map<Long, SubscriptionTracker> subscriptionTrackerRegistry =
      new ConcurrentHashMap<>();
  private final Function<Client.ClientParameters, Client> clientFactory;

  ConsumersCoordinator(
      StreamEnvironment environment, Function<Client.ClientParameters, Client> clientFactory) {
    this.environment = environment;
    this.clientFactory = clientFactory;
  }

  ConsumersCoordinator(StreamEnvironment environment) {
    this(environment, cp -> new Client(cp));
  }

  private static String keyForClientSubscription(Client.Broker broker) {
    // FIXME make sure this is a reasonable key for brokers
    return broker.getHost() + ":" + broker.getPort();
  }

  private BackOffDelayPolicy metadataUpdateBackOffDelayPolicy() {
    return environment.topologyUpdateBackOffDelayPolicy();
  }

  public long subscribe(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      MessageHandler messageHandler) {
    // FIXME fail immediately if there's no locator (can provide a supplier that does not retry)
    List<Client.Broker> candidates = findBrokersForStream(stream);
    Client.Broker newNode = pickBroker(candidates);

    long streamSubscriptionId = globalSubscriptionIdSequence.getAndIncrement();
    // create stream subscription to track final and changing state of this very subscription
    // we keep this instance when we move the subscription from a client to another one
    SubscriptionTracker subscriptionTracker =
        new SubscriptionTracker(streamSubscriptionId, consumer, stream, messageHandler);

    String key = keyForClientSubscription(newNode);

    ManagerPool managerPool =
        pools.computeIfAbsent(
            key,
            s ->
                new ManagerPool(
                    key,
                    environment
                        .clientParametersCopy()
                        .host(newNode.getHost())
                        .port(newNode.getPort())));

    managerPool.add(subscriptionTracker, offsetSpecification);
    subscriptionTrackerRegistry.put(subscriptionTracker.id, subscriptionTracker);

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
        throw new IllegalStateException(
            "Could not get stream metadata, response code: " + streamMetadata.getResponseCode());
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

  public void unsubscribe(long id) {
    SubscriptionTracker subscriptionTracker = this.subscriptionTrackerRegistry.remove(id);
    if (subscriptionTracker != null) {
      subscriptionTracker.cancel();
    }
  }

  public void close() {
    for (ManagerPool subscriptionPool : this.pools.values()) {
      subscriptionPool.close();
    }
  }

  /**
   * Data structure that keeps track of a given {@link StreamConsumer} and its message callback.
   *
   * <p>An instance is "moved" between {@link ClientSubscriptionsManager} instances on stream
   * failure or on disconnection.
   */
  private static class SubscriptionTracker {

    private final long id;
    private final String stream;
    private final MessageHandler messageHandler;
    private final StreamConsumer consumer;
    private volatile long offset;
    private volatile byte subscriptionIdInClient;
    private volatile ClientSubscriptionsManager clientSubscriptionsManager;

    private SubscriptionTracker(
        long id, StreamConsumer consumer, String stream, MessageHandler messageHandler) {
      this.id = id;
      this.consumer = consumer;
      this.stream = stream;
      this.messageHandler = messageHandler;
    }

    private void cancel() {
      this.clientSubscriptionsManager.remove(this);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubscriptionTracker that = (SubscriptionTracker) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  /**
   * Maintains {@link ClientSubscriptionsManager} instances for a given host.
   *
   * <p>Creates new {@link ClientSubscriptionsManager} instances (and so {@link Client}s, i.e.
   * connections) when needed and disposes them when appropriate.
   */
  private class ManagerPool {

    private final List<ClientSubscriptionsManager> managers = new CopyOnWriteArrayList<>();
    private final String name;
    private final Client.ClientParameters clientParameters;

    private ManagerPool(String name, Client.ClientParameters clientParameters) {
      this.name = name;
      this.clientParameters = clientParameters;
      LOGGER.debug("Creating client subscription pool on {}", name);
      managers.add(new ClientSubscriptionsManager(this, clientParameters));
    }

    private synchronized void add(
        SubscriptionTracker subscriptionTracker, OffsetSpecification offsetSpecification) {
      boolean added = false;
      // FIXME deal with manager unavailability (manager may be closing because of connection
      // closing)
      // try all of them until it succeeds, throw exception if failure
      for (ClientSubscriptionsManager manager : managers) {
        if (!manager.isFull()) {
          manager.add(subscriptionTracker, offsetSpecification);
          added = true;
          break;
        }
      }
      if (!added) {
        LOGGER.debug(
            "Creating subscription manager on {}, this is subscription manager #{}",
            name,
            managers.size() + 1);
        ClientSubscriptionsManager manager = new ClientSubscriptionsManager(this, clientParameters);
        managers.add(manager);
        manager.add(subscriptionTracker, offsetSpecification);
      }
    }

    private synchronized void maybeDisposeManager(
        ClientSubscriptionsManager clientSubscriptionsManager) {
      if (clientSubscriptionsManager.isEmpty()) {
        clientSubscriptionsManager.close();
        this.remove(clientSubscriptionsManager);
      }
    }

    private synchronized void remove(ClientSubscriptionsManager clientSubscriptionsManager) {
      managers.remove(clientSubscriptionsManager);
      if (managers.isEmpty()) {
        pools.remove(name);
        LOGGER.debug("Disposed client subscription pool on {} because it was empty", name);
      }
    }

    synchronized void close() {
      Iterator<ClientSubscriptionsManager> iterator = managers.iterator();
      while (iterator.hasNext()) {
        ClientSubscriptionsManager manager = iterator.next();
        manager.close();
      }
      managers.clear();
    }
  }

  /**
   * Maintains a set of {@link SubscriptionTracker} instances on a {@link Client}.
   *
   * <p>It dispatches inbound messages to the appropriate {@link SubscriptionTracker} and
   * re-allocates {@link SubscriptionTracker}s in case of stream unavailability or disconnection.
   */
  private class ClientSubscriptionsManager {

    private final Client client;
    // the 3 following data structures track the subscriptions, they must remain consistent
    private final Map<String, Set<SubscriptionTracker>> streamToStreamSubscriptions =
        new ConcurrentHashMap<>();
    private final Set<Long> globalStreamSubscriptionIds = ConcurrentHashMap.newKeySet();
    private final ManagerPool owner;
    private volatile List<SubscriptionTracker> subscriptionTrackers =
        new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);

    private ClientSubscriptionsManager(
        ManagerPool owner, Client.ClientParameters clientParameters) {
      this.owner = owner;
      String name = owner.name;
      LOGGER.debug("creating subscription manager on {}", name);
      IntStream.range(0, MAX_SUBSCRIPTIONS_PER_CLIENT).forEach(i -> subscriptionTrackers.add(null));
      this.client =
          clientFactory.apply(
              clientParameters
                  .clientProperty("name", "rabbitmq-stream-consumer")
                  .chunkListener(
                      (client, subscriptionId, offset, messageCount, dataSize) ->
                          client.credit(subscriptionId, 1))
                  .creditNotification(
                      (subscriptionId, responseCode) ->
                          LOGGER.debug(
                              "Received credit notification for subscription {}: {}",
                              subscriptionId,
                              responseCode))
                  .messageListener(
                      (subscriptionId, offset, message) -> {
                        SubscriptionTracker subscriptionTracker =
                            subscriptionTrackers.get(subscriptionId & 0xFF);
                        if (subscriptionTracker != null) {
                          subscriptionTracker.offset = offset;
                          subscriptionTracker.messageHandler.handle(offset, message);
                          // FIXME set offset here as well, best effort to avoid duplicates
                        } else {
                          LOGGER.debug("Could not find stream subscription {}", subscriptionId);
                        }
                      })
                  .shutdownListener(
                      shutdownContext -> {
                        // FIXME should the pool check if it's empty and so remove itself from the
                        // pools data structure?
                        owner.remove(this);
                        if (shutdownContext.isShutdownUnexpected()) {
                          LOGGER.debug(
                              "Unexpected shutdown notification on subscription client {}, scheduling consumers re-assignment",
                              name);
                          environment
                              .scheduledExecutorService()
                              .execute(
                                  () -> {
                                    for (Map.Entry<String, Set<SubscriptionTracker>> entry :
                                        streamToStreamSubscriptions.entrySet()) {
                                      String stream = entry.getKey();
                                      LOGGER.debug(
                                          "Re-assigning {} consumer(s) to stream {} after disconnection",
                                          entry.getValue().size(),
                                          stream);
                                      assignConsumersToStream(
                                          entry.getValue(),
                                          stream,
                                          attempt ->
                                              environment
                                                  .recoveryBackOffDelayPolicy()
                                                  .delay(attempt),
                                          false);
                                    }
                                  });
                        }
                      })
                  .metadataListener(
                      (stream, code) -> {
                        LOGGER.debug(
                            "Received metadata notification for {}, stream is likely to have become unavailable",
                            stream);

                        Set<SubscriptionTracker> affectedSubscriptions;
                        synchronized (ClientSubscriptionsManager.this) {
                          Set<SubscriptionTracker> subscriptions =
                              streamToStreamSubscriptions.remove(stream);
                          if (subscriptions != null && !subscriptions.isEmpty()) {
                            List<SubscriptionTracker> newSubscriptions =
                                new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
                            for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                              newSubscriptions.add(subscriptionTrackers.get(i));
                            }
                            for (SubscriptionTracker subscription : subscriptions) {
                              newSubscriptions.set(
                                  subscription.subscriptionIdInClient & 0xFF, null);
                              globalStreamSubscriptionIds.remove(subscription.id);
                            }
                            this.subscriptionTrackers = newSubscriptions;
                          }
                          affectedSubscriptions = subscriptions;
                        }
                        if (isEmpty()) {
                          this.owner.remove(this);
                        }
                        if (affectedSubscriptions != null && !affectedSubscriptions.isEmpty()) {
                          environment
                              .scheduledExecutorService()
                              .execute(
                                  () -> {
                                    LOGGER.debug(
                                        "Trying to move {} subscription(s) (stream {})",
                                        affectedSubscriptions.size(),
                                        stream);
                                    assignConsumersToStream(
                                        affectedSubscriptions,
                                        stream,
                                        attempt ->
                                            metadataUpdateBackOffDelayPolicy().delay(attempt),
                                        isEmpty());
                                  });
                        }
                      }));
    }

    private void assignConsumersToStream(
        Collection<SubscriptionTracker> subscriptions,
        String stream,
        BackOffDelayPolicy delayPolicy,
        boolean closeClient) {
      Runnable consumersClosingCallback =
          () -> {
            for (SubscriptionTracker affectedSubscription : subscriptions) {
              try {
                affectedSubscription.consumer.closeAfterStreamDeletion();
                subscriptionTrackerRegistry.remove(affectedSubscription.id);
              } catch (Exception e) {
                LOGGER.debug("Error while closing consumer", e.getMessage());
              }
            }
          };

      AsyncRetry.asyncRetry(() -> findBrokersForStream(stream))
          .description("Candidate lookup to consume from " + stream)
          .scheduler(environment.scheduledExecutorService())
          .retry(ex -> !(ex instanceof StreamDoesNotExistException))
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(
              candidates -> {
                if (candidates == null) {
                  consumersClosingCallback.run();
                } else {
                  for (SubscriptionTracker affectedSubscription : subscriptions) {
                    try {
                      Client.Broker broker = pickBroker(candidates);
                      LOGGER.debug("Using {} to resume consuming from {}", broker, stream);
                      String key = keyForClientSubscription(broker);
                      // FIXME in case the broker is no longer there, we may have to deal with an
                      // error here
                      // we could renew the list of candidates for the stream
                      ManagerPool subscriptionPool =
                          pools.computeIfAbsent(
                              key,
                              s ->
                                  new ManagerPool(
                                      key,
                                      environment
                                          .clientParametersCopy()
                                          .host(broker.getHost())
                                          .port(broker.getPort())));
                      if (affectedSubscription.consumer.isOpen()) {
                        synchronized (affectedSubscription.consumer) {
                          if (affectedSubscription.consumer.isOpen()) {
                            subscriptionPool.add(
                                affectedSubscription,
                                OffsetSpecification.offset(affectedSubscription.offset));
                          }
                        }
                      }
                    } catch (Exception e) {
                      LOGGER.warn(
                          "Error while re-assigning subscription from stream {}",
                          stream,
                          e.getMessage());
                    }
                  }
                  if (closeClient) {
                    this.close();
                  }
                }
              })
          .exceptionally(
              ex -> {
                consumersClosingCallback.run();
                return null;
              });
    }

    synchronized void add(
        SubscriptionTracker subscriptionTracker, OffsetSpecification offsetSpecification) {
      // FIXME check manager is still open (not closed because of connection failure)
      byte subscriptionId = 0;
      for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
        if (subscriptionTrackers.get(i) == null) {
          subscriptionId = (byte) i;
          break;
        }
      }

      List<SubscriptionTracker> previousSubscriptions = this.subscriptionTrackers;

      LOGGER.debug("Subscribing to {}", subscriptionTracker.stream);
      try {
        // updating data structures before subscribing
        // (to make sure they are up-to-date in case message would arrive super fast)
        subscriptionTracker.subscriptionIdInClient = subscriptionId;
        subscriptionTracker.clientSubscriptionsManager = this;
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .add(subscriptionTracker);
        globalStreamSubscriptionIds.add(subscriptionTracker.id);
        this.subscriptionTrackers =
            update(previousSubscriptions, subscriptionId, subscriptionTracker);
        // FIXME consider using fewer initial credits
        Client.Response subscribeResponse =
            client.subscribe(subscriptionId, subscriptionTracker.stream, offsetSpecification, 10);
        if (!subscribeResponse.isOk()) {
          String message =
              "Subscription to stream "
                  + subscriptionTracker.stream
                  + " failed with code "
                  + subscribeResponse.getResponseCode();
          LOGGER.debug(message);
          throw new StreamException(message);
        }
      } catch (RuntimeException e) {
        subscriptionTracker.subscriptionIdInClient = -1;
        subscriptionTracker.clientSubscriptionsManager = null;
        this.subscriptionTrackers = previousSubscriptions;
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .remove(subscriptionTracker);
        globalStreamSubscriptionIds.remove(subscriptionTracker.id);
        throw e;
      }

      LOGGER.debug("Subscribed to {}", subscriptionTracker.stream);
    }

    synchronized void remove(SubscriptionTracker subscriptionTracker) {
      // FIXME check manager is still open (not closed because of connection failure)
      byte subscriptionIdInClient = subscriptionTracker.subscriptionIdInClient;
      Client.Response unsubscribeResponse = client.unsubscribe(subscriptionIdInClient);
      if (!unsubscribeResponse.isOk()) {
        LOGGER.warn(
            "Unexpected response code when unsubscribing from {}: {} (subscription ID {})",
            subscriptionTracker.stream,
            unsubscribeResponse.getResponseCode(),
            subscriptionIdInClient);
      }
      this.subscriptionTrackers = update(this.subscriptionTrackers, subscriptionIdInClient, null);
      globalStreamSubscriptionIds.remove(subscriptionTracker.id);
      streamToStreamSubscriptions.compute(
          subscriptionTracker.stream,
          (stream, subscriptionsForThisStream) -> {
            if (subscriptionsForThisStream == null || subscriptionsForThisStream.isEmpty()) {
              // should not happen
              return null;
            } else {
              subscriptionsForThisStream.remove(subscriptionTracker);
              return subscriptionsForThisStream.isEmpty() ? null : subscriptionsForThisStream;
            }
          });
      this.owner.maybeDisposeManager(this);
    }

    private List<SubscriptionTracker> update(
        List<SubscriptionTracker> original, byte index, SubscriptionTracker newValue) {
      List<SubscriptionTracker> newSubcriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
      int intIndex = index & 0xFF;
      for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
        newSubcriptions.add(i == intIndex ? newValue : original.get(i));
      }
      return newSubcriptions;
    }

    boolean isFull() {
      return this.globalStreamSubscriptionIds.size() == MAX_SUBSCRIPTIONS_PER_CLIENT;
    }

    boolean isEmpty() {
      return this.globalStreamSubscriptionIds.isEmpty();
    }

    void close() {
      // FIXME consider cancelling subscriptions, this would avoid receiving messages interleaved
      // with the closing response
      // or make this on the server side, as soon as the close command is received
      if (this.client.isOpen()) {
        this.client.close();
      }
    }
  }

  int poolSize() {
    return pools.size();
  }

  @Override
  public String toString() {
    return ("[ \n"
            + pools.entrySet().stream()
                .map(
                    poolEntry ->
                        "  { 'broker' : '"
                            + poolEntry.getKey()
                            + "', 'clients' : [ "
                            + poolEntry.getValue().managers.stream()
                                .map(
                                    manager ->
                                        "{ 'consumer_count' : "
                                            + manager.subscriptionTrackers.stream()
                                                .filter(tracker -> tracker != null)
                                                .count()
                                            + " }")
                                .collect(Collectors.joining(", "))
                            + " ] }")
                .collect(Collectors.joining(", \n"))
            + "\n]")
        .replace("'", "\"");
  }
}
