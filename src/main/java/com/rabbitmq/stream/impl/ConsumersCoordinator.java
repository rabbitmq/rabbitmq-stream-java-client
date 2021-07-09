// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.Utils.formatConstant;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.impl.Client.ChunkListener;
import com.rabbitmq.stream.impl.Client.CreditNotification;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.MetadataListener;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumersCoordinator {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;

  private static final OffsetSpecification DEFAULT_OFFSET_SPECIFICATION =
      OffsetSpecification.next();

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumersCoordinator.class);
  private final Random random = new Random();
  private final StreamEnvironment environment;
  private final Map<String, ManagerPool> pools = new ConcurrentHashMap<>();
  private final ClientFactory clientFactory;
  private final int maxConsumersByConnection;

  ConsumersCoordinator(
      StreamEnvironment environment, int maxConsumersByConnection, ClientFactory clientFactory) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxConsumersByConnection = maxConsumersByConnection;
  }

  private static String keyForClientSubscription(Client.Broker broker) {
    // FIXME make sure this is a reasonable key for brokers
    return broker.getHost() + ":" + broker.getPort();
  }

  private BackOffDelayPolicy metadataUpdateBackOffDelayPolicy() {
    return environment.topologyUpdateBackOffDelayPolicy();
  }

  Runnable subscribe(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      String trackingReference,
      MessageHandler messageHandler) {
    // FIXME fail immediately if there's no locator (can provide a supplier that does not retry)
    List<Client.Broker> candidates = findBrokersForStream(stream);
    Client.Broker newNode = pickBroker(candidates);
    if (newNode == null) {
      throw new IllegalStateException("No available node to subscribe to");
    }

    // create stream subscription to track final and changing state of this very subscription
    // we keep this instance when we move the subscription from a client to another one
    SubscriptionTracker subscriptionTracker =
        new SubscriptionTracker(consumer, stream, trackingReference, messageHandler);

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

    managerPool.add(subscriptionTracker, offsetSpecification, true);

    return subscriptionTracker::cancel;
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
            "Could not get stream metadata, response code: "
                + formatConstant(streamMetadata.getResponseCode()));
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

  public void close() {
    for (ManagerPool subscriptionPool : this.pools.values()) {
      subscriptionPool.close();
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
                                                .filter(Objects::nonNull)
                                                .count()
                                            + " }")
                                .collect(Collectors.joining(", "))
                            + " ] }")
                .collect(Collectors.joining(", \n"))
            + "\n]")
        .replace("'", "\"");
  }

  /**
   * Data structure that keeps track of a given {@link StreamConsumer} and its message callback.
   *
   * <p>An instance is "moved" between {@link ClientSubscriptionsManager} instances on stream
   * failure or on disconnection.
   */
  private static class SubscriptionTracker {

    private final String stream;
    private final String offsetTrackingReference;
    private final MessageHandler messageHandler;
    private final StreamConsumer consumer;
    private volatile long offset;
    private volatile byte subscriptionIdInClient;
    private volatile ClientSubscriptionsManager manager;
    private volatile boolean closing = false;

    private SubscriptionTracker(
        StreamConsumer consumer,
        String stream,
        String offsetTrackingReference,
        MessageHandler messageHandler) {
      this.consumer = consumer;
      this.stream = stream;
      this.offsetTrackingReference = offsetTrackingReference;
      this.messageHandler = messageHandler;
    }

    synchronized void cancel() {
      this.closing = true;
      if (this.manager != null) {
        LOGGER.debug("Removing consumer from manager");
        this.manager.remove(this);
      } else {
        LOGGER.debug("No manager to remove consumer from");
      }
    }

    boolean isClosing() {
      return this.closing;
    }

    synchronized void assign(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      this.subscriptionIdInClient = subscriptionIdInClient;
      this.manager = manager;
    }

    synchronized void detachFromManager() {
      this.manager = null;
    }
  }

  private static final class MessageHandlerContext implements Context {

    private final long offset;
    private final Consumer consumer;

    private MessageHandlerContext(long offset, Consumer consumer) {
      this.offset = offset;
      this.consumer = consumer;
    }

    @Override
    public long offset() {
      return this.offset;
    }

    @Override
    public void storeOffset() {
      this.consumer.store(this.offset);
    }

    @Override
    public Consumer consumer() {
      return this.consumer;
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
        SubscriptionTracker subscriptionTracker,
        OffsetSpecification offsetSpecification,
        boolean isSubscription) {
      boolean added = false;
      // FIXME deal with manager unavailability (manager may be closing because of connection
      // closing)
      // try all of them until it succeeds, throw exception if failure
      for (ClientSubscriptionsManager manager : managers) {
        if (!manager.isFull()) {
          manager.add(subscriptionTracker, offsetSpecification, isSubscription);
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
        manager.add(subscriptionTracker, offsetSpecification, isSubscription);
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
      for (ClientSubscriptionsManager manager : managers) {
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
    // the 2 data structures track the subscriptions, they must remain consistent
    private final Map<String, Set<SubscriptionTracker>> streamToStreamSubscriptions =
        new ConcurrentHashMap<>();
    private final ManagerPool owner;
    private volatile List<SubscriptionTracker> subscriptionTrackers =
        new ArrayList<>(maxConsumersByConnection);

    private ClientSubscriptionsManager(
        ManagerPool owner, Client.ClientParameters clientParameters) {
      this.owner = owner;
      String name = owner.name;
      LOGGER.debug("creating subscription manager on {}", name);
      IntStream.range(0, maxConsumersByConnection).forEach(i -> subscriptionTrackers.add(null));
      AtomicBoolean clientInitializedInManager = new AtomicBoolean(false);
      ChunkListener chunkListener =
          (client, subscriptionId, offset, messageCount, dataSize) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null && !subscriptionTracker.isClosing()) {
              client.credit(subscriptionId, 1);
            } else {
              LOGGER.debug(
                  "Could not find stream subscription {} or subscription closing, not providing credits",
                  subscriptionId & 0xFF);
            }
          };

      CreditNotification creditNotification =
          (subscriptionId, responseCode) ->
              LOGGER.debug(
                  "Received credit notification for subscription {}: {}",
                  subscriptionId & 0xFF,
                  Utils.formatConstant(responseCode));
      MessageListener messageListener =
          (subscriptionId, offset, message) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null) {
              subscriptionTracker.offset = offset;
              subscriptionTracker.messageHandler.handle(
                  new MessageHandlerContext(offset, subscriptionTracker.consumer), message);
              // FIXME set offset here as well, best effort to avoid duplicates
            } else {
              LOGGER.debug("Could not find stream subscription {}", subscriptionId);
            }
          };
      ShutdownListener shutdownListener =
          shutdownContext -> {
            // FIXME should the pool check if it's empty and so remove itself from the
            // pools data structure?

            // we may be closing the client because it's not the right node, so the manager
            // should not be removed from its pool, because it's not really in it already
            if (clientInitializedInManager.get()) {
              owner.remove(this);
            }
            if (shutdownContext.isShutdownUnexpected()) {
              LOGGER.debug(
                  "Unexpected shutdown notification on subscription client {}, scheduling consumers re-assignment",
                  name);
              environment
                  .scheduledExecutorService()
                  .execute(
                      () -> {
                        if (Thread.currentThread().isInterrupted()) {
                          return;
                        }
                        subscriptionTrackers.stream()
                            .filter(Objects::nonNull)
                            .forEach(SubscriptionTracker::detachFromManager);
                        for (Entry<String, Set<SubscriptionTracker>> entry :
                            streamToStreamSubscriptions.entrySet()) {
                          if (Thread.currentThread().isInterrupted()) {
                            break;
                          }
                          String stream = entry.getKey();
                          LOGGER.debug(
                              "Re-assigning {} consumer(s) to stream {} after disconnection",
                              entry.getValue().size(),
                              stream);
                          assignConsumersToStream(
                              entry.getValue(),
                              stream,
                              attempt -> environment.recoveryBackOffDelayPolicy().delay(attempt),
                              false);
                        }
                      });
            }
          };
      MetadataListener metadataListener =
          (stream, code) -> {
            LOGGER.debug(
                "Received metadata notification for {}, stream is likely to have become unavailable",
                stream);

            Set<SubscriptionTracker> affectedSubscriptions;
            synchronized (ClientSubscriptionsManager.this) {
              Set<SubscriptionTracker> subscriptions = streamToStreamSubscriptions.remove(stream);
              if (subscriptions != null && !subscriptions.isEmpty()) {
                List<SubscriptionTracker> newSubscriptions =
                    new ArrayList<>(maxConsumersByConnection);
                for (int i = 0; i < maxConsumersByConnection; i++) {
                  newSubscriptions.add(subscriptionTrackers.get(i));
                }
                for (SubscriptionTracker subscription : subscriptions) {
                  LOGGER.debug(
                      "Subscription {} was at offset {}",
                      subscription.subscriptionIdInClient,
                      subscription.offset);
                  newSubscriptions.set(subscription.subscriptionIdInClient & 0xFF, null);
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
                        if (Thread.currentThread().isInterrupted()) {
                          return;
                        }
                        LOGGER.debug(
                            "Trying to move {} subscription(s) (stream {})",
                            affectedSubscriptions.size(),
                            stream);
                        assignConsumersToStream(
                            affectedSubscriptions,
                            stream,
                            metadataUpdateBackOffDelayPolicy(),
                            isEmpty());
                      });
            }
          };
      ClientFactoryContext clientFactoryContext =
          ClientFactoryContext.fromParameters(
                  clientParameters
                      .clientProperty("connection_name", "rabbitmq-stream-consumer")
                      .chunkListener(chunkListener)
                      .creditNotification(creditNotification)
                      .messageListener(messageListener)
                      .shutdownListener(shutdownListener)
                      .metadataListener(metadataListener))
              .key(owner.name);
      this.client = clientFactory.client(clientFactoryContext);
      clientInitializedInManager.set(true);
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
              } catch (Exception e) {
                LOGGER.debug("Error while closing consumer: {}", e.getMessage());
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
                      if (affectedSubscription.consumer.isOpen()) {
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
                        synchronized (affectedSubscription.consumer) {
                          if (affectedSubscription.consumer.isOpen()) {
                            subscriptionPool.add(
                                affectedSubscription,
                                OffsetSpecification.offset(affectedSubscription.offset),
                                false);
                          }
                        }
                      } else {
                        LOGGER.debug("Not re-assigning consumer because it has been closed");
                      }
                    } catch (Exception e) {
                      LOGGER.warn(
                          "Error while re-assigning subscription from stream {}", stream, e);
                    }
                  }
                  if (closeClient) {
                    this.close();
                  }
                }
              })
          .exceptionally(
              ex -> {
                LOGGER.debug(
                    "Error while trying to assign {} consumer(s) to {}",
                    subscriptions.size(),
                    stream,
                    ex);
                consumersClosingCallback.run();
                return null;
              });
    }

    synchronized void add(
        SubscriptionTracker subscriptionTracker,
        OffsetSpecification offsetSpecification,
        boolean isSubcription) {
      // FIXME check manager is still open (not closed because of connection failure)
      byte subscriptionId = 0;
      for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
        if (subscriptionTrackers.get(i) == null) {
          subscriptionId = (byte) i;
          break;
        }
      }

      List<SubscriptionTracker> previousSubscriptions = this.subscriptionTrackers;

      LOGGER.debug(
          "Subscribing to {}, requested offset specification is {}, offset tracking reference is {}",
          subscriptionTracker.stream,
          offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification,
          subscriptionTracker.offsetTrackingReference);
      try {
        // updating data structures before subscribing
        // (to make sure they are up-to-date in case message would arrive super fast)
        subscriptionTracker.assign(subscriptionId, this);
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .add(subscriptionTracker);
        this.subscriptionTrackers =
            update(previousSubscriptions, subscriptionId, subscriptionTracker);
        // FIXME consider using fewer initial credits

        String offsetTrackingReference = subscriptionTracker.offsetTrackingReference;
        if (subscriptionTracker.offsetTrackingReference != null) {
          long trackedOffset =
              client.queryOffset(offsetTrackingReference, subscriptionTracker.stream);
          if (trackedOffset != 0) {
            if (offsetSpecification != null && isSubcription) {
              // subscription call (not recovery), so telling the user their offset specification is
              // ignored
              LOGGER.info(
                  "Requested offset specification {} not used in favor of stored offset found for reference {}",
                  offsetSpecification,
                  offsetTrackingReference);
            }
            LOGGER.debug(
                "Using offset {} to start consuming from {} with consumer {} " + "(instead of {})",
                trackedOffset,
                subscriptionTracker.stream,
                offsetTrackingReference,
                offsetSpecification);
            offsetSpecification = OffsetSpecification.offset(trackedOffset + 1);
          }
        }

        offsetSpecification =
            offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification;

        Map<String, String> subscriptionProperties = Collections.emptyMap();
        if (subscriptionTracker.offsetTrackingReference != null) {
          subscriptionProperties = new HashMap<>(1);
          subscriptionProperties.put("name", subscriptionTracker.offsetTrackingReference);
        }

        Client.Response subscribeResponse =
            client.subscribe(
                subscriptionId,
                subscriptionTracker.stream,
                offsetSpecification,
                10,
                subscriptionProperties);
        if (!subscribeResponse.isOk()) {
          String message =
              "Subscription to stream "
                  + subscriptionTracker.stream
                  + " failed with code "
                  + formatConstant(subscribeResponse.getResponseCode());
          LOGGER.debug(message);
          throw new StreamException(message);
        }
      } catch (RuntimeException e) {
        subscriptionTracker.assign((byte) -1, null);
        this.subscriptionTrackers = previousSubscriptions;
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .remove(subscriptionTracker);
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
            formatConstant(unsubscribeResponse.getResponseCode()),
            subscriptionIdInClient);
      }
      this.subscriptionTrackers = update(this.subscriptionTrackers, subscriptionIdInClient, null);
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
      List<SubscriptionTracker> newSubcriptions = new ArrayList<>(maxConsumersByConnection);
      int intIndex = index & 0xFF;
      for (int i = 0; i < maxConsumersByConnection; i++) {
        newSubcriptions.add(i == intIndex ? newValue : original.get(i));
      }
      return newSubcriptions;
    }

    synchronized boolean isFull() {
      return trackersCount() == maxConsumersByConnection;
    }

    synchronized boolean isEmpty() {
      return trackersCount() == 0;
    }

    private synchronized int trackersCount() {
      return (int) this.subscriptionTrackers.stream().filter(Objects::nonNull).count();
    }

    synchronized void close() {
      if (this.client != null && this.client.isOpen()) {
        subscriptionTrackers.stream()
            .filter(Objects::nonNull)
            .forEach(
                tracker -> {
                  try {
                    if (this.client != null && this.client.isOpen() && tracker.consumer.isOpen()) {
                      this.client.unsubscribe(tracker.subscriptionIdInClient);
                    }
                  } catch (Exception e) {
                    // OK, moving on
                  }
                });

        streamToStreamSubscriptions.clear();
        subscriptionTrackers.clear();

        if (this.client != null && this.client.isOpen()) {
          this.client.close();
        }
      }
    }
  }
}
