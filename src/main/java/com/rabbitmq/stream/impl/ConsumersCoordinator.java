// Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.Utils.convertCodeToException;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.isSac;
import static com.rabbitmq.stream.impl.Utils.namedFunction;
import static com.rabbitmq.stream.impl.Utils.namedRunnable;
import static java.lang.String.format;

import com.rabbitmq.stream.BackOffDelayPolicy;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.StreamException;
import com.rabbitmq.stream.StreamNotAvailableException;
import com.rabbitmq.stream.SubscriptionListener;
import com.rabbitmq.stream.SubscriptionListener.SubscriptionContext;
import com.rabbitmq.stream.impl.Client.Broker;
import com.rabbitmq.stream.impl.Client.ChunkListener;
import com.rabbitmq.stream.impl.Client.ConsumerUpdateListener;
import com.rabbitmq.stream.impl.Client.CreditNotification;
import com.rabbitmq.stream.impl.Client.MessageListener;
import com.rabbitmq.stream.impl.Client.MetadataListener;
import com.rabbitmq.stream.impl.Client.QueryOffsetResponse;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumersCoordinator {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;

  static final OffsetSpecification DEFAULT_OFFSET_SPECIFICATION = OffsetSpecification.next();

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumersCoordinator.class);
  private final Random random = new Random();
  private final StreamEnvironment environment;
  private final Map<String, ManagerPool> pools = new ConcurrentHashMap<>();
  private final ClientFactory clientFactory;
  private final int maxConsumersByConnection;
  private final Function<ClientConnectionType, String> connectionNamingStrategy;
  private final List<SubscriptionTracker> trackers = new CopyOnWriteArrayList<>();

  ConsumersCoordinator(
      StreamEnvironment environment,
      int maxConsumersByConnection,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      ClientFactory clientFactory) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxConsumersByConnection = maxConsumersByConnection;
    this.connectionNamingStrategy = connectionNamingStrategy;
  }

  private static String keyForClientSubscription(Client.Broker broker) {
    return broker.getHost() + ":" + broker.getPort();
  }

  private BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.environment.recoveryBackOffDelayPolicy();
  }

  private BackOffDelayPolicy metadataUpdateBackOffDelayPolicy() {
    return environment.topologyUpdateBackOffDelayPolicy();
  }

  Runnable subscribe(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      String trackingReference,
      SubscriptionListener subscriptionListener,
      Runnable trackingClosingCallback,
      MessageHandler messageHandler,
      Map<String, String> subscriptionProperties) {
    List<Client.Broker> candidates = findBrokersForStream(stream);
    Client.Broker newNode = pickBroker(candidates);
    if (newNode == null) {
      throw new IllegalStateException("No available node to subscribe to");
    }

    // create stream subscription to track final and changing state of this very subscription
    // we keep this instance when we move the subscription from a client to another one
    SubscriptionTracker subscriptionTracker =
        new SubscriptionTracker(
            consumer,
            stream,
            offsetSpecification,
            trackingReference,
            subscriptionListener,
            trackingClosingCallback,
            messageHandler,
            subscriptionProperties);

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

    try {
      managerPool.add(subscriptionTracker, offsetSpecification, true);
    } catch (RuntimeException e) {
      managerPool.clean();
      throw e;
    }
    this.trackers.add(subscriptionTracker);
    return subscriptionTracker::cancel;
  }

  // package protected for testing
  List<Client.Broker> findBrokersForStream(String stream) {
    Map<String, Client.StreamMetadata> metadata =
        this.environment.locatorOperation(
            namedFunction(
                c -> c.metadata(stream), "Candidate lookup to consume from '%s'", stream));
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
      LOGGER.debug("Only leader node {} for consuming from {}", streamMetadata.getLeader(), stream);
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

  private static String quote(String value) {
    if (value == null) {
      return "null";
    } else {
      return "\"" + value + "\"";
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append("\"subscription_count\" : ").append(this.trackers.size()).append(",");
    builder.append("\"subscriptions\" : [");
    builder.append(
        this.trackers.stream()
            .map(
                t -> {
                  StringBuilder b = new StringBuilder("{");
                  b.append(quote("stream")).append(":").append(quote(t.stream)).append(",");
                  b.append(quote("node")).append(":");
                  Client client = null;
                  ClientSubscriptionsManager manager = t.manager;
                  if (manager != null) {
                    client = manager.client;
                  }
                  if (client == null) {
                    b.append("null");
                  } else {
                    b.append(quote(client.getHost() + ":" + client.getPort()));
                  }
                  b.append(", ").append(quote("pool")).append(" : ");
                  ManagerPool pool = null;
                  if (manager != null) {
                    pool = manager.owner;
                  }
                  if (pool == null) {
                    b.append("null");
                  } else {
                    b.append(quote(pool.name));
                  }
                  b.append("}");
                  return b.toString();
                })
            .collect(Collectors.joining(",")));
    builder.append("],");
    builder.append("\"pool_count\" : ").append(this.pools.size()).append(",");
    builder.append("\"pools\" : ");
    builder.append(
        "[ \n"
            + pools.entrySet().stream()
                .map(
                    poolEntry ->
                        "  { \"broker\" : \""
                            + poolEntry.getKey()
                            + "\", \"client_count\" : "
                            + poolEntry.getValue().managers.size()
                            + ", \"clients\" : [ "
                            + poolEntry.getValue().managers.stream()
                                .map(
                                    manager ->
                                        "{ \"consumer_count\" : "
                                            + manager.subscriptionTrackers.stream()
                                                .filter(Objects::nonNull)
                                                .count()
                                            + " }")
                                .collect(Collectors.joining(", "))
                            + " ] }")
                .collect(Collectors.joining(", \n"))
            + "\n]}");
    return builder.toString();
  }

  /**
   * Data structure that keeps track of a given {@link StreamConsumer} and its message callback.
   *
   * <p>An instance is "moved" between {@link ClientSubscriptionsManager} instances on stream
   * failure or on disconnection.
   */
  private static class SubscriptionTracker {

    private final String stream;
    private final OffsetSpecification initialOffsetSpecification;
    private final String offsetTrackingReference;
    private final MessageHandler messageHandler;
    private final StreamConsumer consumer;
    private final SubscriptionListener subscriptionListener;
    private final Runnable trackingClosingCallback;
    private final Map<String, String> subscriptionProperties;
    private volatile long offset;
    private volatile boolean hasReceivedSomething = false;
    private volatile byte subscriptionIdInClient;
    private volatile ClientSubscriptionsManager manager;
    private volatile AtomicReference<SubscriptionState> state =
        new AtomicReference<>(SubscriptionState.OPENING);

    private SubscriptionTracker(
        StreamConsumer consumer,
        String stream,
        OffsetSpecification initialOffsetSpecification,
        String offsetTrackingReference,
        SubscriptionListener subscriptionListener,
        Runnable trackingClosingCallback,
        MessageHandler messageHandler,
        Map<String, String> subscriptionProperties) {
      this.consumer = consumer;
      this.stream = stream;
      this.initialOffsetSpecification = initialOffsetSpecification;
      this.offsetTrackingReference = offsetTrackingReference;
      this.subscriptionListener = subscriptionListener;
      this.trackingClosingCallback = trackingClosingCallback;
      this.messageHandler = messageHandler;
      if (this.offsetTrackingReference == null) {
        this.subscriptionProperties = subscriptionProperties;
      } else {
        Map<String, String> properties = new ConcurrentHashMap<>(subscriptionProperties.size() + 1);
        properties.putAll(subscriptionProperties);
        // we propagate the subscription name, used for monitoring
        properties.put("name", this.offsetTrackingReference);
        this.subscriptionProperties = Collections.unmodifiableMap(properties);
      }
    }

    synchronized void cancel() {
      // the flow of messages in the user message handler should stop, we can call the tracking
      // closing callback
      // with automatic offset tracking, it will store the last dispatched offset
      LOGGER.debug("Calling tracking consumer closing callback (may be no-op)");
      this.trackingClosingCallback.run();
      if (this.manager != null) {
        LOGGER.debug("Removing consumer from manager " + this.consumer);
        this.manager.remove(this);
      } else {
        LOGGER.debug("No manager to remove consumer from");
      }
      this.state(SubscriptionState.CLOSED);
    }

    synchronized void assign(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      this.subscriptionIdInClient = subscriptionIdInClient;
      this.manager = manager;
      if (this.manager == null) {
        if (consumer != null) {
          this.consumer.setSubscriptionClient(null);
        }
      } else {
        this.consumer.setSubscriptionClient(this.manager.client);
      }
    }

    synchronized void detachFromManager() {
      this.manager = null;
      this.consumer.setSubscriptionClient(null);
    }

    void state(SubscriptionState state) {
      this.state.set(state);
    }

    boolean compareAndSet(SubscriptionState expected, SubscriptionState newValue) {
      return this.state.compareAndSet(expected, newValue);
    }

    SubscriptionState state() {
      return this.state.get();
    }
  }

  private enum SubscriptionState {
    OPENING,
    ACTIVE,
    RECOVERING,
    CLOSED
  }

  private static final class MessageHandlerContext implements Context {

    private final long offset;
    private final long timestamp;
    private final long committedOffset;
    private final StreamConsumer consumer;

    private MessageHandlerContext(
        long offset, long timestamp, long committedOffset, StreamConsumer consumer) {
      this.offset = offset;
      this.timestamp = timestamp;
      this.committedOffset = committedOffset;
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
    public long timestamp() {
      return this.timestamp;
    }

    @Override
    public long committedChunkId() {
      return committedOffset;
    }

    public String stream() {
      return this.consumer.stream();
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

    private void add(
        SubscriptionTracker subscriptionTracker,
        OffsetSpecification offsetSpecification,
        boolean isInitialSubscription) {

      ClientSubscriptionsManager pickedManager = null;
      while (pickedManager == null) {
        // FIXME deal with manager unavailability (manager may be closing because of connection
        // closing)
        synchronized (this) {
          for (ClientSubscriptionsManager manager : managers) {
            if (!manager.isFull()) {
              pickedManager = manager;
              break;
            }
          }
          if (pickedManager == null) {
            LOGGER.debug(
                "Creating subscription manager on {}, this is subscription manager #{}",
                name,
                managers.size() + 1);
            pickedManager = new ClientSubscriptionsManager(this, clientParameters);
            managers.add(pickedManager);
          }
        }
        try {
          pickedManager.add(subscriptionTracker, offsetSpecification, isInitialSubscription);
        } catch (IllegalStateException e) {
          pickedManager = null;
        } catch (TimeoutStreamException | ClientClosedException | StreamNotAvailableException e) {
          // manager connection is dead or stream not available
          // scheduling manager closing if necessary in another thread to avoid blocking this one
          if (pickedManager.isEmpty()) {
            ClientSubscriptionsManager manager = pickedManager;
            ConsumersCoordinator.this.environment.execute(
                () -> {
                  this.remove(manager);
                  manager.close();
                },
                "Consumer manager closing after timeout, consumer %d on stream '%s'",
                subscriptionTracker.consumer.id(),
                subscriptionTracker.stream);
          }
          throw e;
        } catch (RuntimeException e) {
          this.maybeDisposeManager(pickedManager);
          throw e;
        }
      }
    }

    private synchronized void clean() {
      for (ClientSubscriptionsManager manager : managers) {
        maybeDisposeManager(manager);
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
    // trackers and tracker count must be kept in sync
    private volatile List<SubscriptionTracker> subscriptionTrackers =
        new ArrayList<>(maxConsumersByConnection);
    private volatile int trackerCount = 0;
    private final ManagerPool owner;

    private ClientSubscriptionsManager(
        ManagerPool owner, Client.ClientParameters clientParameters) {
      this.owner = owner;
      String name = owner.name;
      LOGGER.debug("creating subscription manager on {}", name);
      IntStream.range(0, maxConsumersByConnection).forEach(i -> subscriptionTrackers.add(null));
      this.trackerCount = 0;
      AtomicBoolean clientInitializedInManager = new AtomicBoolean(false);
      ChunkListener chunkListener =
          (client, subscriptionId, offset, messageCount, dataSize) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null && subscriptionTracker.consumer.isOpen()) {
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
          (subscriptionId, offset, chunkTimestamp, committedOffset, message) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);

            if (subscriptionTracker != null) {
              subscriptionTracker.offset = offset;
              subscriptionTracker.hasReceivedSomething = true;
              subscriptionTracker.messageHandler.handle(
                  new MessageHandlerContext(
                      offset, chunkTimestamp, committedOffset, subscriptionTracker.consumer),
                  message);
              // FIXME set offset here as well, best effort to avoid duplicates?
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
              this.owner.remove(this);
            }
            if (shutdownContext.isShutdownUnexpected()) {
              LOGGER.debug(
                  "Unexpected shutdown notification on subscription connection {}, scheduling consumers re-assignment",
                  name);
              LOGGER.debug(
                  "Subscription connection has {} consumer(s) over {} stream(s) to recover",
                  this.subscriptionTrackers.stream().filter(Objects::nonNull).count(),
                  this.streamToStreamSubscriptions.size());
              environment
                  .scheduledExecutorService()
                  .execute(
                      namedRunnable(
                          () -> {
                            if (Thread.currentThread().isInterrupted()) {
                              return;
                            }
                            subscriptionTrackers.stream()
                                .filter(Objects::nonNull)
                                .filter(t -> t.state() == SubscriptionState.ACTIVE)
                                .forEach(SubscriptionTracker::detachFromManager);
                            for (Entry<String, Set<SubscriptionTracker>> entry :
                                streamToStreamSubscriptions.entrySet()) {
                              if (Thread.currentThread().isInterrupted()) {
                                LOGGER.debug("Interrupting consumer re-assignment task");
                                break;
                              }
                              String stream = entry.getKey();
                              Set<SubscriptionTracker> trackersToReAssign = entry.getValue();
                              if (trackersToReAssign == null || trackersToReAssign.isEmpty()) {
                                LOGGER.debug(
                                    "No consumer to re-assign to stream {} after disconnection",
                                    stream);
                              } else {
                                LOGGER.debug(
                                    "Re-assigning {} consumer(s) to stream {} after disconnection",
                                    trackersToReAssign.size(),
                                    stream);
                                assignConsumersToStream(
                                    trackersToReAssign,
                                    stream,
                                    recoveryBackOffDelayPolicy(),
                                    false);
                              }
                            }
                          },
                          "Consumers re-assignment after disconnection from %s",
                          name));
            }
          };
      MetadataListener metadataListener =
          (stream, code) -> {
            LOGGER.debug(
                "Received metadata notification for '{}', stream is likely to have become unavailable",
                stream);

            Set<SubscriptionTracker> affectedSubscriptions;
            synchronized (this) {
              Set<SubscriptionTracker> subscriptions = streamToStreamSubscriptions.remove(stream);
              if (subscriptions != null && !subscriptions.isEmpty()) {
                List<SubscriptionTracker> newSubscriptions =
                    new ArrayList<>(maxConsumersByConnection);
                for (int i = 0; i < maxConsumersByConnection; i++) {
                  newSubscriptions.add(subscriptionTrackers.get(i));
                }
                for (SubscriptionTracker subscription : subscriptions) {
                  LOGGER.debug(
                      "Subscription {} was at offset {} (received something? {})",
                      subscription.subscriptionIdInClient,
                      subscription.offset,
                      subscription.hasReceivedSomething);
                  newSubscriptions.set(subscription.subscriptionIdInClient & 0xFF, null);
                  subscription.consumer.setSubscriptionClient(null);
                }
                this.setSubscriptionTrackers(newSubscriptions);
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
                      namedRunnable(
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
                          },
                          "Consumers re-assignment after metadata update on stream '%s'",
                          stream));
            }
          };
      ConsumerUpdateListener consumerUpdateListener =
          (client, subscriptionId, active) -> {
            OffsetSpecification result = null;
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null) {
              if (isSac(subscriptionTracker.subscriptionProperties)) {
                result = subscriptionTracker.consumer.consumerUpdate(active);
              } else {
                LOGGER.debug(
                    "Subscription {} is not a single active consumer, nothing to do.",
                    subscriptionId);
              }
            } else {
              LOGGER.debug(
                  "Could not find stream subscription {} for consumer update", subscriptionId);
            }
            return result;
          };
      String connectionName = connectionNamingStrategy.apply(ClientConnectionType.CONSUMER);
      ClientFactoryContext clientFactoryContext =
          ClientFactoryContext.fromParameters(
                  clientParameters
                      .clientProperty("connection_name", connectionName)
                      .chunkListener(chunkListener)
                      .creditNotification(creditNotification)
                      .messageListener(messageListener)
                      .shutdownListener(shutdownListener)
                      .metadataListener(metadataListener)
                      .consumerUpdateListener(consumerUpdateListener))
              .key(owner.name);
      this.client = clientFactory.client(clientFactoryContext);
      LOGGER.debug("Created consumer connection '{}'", connectionName);
      maybeExchangeCommandVersions(client);
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
          .description("Candidate lookup to consume from '%s'", stream)
          .scheduler(environment.scheduledExecutorService())
          .retry(ex -> !(ex instanceof StreamDoesNotExistException))
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(
              candidateNodes -> {
                List<Broker> candidates = candidateNodes;
                if (candidates == null) {
                  LOGGER.debug("No candidate nodes to consume from '{}'", stream);
                  consumersClosingCallback.run();
                } else {
                  for (SubscriptionTracker affectedSubscription : subscriptions) {
                    if (affectedSubscription.compareAndSet(
                        SubscriptionState.ACTIVE, SubscriptionState.RECOVERING)) {
                      recoverSubscription(stream, candidates, affectedSubscription);
                    } else {
                      LOGGER.debug(
                          "Not recovering consumer {} from stream {}, state is {}, expected is {}",
                          affectedSubscription.consumer.id(),
                          affectedSubscription.stream,
                          affectedSubscription.state(),
                          SubscriptionState.ACTIVE);
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

    private void recoverSubscription(
        String stream, List<Broker> candidates, SubscriptionTracker affectedSubscription) {
      ManagerPool subscriptionPool = null;
      boolean reassignmentCompleted = false;
      while (!reassignmentCompleted) {
        try {
          if (affectedSubscription.consumer.isOpen()) {
            Broker broker = pickBroker(candidates);
            LOGGER.debug("Using {} to resume consuming from {}", broker, stream);
            String key = keyForClientSubscription(broker);
            subscriptionPool =
                pools.computeIfAbsent(
                    key,
                    s -> {
                      LOGGER.debug(
                          "Creating new connection pool {} to consume from {}", key, stream);
                      return new ManagerPool(
                          key,
                          environment
                              .clientParametersCopy()
                              .host(broker.getHost())
                              .port(broker.getPort()));
                    });
            synchronized (affectedSubscription.consumer) {
              if (affectedSubscription.consumer.isOpen()) {
                OffsetSpecification offsetSpecification;
                if (affectedSubscription.hasReceivedSomething) {
                  offsetSpecification = OffsetSpecification.offset(affectedSubscription.offset);
                } else {
                  offsetSpecification = affectedSubscription.initialOffsetSpecification;
                }
                subscriptionPool.add(affectedSubscription, offsetSpecification, false);
                reassignmentCompleted = true;
              } else {
                reassignmentCompleted = true;
              }
            }
          } else {
            LOGGER.debug("Not re-assigning consumer because it has been closed");
            reassignmentCompleted = true;
          }
        } catch (TimeoutStreamException | ClientClosedException | StreamNotAvailableException e) {
          LOGGER.debug(
              "Consumer {} re-assignment on stream {} timed out or connection closed or stream not available, "
                  + "refreshing candidates and retrying",
              affectedSubscription.consumer.id(),
              affectedSubscription.stream);
          if (subscriptionPool != null) {
            subscriptionPool.clean();
          }
          // maybe not a good candidate, let's refresh and retry for this one
          candidates =
              callAndMaybeRetry(
                  () -> findBrokersForStream(stream),
                  ex -> !(ex instanceof StreamDoesNotExistException),
                  environment.recoveryBackOffDelayPolicy(),
                  "Candidate lookup to consume from '%s'",
                  stream);

        } catch (Exception e) {
          LOGGER.warn("Error while re-assigning subscription from stream {}", stream, e);
          reassignmentCompleted = true;
        }
      }
    }

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    synchronized void add(
        SubscriptionTracker subscriptionTracker,
        OffsetSpecification offsetSpecification,
        boolean isInitialSubscription) {
      if (this.isFull()) {
        throw new IllegalStateException("Cannot add subscription tracker, the manager is full");
      }

      checkNotClosed();

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
          "Subscribing to {}, requested offset specification is {}, offset tracking reference is {}, properties are {}",
          subscriptionTracker.stream,
          offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification,
          subscriptionTracker.offsetTrackingReference,
          subscriptionTracker.subscriptionProperties);
      try {
        // updating data structures before subscribing
        // (to make sure they are up-to-date in case message would arrive super fast)
        subscriptionTracker.assign(subscriptionId, this);
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .add(subscriptionTracker);
        this.setSubscriptionTrackers(
            update(previousSubscriptions, subscriptionId, subscriptionTracker));

        String offsetTrackingReference = subscriptionTracker.offsetTrackingReference;
        if (offsetTrackingReference != null) {
          checkNotClosed();
          QueryOffsetResponse queryOffsetResponse =
              callAndMaybeRetry(
                  () -> client.queryOffset(offsetTrackingReference, subscriptionTracker.stream),
                  RETRY_ON_TIMEOUT,
                  "Offset query for consumer %s on stream '%s' (reference %s)",
                  subscriptionTracker.consumer.id(),
                  subscriptionTracker.stream,
                  offsetTrackingReference);
          if (queryOffsetResponse.isOk() && queryOffsetResponse.getOffset() != 0) {
            if (offsetSpecification != null && isInitialSubscription) {
              // subscription call (not recovery), so telling the user their offset specification
              // is
              // ignored
              LOGGER.info(
                  "Requested offset specification {} not used in favor of stored offset found for reference {}",
                  offsetSpecification,
                  offsetTrackingReference);
            }
            LOGGER.debug(
                "Using offset {} to start consuming from {} with consumer {} " + "(instead of {})",
                queryOffsetResponse.getOffset(),
                subscriptionTracker.stream,
                offsetTrackingReference,
                offsetSpecification);
            offsetSpecification = OffsetSpecification.offset(queryOffsetResponse.getOffset() + 1);
          }
        }

        offsetSpecification =
            offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification;

        // TODO consider using/emulating ConsumerUpdateListener, to have only one API, not 2
        // even when the consumer is not a SAC.
        SubscriptionContext subscriptionContext =
            new DefaultSubscriptionContext(offsetSpecification);
        subscriptionTracker.subscriptionListener.preSubscribe(subscriptionContext);
        LOGGER.info(
            "Computed offset specification {}, offset specification used after subscription listener {}",
            offsetSpecification,
            subscriptionContext.offsetSpecification());

        checkNotClosed();
        // FIXME consider using fewer initial credits
        byte subId = subscriptionId;
        Client.Response subscribeResponse =
            callAndMaybeRetry(
                () ->
                    client.subscribe(
                        subId,
                        subscriptionTracker.stream,
                        subscriptionContext.offsetSpecification(),
                        10,
                        subscriptionTracker.subscriptionProperties),
                RETRY_ON_TIMEOUT,
                "Subscribe request for consumer %s on stream '%s'",
                subscriptionTracker.consumer.id(),
                subscriptionTracker.stream);
        if (!subscribeResponse.isOk()) {
          String message =
              "Subscription to stream "
                  + subscriptionTracker.stream
                  + " failed with code "
                  + formatConstant(subscribeResponse.getResponseCode());
          LOGGER.debug(message);
          throw convertCodeToException(
              subscribeResponse.getResponseCode(), subscriptionTracker.stream, () -> message);
          //          throw new StreamException(message);
        }
      } catch (RuntimeException e) {
        subscriptionTracker.assign((byte) -1, null);
        this.setSubscriptionTrackers(previousSubscriptions);
        streamToStreamSubscriptions
            .computeIfAbsent(subscriptionTracker.stream, s -> ConcurrentHashMap.newKeySet())
            .remove(subscriptionTracker);
        maybeCleanStreamToStreamSubscriptions(subscriptionTracker.stream);
        throw e;
      }
      subscriptionTracker.state(SubscriptionState.ACTIVE);
      LOGGER.debug("Subscribed to {}", subscriptionTracker.stream);
    }

    private void maybeCleanStreamToStreamSubscriptions(String stream) {
      this.streamToStreamSubscriptions.compute(
          stream,
          (s, trackers) -> {
            if (trackers == null || trackers.isEmpty()) {
              return null;
            } else {
              return trackers;
            }
          });
    }

    synchronized void remove(SubscriptionTracker subscriptionTracker) {
      // FIXME check manager is still open (not closed because of connection failure)
      byte subscriptionIdInClient = subscriptionTracker.subscriptionIdInClient;
      try {
        Client.Response unsubscribeResponse =
            callAndMaybeRetry(
                () -> client.unsubscribe(subscriptionIdInClient),
                RETRY_ON_TIMEOUT,
                "Unsubscribe request for consumer %d on stream '%s'",
                subscriptionTracker.consumer.id(),
                subscriptionTracker.stream);
        if (!unsubscribeResponse.isOk()) {
          LOGGER.warn(
              "Unexpected response code when unsubscribing from {}: {} (subscription ID {})",
              subscriptionTracker.stream,
              formatConstant(unsubscribeResponse.getResponseCode()),
              subscriptionIdInClient);
        }
      } catch (TimeoutStreamException e) {
        LOGGER.debug(
            "Reached timeout when trying to unsubscribe consumer {} from stream '{}'",
            subscriptionTracker.consumer.id(),
            subscriptionTracker.stream);
      }

      this.setSubscriptionTrackers(update(this.subscriptionTrackers, subscriptionIdInClient, null));
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

    private void setSubscriptionTrackers(List<SubscriptionTracker> trackers) {
      this.subscriptionTrackers = trackers;
      this.trackerCount = (int) this.subscriptionTrackers.stream().filter(Objects::nonNull).count();
    }

    boolean isFull() {
      return this.trackerCount == maxConsumersByConnection;
    }

    boolean isEmpty() {
      return this.trackerCount == 0;
    }

    synchronized void close() {
      if (this.client != null && this.client.isOpen()) {
        for (int i = 0; i < this.subscriptionTrackers.size(); i++) {
          SubscriptionTracker tracker = this.subscriptionTrackers.get(i);
          if (tracker != null) {
            try {
              if (this.client != null && this.client.isOpen() && tracker.consumer.isOpen()) {
                this.client.unsubscribe(tracker.subscriptionIdInClient);
              }
            } catch (Exception e) {
              // OK, moving on
              LOGGER.debug(
                  "Error while unsubscribing from {}, registration {}",
                  tracker.stream,
                  tracker.subscriptionIdInClient);
            }
            this.subscriptionTrackers.set(i, null);
          }
        }

        streamToStreamSubscriptions.clear();

        if (this.client != null && this.client.isOpen()) {
          this.client.close();
        }
      }
    }
  }

  private static final class DefaultSubscriptionContext implements SubscriptionContext {

    private volatile OffsetSpecification offsetSpecification;

    private DefaultSubscriptionContext(OffsetSpecification computedOffsetSpecification) {
      this.offsetSpecification = computedOffsetSpecification;
    }

    @Override
    public OffsetSpecification offsetSpecification() {
      return this.offsetSpecification;
    }

    @Override
    public void offsetSpecification(OffsetSpecification offsetSpecification) {
      this.offsetSpecification = offsetSpecification;
    }

    @Override
    public String toString() {
      return "SubscriptionContext{" + "offsetSpecification=" + offsetSpecification + '}';
    }
  }

  private static void maybeExchangeCommandVersions(Client client) {
    try {
      if (Utils.is3_11_OrMore(client.brokerVersion())) {
        client.exchangeCommandVersions();
      }
    } catch (Exception e) {
      LOGGER.info("Error while exchanging command versions: {}", e.getMessage());
    }
  }

  static <T> T callAndMaybeRetry(
      Supplier<T> operation, Predicate<Exception> retryCondition, String format, Object... args) {
    return callAndMaybeRetry(operation, retryCondition, i -> Duration.ZERO, format, args);
  }

  static <T> T callAndMaybeRetry(
      Supplier<T> operation,
      Predicate<Exception> retryCondition,
      BackOffDelayPolicy delayPolicy,
      String format,
      Object... args) {
    String description = format(format, args);
    int attempt = 0;
    Exception lastException = null;
    while (attempt++ < 3) {
      try {
        return operation.get();
      } catch (Exception e) {
        lastException = e;
        if (retryCondition.test(e)) {
          LOGGER.debug("Operation '{}' failed, retrying...", description);
          Duration delay = delayPolicy.delay(attempt - 1);
          if (!delay.isZero()) {
            try {
              Thread.sleep(delay.toMillis());
            } catch (InterruptedException ex) {
              Thread.interrupted();
              lastException = ex;
              break;
            }
          }
        } else {
          break;
        }
      }
    }
    String message =
        format("Could not complete task '%s' after %d attempt(s)", description, --attempt);
    LOGGER.debug(message);
    if (lastException == null) {
      throw new StreamException(message);
    } else if (lastException instanceof RuntimeException) {
      throw (RuntimeException) lastException;
    } else {
      throw new StreamException(message, lastException);
    }
  }

  private static final Predicate<Exception> RETRY_ON_TIMEOUT =
      e -> e instanceof TimeoutStreamException;

  private static class ClientClosedException extends StreamException {

    public ClientClosedException() {
      super("Client already closed");
    }
  }
}
