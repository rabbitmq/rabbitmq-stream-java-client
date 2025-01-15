// Copyright (c) 2020-2025 Broadcom. All Rights Reserved.
// The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.
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

import static com.rabbitmq.stream.Constants.RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS;
import static com.rabbitmq.stream.impl.Utils.*;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.SubscriptionListener.SubscriptionContext;
import com.rabbitmq.stream.impl.Client.*;
import com.rabbitmq.stream.impl.Client.ConsumerUpdateListener;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ConsumersCoordinator implements AutoCloseable {

  static final int MAX_SUBSCRIPTIONS_PER_CLIENT = 256;
  static final int MAX_ATTEMPT_BEFORE_FALLING_BACK_TO_LEADER = 5;
  private static final boolean DEBUG = false;

  static final OffsetSpecification DEFAULT_OFFSET_SPECIFICATION = OffsetSpecification.next();

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumersCoordinator.class);
  private final StreamEnvironment environment;
  private final ClientFactory clientFactory;
  private final int maxConsumersByConnection;
  private final Function<ClientConnectionType, String> connectionNamingStrategy;
  private final AtomicLong managerIdSequence = new AtomicLong(0);
  private final NavigableSet<ClientSubscriptionsManager> managers = new ConcurrentSkipListSet<>();
  private final AtomicLong trackerIdSequence = new AtomicLong(0);
  private final Function<List<Broker>, Broker> brokerPicker;

  private final List<SubscriptionTracker> trackers = new CopyOnWriteArrayList<>();
  private final ExecutorServiceFactory executorServiceFactory =
      new DefaultExecutorServiceFactory(
          Runtime.getRuntime().availableProcessors(), 10, "rabbitmq-stream-consumer-connection-");
  private final boolean forceReplica;
  private final Lock coordinatorLock = new ReentrantLock();

  ConsumersCoordinator(
      StreamEnvironment environment,
      int maxConsumersByConnection,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      ClientFactory clientFactory,
      boolean forceReplica,
      Function<List<Broker>, Broker> brokerPicker) {
    this.environment = environment;
    this.clientFactory = clientFactory;
    this.maxConsumersByConnection = maxConsumersByConnection;
    this.connectionNamingStrategy = connectionNamingStrategy;
    this.forceReplica = forceReplica;
    this.brokerPicker = brokerPicker;
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
      Map<String, String> subscriptionProperties,
      ConsumerFlowStrategy flowStrategy) {
    return lock(
        this.coordinatorLock,
        () -> {
          List<BrokerWrapper> candidates = findCandidateNodes(stream, forceReplica);
          Broker newNode = pickBroker(this.brokerPicker, candidates);
          if (newNode == null) {
            throw new IllegalStateException("No available node to subscribe to");
          }

          // create stream subscription to track final and changing state of this very subscription
          // we keep this instance when we move the subscription from a client to another one
          SubscriptionTracker subscriptionTracker =
              new SubscriptionTracker(
                  this.trackerIdSequence.getAndIncrement(),
                  consumer,
                  stream,
                  offsetSpecification,
                  trackingReference,
                  subscriptionListener,
                  trackingClosingCallback,
                  messageHandler,
                  subscriptionProperties,
                  flowStrategy);

          try {
            addToManager(newNode, candidates, subscriptionTracker, offsetSpecification, true);
          } catch (ConnectionStreamException e) {
            // these exceptions are not public
            throw new StreamException(e.getMessage());
          }

          if (DEBUG) {
            this.trackers.add(subscriptionTracker);
            return () -> {
              try {
                this.trackers.remove(subscriptionTracker);
              } catch (Exception e) {
                LOGGER.debug("Error while removing subscription tracker from list");
              }
              subscriptionTracker.cancel();
            };
          } else {
            return subscriptionTracker::cancel;
          }
        });
  }

  private void addToManager(
      Broker node,
      List<BrokerWrapper> candidates,
      SubscriptionTracker tracker,
      OffsetSpecification offsetSpecification,
      boolean isInitialSubscription) {
    ClientParameters clientParameters =
        environment
            .clientParametersCopy()
            .executorServiceFactory(this.executorServiceFactory)
            .host(node.getHost())
            .port(node.getPort());
    ClientSubscriptionsManager pickedManager = null;
    while (pickedManager == null) {
      Iterator<ClientSubscriptionsManager> iterator = this.managers.iterator();
      while (iterator.hasNext()) {
        pickedManager = iterator.next();
        if (pickedManager.isClosed()) {
          iterator.remove();
          pickedManager = null;
        } else {
          if (node.equals(pickedManager.node) && !pickedManager.isFull()) {
            // let's try this one
            break;
          } else {
            pickedManager = null;
          }
        }
      }
      if (pickedManager == null) {
        String name = keyForNode(node);
        LOGGER.debug("Creating subscription manager on {}", name);
        pickedManager = new ClientSubscriptionsManager(node, candidates, clientParameters);
        LOGGER.debug("Created subscription manager on {}, id {}", name, pickedManager.id);
      }
      try {
        pickedManager.add(tracker, offsetSpecification, isInitialSubscription);
        LOGGER.debug(
            "Assigned tracker {} to manager {} (node {}), subscription ID {}",
            tracker.label(),
            pickedManager.id,
            pickedManager.name,
            tracker.subscriptionIdInClient);
        this.managers.add(pickedManager);
      } catch (IllegalStateException e) {
        pickedManager = null;
      } catch (ConnectionStreamException | ClientClosedException | StreamNotAvailableException e) {
        // manager connection is dead or stream not available
        // scheduling manager closing if necessary in another thread to avoid blocking this one
        if (pickedManager.isEmpty()) {
          ConsumersCoordinator.this.environment.execute(
              pickedManager::closeIfEmpty,
              "Consumer manager closing after timeout, consumer %d on stream '%s'",
              tracker.consumer.id(),
              tracker.stream);
        }
        throw e;
      } catch (RuntimeException e) {
        if (pickedManager != null) {
          pickedManager.closeIfEmpty();
        }
        throw e;
      }
    }
  }

  int managerCount() {
    return this.managers.size();
  }

  // package protected for testing
  List<BrokerWrapper> findCandidateNodes(String stream, boolean forceReplica) {
    LOGGER.debug(
        "Candidate lookup to consumer from '{}', forcing replica? {}", stream, forceReplica);
    Map<String, Client.StreamMetadata> metadata =
        this.environment.locatorOperation(
            namedFunction(
                c -> c.metadata(stream), "Candidate lookup to consume from '%s'", stream));
    if (metadata.isEmpty() || metadata.get(stream) == null) {
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

    Broker leader = streamMetadata.getLeader();
    List<Broker> replicas = streamMetadata.getReplicas();
    if ((replicas == null || replicas.isEmpty()) && leader == null) {
      throw new IllegalStateException("No node available to consume from stream " + stream);
    }

    List<BrokerWrapper> brokers;
    if (replicas == null || replicas.isEmpty()) {
      if (forceReplica) {
        throw new IllegalStateException(
            format(
                "Only the leader node is available for consuming from %s and "
                    + "consuming from leader has been deactivated for this consumer",
                stream));
      } else {
        brokers = Collections.singletonList(new BrokerWrapper(leader, true));
        LOGGER.debug("Only leader node {} for consuming from {}", leader, stream);
      }
    } else {
      LOGGER.debug("Replicas for consuming from {}: {}", stream, replicas);
      brokers =
          replicas.stream()
              .map(b -> new BrokerWrapper(b, false))
              .collect(Collectors.toCollection(ArrayList::new));
      if (!forceReplica && leader != null) {
        brokers.add(new BrokerWrapper(leader, true));
      }
    }

    LOGGER.debug("Candidates to consume from {}: {}", stream, brokers);

    return brokers;
  }

  private Callable<List<BrokerWrapper>> findCandidateNodes(String stream) {
    AtomicInteger attemptNumber = new AtomicInteger();
    return () -> {
      boolean mustUseReplica;
      if (forceReplica) {
        mustUseReplica =
            attemptNumber.incrementAndGet() <= MAX_ATTEMPT_BEFORE_FALLING_BACK_TO_LEADER;
      } else {
        mustUseReplica = false;
      }
      LOGGER.debug(
          "Looking for broker(s) for stream {}, forcing replica {}", stream, mustUseReplica);
      return findCandidateNodes(stream, mustUseReplica);
    };
  }

  public void close() {
    Iterator<ClientSubscriptionsManager> iterator = this.managers.iterator();
    while (iterator.hasNext()) {
      ClientSubscriptionsManager manager = iterator.next();
      try {
        iterator.remove();
        manager.close();
      } catch (Exception e) {
        LOGGER.info(
            "Error while closing manager {} connected to node {}: {}",
            manager.id,
            manager.name,
            e.getMessage());
      }
    }
    try {
      this.executorServiceFactory.close();
    } catch (Exception e) {
      LOGGER.info("Error while closing executor service factory: {}", e.getMessage());
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{");
    builder.append(jsonField("client_count", this.managers.size())).append(", ");
    builder.append(quote("clients")).append(" : [");
    builder.append(
        this.managers.stream()
            .map(
                m -> {
                  StringBuilder managerBuilder = new StringBuilder("{");
                  managerBuilder
                      .append(jsonField("id", m.id))
                      .append(",")
                      .append(jsonField("node", m.name))
                      .append(",")
                      .append(jsonField("consumer_count", m.trackerCount))
                      .append(",");
                  managerBuilder.append("\"subscriptions\" : [");
                  List<SubscriptionTracker> trackers = m.subscriptionTrackers;
                  managerBuilder.append(
                      trackers.stream()
                          .filter(Objects::nonNull)
                          .map(
                              t -> {
                                StringBuilder trackerBuilder = new StringBuilder("{");
                                trackerBuilder.append(jsonField("stream", t.stream)).append(",");
                                trackerBuilder.append(jsonField("id", t.id)).append(",");
                                trackerBuilder.append(
                                    jsonField("subscription_id", t.subscriptionIdInClient));
                                return trackerBuilder.append("}").toString();
                              })
                          .collect(Collectors.joining(",")));
                  managerBuilder.append("]");
                  return managerBuilder.append("}").toString();
                })
            .collect(Collectors.joining(",")));
    builder.append("]");
    if (DEBUG) {
      builder.append(",");
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
                    return b.append("}").toString();
                  })
              .collect(Collectors.joining(",")));
      builder.append("]");
    }
    builder.append("}");
    return builder.toString();
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
    private final AtomicReference<SubscriptionState> state =
        new AtomicReference<>(SubscriptionState.OPENING);
    private final ConsumerFlowStrategy flowStrategy;
    private final Lock subscriptionTrackerLock = new ReentrantLock();

    private SubscriptionTracker(
        long id,
        StreamConsumer consumer,
        String stream,
        OffsetSpecification initialOffsetSpecification,
        String offsetTrackingReference,
        SubscriptionListener subscriptionListener,
        Runnable trackingClosingCallback,
        MessageHandler messageHandler,
        Map<String, String> subscriptionProperties,
        ConsumerFlowStrategy flowStrategy) {
      this.id = id;
      this.consumer = consumer;
      this.stream = stream;
      this.initialOffsetSpecification = initialOffsetSpecification;
      this.offsetTrackingReference = offsetTrackingReference;
      this.subscriptionListener = subscriptionListener;
      this.trackingClosingCallback = trackingClosingCallback;
      this.messageHandler = messageHandler;
      this.flowStrategy = flowStrategy;
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

    void cancel() {
      lock(
          this.subscriptionTrackerLock,
          () -> {
            // the flow of messages in the user message handler should stop, we can call the
            // tracking
            // closing callback
            // with automatic offset tracking, it will store the last dispatched offset
            LOGGER.debug("Calling tracking consumer closing callback (may be no-op)");
            this.trackingClosingCallback.run();
            if (this.manager != null) {
              LOGGER.debug("Removing tracker {} from manager", this.label());
              this.manager.remove(this);
            } else {
              LOGGER.debug("No manager to remove consumer from");
            }
            this.state(SubscriptionState.CLOSED);
          });
    }

    void assign(byte subscriptionIdInClient, ClientSubscriptionsManager manager) {
      lock(
          this.subscriptionTrackerLock,
          () -> {
            this.subscriptionIdInClient = subscriptionIdInClient;
            this.manager = manager;
            if (this.manager == null) {
              if (consumer != null) {
                this.consumer.setSubscriptionClient(null);
              }
            } else {
              this.consumer.setSubscriptionClient(this.manager.client);
            }
          });
    }

    void detachFromManager() {
      lock(
          this.subscriptionTrackerLock,
          () -> {
            this.manager = null;
            this.consumer.setSubscriptionClient(null);
          });
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

    String label() {
      return String.format(
          "[id=%d, stream=%s, name=%s, consumer=%d]",
          this.id, this.stream, this.offsetTrackingReference, this.consumer.id());
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
    private final ConsumerFlowStrategy.MessageProcessedCallback processedCallback;

    private MessageHandlerContext(
        long offset,
        long timestamp,
        long committedOffset,
        StreamConsumer consumer,
        ConsumerFlowStrategy.MessageProcessedCallback processedCallback) {
      this.offset = offset;
      this.timestamp = timestamp;
      this.committedOffset = committedOffset;
      this.consumer = consumer;
      this.processedCallback = processedCallback;
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

    @Override
    public void processed() {
      this.processedCallback.processed(this);
    }
  }

  /**
   * Maintains a set of {@link SubscriptionTracker} instances on a {@link Client}.
   *
   * <p>It dispatches inbound messages to the appropriate {@link SubscriptionTracker} and
   * re-allocates {@link SubscriptionTracker}s in case of stream unavailability or disconnection.
   */
  private class ClientSubscriptionsManager implements Comparable<ClientSubscriptionsManager> {

    private final long id;
    private final Broker node;
    private final Client client;
    // <host>:<port> (actual or advertised)
    private final String name;
    // the 2 data structures track the subscriptions, they must remain consistent
    private final Map<String, Set<SubscriptionTracker>> streamToStreamSubscriptions =
        new ConcurrentHashMap<>();
    // trackers and tracker count must be kept in sync
    private volatile List<SubscriptionTracker> subscriptionTrackers =
        createSubscriptionTrackerList();
    private final AtomicInteger consumerIndexSequence = new AtomicInteger(0);
    private volatile int trackerCount;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Lock subscriptionManagerLock = new ReentrantLock();

    private ClientSubscriptionsManager(
        Broker targetNode,
        List<BrokerWrapper> candidates,
        Client.ClientParameters clientParameters) {
      this.id = managerIdSequence.getAndIncrement();
      this.trackerCount = 0;
      AtomicReference<String> nameReference = new AtomicReference<>();

      AtomicBoolean clientInitializedInManager = new AtomicBoolean(false);
      ChunkListener chunkListener =
          (client, subscriptionId, offset, messageCount, dataSize) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            ConsumerFlowStrategy.MessageProcessedCallback processCallback;
            if (subscriptionTracker != null && subscriptionTracker.consumer.isOpen()) {
              processCallback =
                  subscriptionTracker.flowStrategy.start(
                      new DefaultConsumerFlowStrategyContext(subscriptionId, client, messageCount));
            } else {
              LOGGER.debug(
                  "Could not find stream subscription {} or subscription closing, not providing credits",
                  subscriptionId & 0xFF);
              processCallback = null;
            }
            return processCallback;
          };

      CreditNotification creditNotification =
          (subscriptionId, responseCode) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            String stream = subscriptionTracker == null ? "?" : subscriptionTracker.stream;
            LOGGER.debug(
                "Received credit notification for subscription {} (stream '{}'): {}",
                subscriptionId & 0xFF,
                stream,
                Utils.formatConstant(responseCode));
          };

      MessageListener messageListener =
          (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext, message) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null) {
              subscriptionTracker.offset = offset;
              subscriptionTracker.hasReceivedSomething = true;
              subscriptionTracker.messageHandler.handle(
                  new MessageHandlerContext(
                      offset,
                      chunkTimestamp,
                      committedChunkId,
                      subscriptionTracker.consumer,
                      (ConsumerFlowStrategy.MessageProcessedCallback) chunkContext),
                  message);
            } else {
              LOGGER.debug(
                  "Could not find stream subscription {} in manager {}, node {} for message listener",
                  subscriptionId,
                  this.id,
                  nameReference.get());
            }
          };
      MessageIgnoredListener messageIgnoredListener =
          (subscriptionId, offset, chunkTimestamp, committedChunkId, chunkContext) -> {
            SubscriptionTracker subscriptionTracker =
                subscriptionTrackers.get(subscriptionId & 0xFF);
            if (subscriptionTracker != null) {
              // message at the beginning of the first chunk is ignored
              // we "simulate" the processing
              MessageHandlerContext messageHandlerContext =
                  new MessageHandlerContext(
                      offset,
                      chunkTimestamp,
                      committedChunkId,
                      subscriptionTracker.consumer,
                      (ConsumerFlowStrategy.MessageProcessedCallback) chunkContext);
              ((ConsumerFlowStrategy.MessageProcessedCallback) chunkContext)
                  .processed(messageHandlerContext);
            } else {
              LOGGER.debug(
                  "Could not find stream subscription {} in manager {}, node {} for message ignored listener",
                  subscriptionId,
                  this.id,
                  nameReference.get());
            }
          };
      ShutdownListener shutdownListener =
          shutdownContext -> {
            if (clientInitializedInManager.get()) {
              this.closed.set(true);
              managers.remove(this);
            }
            if (shutdownContext.isShutdownUnexpected()) {
              LOGGER.debug(
                  "Unexpected shutdown notification on subscription connection {}, scheduling consumers re-assignment",
                  nameReference.get());
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
                          nameReference.get()));
            }
          };
      MetadataListener metadataListener =
          (stream, code) -> {
            LOGGER.debug(
                "Received metadata notification for '{}', stream is likely to have become unavailable",
                stream);
            Set<SubscriptionTracker> affectedSubscriptions;

            this.subscriptionManagerLock.lock();
            try {
              Set<SubscriptionTracker> subscriptions = streamToStreamSubscriptions.remove(stream);
              if (subscriptions != null && !subscriptions.isEmpty()) {
                List<SubscriptionTracker> newSubscriptions = createSubscriptionTrackerList();
                for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
                  newSubscriptions.set(i, subscriptionTrackers.get(i));
                }
                for (SubscriptionTracker subscription : subscriptions) {
                  LOGGER.debug(
                      "Subscription {} ({}) was at offset {} (received something? {})",
                      subscription.subscriptionIdInClient,
                      subscription.label(),
                      subscription.offset,
                      subscription.hasReceivedSomething);
                  newSubscriptions.set(subscription.subscriptionIdInClient & 0xFF, null);
                  subscription.consumer.setSubscriptionClient(null);
                }
                this.setSubscriptionTrackers(newSubscriptions);
              }
              affectedSubscriptions = subscriptions;
            } finally {
              this.subscriptionManagerLock.unlock();
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
                                "Trying to move {} subscription(s) (stream '{}')",
                                affectedSubscriptions.size(),
                                stream);
                            assignConsumersToStream(
                                affectedSubscriptions,
                                stream,
                                metadataUpdateBackOffDelayPolicy(),
                                true);
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
          new ClientFactoryContext(
              clientParameters
                  .clientProperty("connection_name", connectionName)
                  .chunkListener(chunkListener)
                  .creditNotification(creditNotification)
                  .messageListener(messageListener)
                  .messageIgnoredListener(messageIgnoredListener)
                  .shutdownListener(shutdownListener)
                  .metadataListener(metadataListener)
                  .consumerUpdateListener(consumerUpdateListener),
              keyForNode(targetNode),
              candidates.stream().map(BrokerWrapper::broker).collect(toList()));
      this.client = clientFactory.client(clientFactoryContext);
      this.node = brokerFromClient(this.client);
      this.name = keyForNode(this.node);
      nameReference.set(this.name);
      LOGGER.debug("creating subscription manager on {}", name);
      LOGGER.debug("Created consumer connection '{}'", connectionName);
      clientInitializedInManager.set(true);
    }

    private void assignConsumersToStream(
        Collection<SubscriptionTracker> subscriptions,
        String stream,
        BackOffDelayPolicy delayPolicy,
        boolean maybeCloseClient) {
      Runnable consumersClosingCallback =
          () -> {
            LOGGER.debug(
                "Running consumer closing callback after recovery failure, "
                    + "closing {} subscription(s)",
                subscriptions.size());
            for (SubscriptionTracker affectedSubscription : subscriptions) {
              try {
                affectedSubscription.consumer.closeAfterStreamDeletion();
              } catch (Exception e) {
                LOGGER.debug("Error while closing consumer: {}", e.getMessage());
              }
            }
          };

      AsyncRetry.asyncRetry(findCandidateNodes(stream))
          .description("Candidate lookup to consume from '%s'", stream)
          .scheduler(environment.scheduledExecutorService())
          .retry(ex -> !(ex instanceof StreamDoesNotExistException))
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(
              candidateNodes -> {
                List<BrokerWrapper> candidates = candidateNodes;
                if (candidates == null) {
                  LOGGER.debug("No candidate nodes to consume from '{}'", stream);
                  consumersClosingCallback.run();
                } else {
                  for (SubscriptionTracker affectedSubscription : subscriptions) {
                    maybeRecoverSubscription(candidates, affectedSubscription);
                  }
                  if (maybeCloseClient) {
                    this.closeIfEmpty();
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
                if (maybeCloseClient) {
                  this.closeIfEmpty();
                }
                return null;
              });
    }

    private List<SubscriptionTracker> createSubscriptionTrackerList() {
      List<SubscriptionTracker> newSubscriptions = new ArrayList<>(MAX_SUBSCRIPTIONS_PER_CLIENT);
      IntStream.range(0, MAX_SUBSCRIPTIONS_PER_CLIENT).forEach(i -> newSubscriptions.add(null));
      return newSubscriptions;
    }

    private void maybeRecoverSubscription(
        List<BrokerWrapper> candidates, SubscriptionTracker tracker) {
      if (tracker.compareAndSet(SubscriptionState.ACTIVE, SubscriptionState.RECOVERING)) {
        try {
          recoverSubscription(candidates, tracker);
        } catch (Exception e) {
          LOGGER.warn(
              "Error while recovering consumer tracker {}. Reason: {}",
              tracker.label(),
              Utils.exceptionMessage(e));
        }
      } else {
        LOGGER.debug(
            "Not recovering consumer tracker {}, state is {}, expected is {}",
            tracker.label(),
            tracker.state(),
            SubscriptionState.ACTIVE);
      }
    }

    private void recoverSubscription(List<BrokerWrapper> candidates, SubscriptionTracker tracker) {
      boolean reassignmentCompleted = false;
      while (!reassignmentCompleted) {
        try {
          if (tracker.consumer.isOpen()) {
            Broker broker = pickBroker(brokerPicker, candidates);
            LOGGER.debug("Using {} to resume consuming from {}", broker, tracker.stream);
            tracker.consumer.lock();
            try {
              if (tracker.consumer.isOpen()) {
                OffsetSpecification offsetSpecification;
                if (tracker.hasReceivedSomething) {
                  offsetSpecification = OffsetSpecification.offset(tracker.offset);
                } else {
                  offsetSpecification = tracker.initialOffsetSpecification;
                }
                addToManager(broker, candidates, tracker, offsetSpecification, false);
              }
            } finally {
              tracker.consumer.unlock();
            }
          } else {
            LOGGER.debug(
                "Not re-assigning consumer {} (stream '{}') because it has been closed",
                tracker.consumer.id(),
                tracker.stream);
          }
          reassignmentCompleted = true;
        } catch (ConnectionStreamException
            | ClientClosedException
            | StreamNotAvailableException e) {
          LOGGER.debug(
              "Consumer {} re-assignment on stream {} timed out or connection closed or stream not available, "
                  + "refreshing candidates and retrying",
              tracker.consumer.id(),
              tracker.stream);
          // maybe not a good candidate, let's refresh and retry for this one
          candidates =
              Utils.callAndMaybeRetry(
                  findCandidateNodes(tracker.stream),
                  ex -> !(ex instanceof StreamDoesNotExistException),
                  recoveryBackOffDelayPolicy(),
                  "Candidate lookup to consume from '%s' (subscription recovery)",
                  tracker.stream);
        } catch (StreamException e) {
          LOGGER.warn(
              "Stream error while re-assigning subscription from stream {} (name {})",
              tracker.stream,
              tracker.offsetTrackingReference,
              e);
          if (e.getCode() == RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS) {
            LOGGER.debug("Subscription ID already existing, retrying");
          } else {
            LOGGER.debug(
                "Not re-assigning consumer '{}' because of '{}'", tracker.label(), e.getMessage());
            reassignmentCompleted = true;
          }
        } catch (Exception e) {
          LOGGER.warn(
              "Error while re-assigning subscription from stream {} (name {})",
              tracker.stream,
              tracker.offsetTrackingReference,
              e);
          LOGGER.debug(
              "Not re-assigning consumer '{}' because of '{}'", tracker.label(), e.getMessage());
          reassignmentCompleted = true;
        }
      }
    }

    private void checkNotClosed() {
      if (!this.client.isOpen()) {
        throw new ClientClosedException();
      }
    }

    void add(
        SubscriptionTracker subscriptionTracker,
        OffsetSpecification offsetSpecification,
        boolean isInitialSubscription) {
      this.subscriptionManagerLock.lock();
      try {
        if (this.isFull()) {
          LOGGER.debug(
              "Cannot add subscription tracker for stream '{}', manager is full",
              subscriptionTracker.stream);
          throw new IllegalStateException("Cannot add subscription tracker, the manager is full");
        }
        if (this.isClosed()) {
          LOGGER.debug(
              "Cannot add subscription tracker for stream '{}', manager is closed",
              subscriptionTracker.stream);
          throw new IllegalStateException("Cannot add subscription tracker, the manager is closed");
        }

        checkNotClosed();

        byte subscriptionId =
            (byte) pickSlot(this.subscriptionTrackers, this.consumerIndexSequence);

        List<SubscriptionTracker> previousSubscriptions = this.subscriptionTrackers;

        LOGGER.debug(
            "Subscribing to {}, requested offset specification is {}, offset tracking reference is {}, properties are {}, "
                + "subscription ID is {}",
            subscriptionTracker.stream,
            offsetSpecification == null ? DEFAULT_OFFSET_SPECIFICATION : offsetSpecification,
            subscriptionTracker.offsetTrackingReference,
            subscriptionTracker.subscriptionProperties,
            subscriptionId);
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
                Utils.callAndMaybeRetry(
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
                  "Using offset {} to start consuming from {} with consumer {} "
                      + "(instead of {})",
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
              new DefaultSubscriptionContext(offsetSpecification, subscriptionTracker.stream);
          subscriptionTracker.subscriptionListener.preSubscribe(subscriptionContext);
          LOGGER.info(
              "Computed offset specification {}, offset specification used after subscription listener {}",
              offsetSpecification,
              subscriptionContext.offsetSpecification());

          checkNotClosed();
          Client.Response subscribeResponse =
              Utils.callAndMaybeRetry(
                  () ->
                      client.subscribe(
                          subscriptionId,
                          subscriptionTracker.stream,
                          subscriptionContext.offsetSpecification(),
                          subscriptionTracker.flowStrategy.initialCredits(),
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
            if (subscribeResponse.getResponseCode()
                == RESPONSE_CODE_SUBSCRIPTION_ID_ALREADY_EXISTS) {
              if (LOGGER.isDebugEnabled()) {
                SubscriptionTracker initialTracker = previousSubscriptions.get(subscriptionId);
                LOGGER.debug("Subscription ID already exists");
                LOGGER.debug(
                    "Initial tracker with sub ID {}: consumer {}, stream {}, name {}",
                    subscriptionId,
                    initialTracker.consumer.id(),
                    initialTracker.stream,
                    initialTracker.offsetTrackingReference);
              }
            }
            throw convertCodeToException(
                subscribeResponse.getResponseCode(), subscriptionTracker.stream, () -> message);
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
        LOGGER.debug("Subscribed to '{}'", subscriptionTracker.stream);
      } finally {
        this.subscriptionManagerLock.unlock();
      }
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

    void remove(SubscriptionTracker subscriptionTracker) {
      Utils.lock(
          this.subscriptionManagerLock,
          () -> {
            byte subscriptionIdInClient = subscriptionTracker.subscriptionIdInClient;
            try {
              Client.Response unsubscribeResponse =
                  Utils.callAndMaybeRetry(
                      () -> {
                        if (client.isOpen()) {
                          return client.unsubscribe(subscriptionIdInClient);
                        } else {
                          return Client.responseOk();
                        }
                      },
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

            this.setSubscriptionTrackers(
                update(this.subscriptionTrackers, subscriptionIdInClient, null));
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
            closeIfEmpty();
          });
    }

    private List<SubscriptionTracker> update(
        List<SubscriptionTracker> original, byte index, SubscriptionTracker newValue) {
      List<SubscriptionTracker> newSubcriptions = createSubscriptionTrackerList();
      int intIndex = index & 0xFF;
      for (int i = 0; i < MAX_SUBSCRIPTIONS_PER_CLIENT; i++) {
        newSubcriptions.set(i, i == intIndex ? newValue : original.get(i));
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

    boolean isClosed() {
      if (!this.client.isOpen()) {
        this.close();
      }
      return this.closed.get();
    }

    void closeIfEmpty() {
      Utils.lock(
          this.subscriptionManagerLock,
          () -> {
            if (this.isEmpty()) {
              this.close();
            }
          });
    }

    void close() {
      Utils.lock(
          this.subscriptionManagerLock,
          () -> {
            if (this.closed.compareAndSet(false, true)) {
              managers.remove(this);
              LOGGER.debug(
                  "Closing consumer subscription manager on {}, id {}", this.name, this.id);
              if (this.client != null && this.client.isOpen()) {
                for (int i = 0; i < this.subscriptionTrackers.size(); i++) {
                  SubscriptionTracker tracker = this.subscriptionTrackers.get(i);
                  if (tracker != null) {
                    try {
                      if (this.client.isOpen() && tracker.consumer.isOpen()) {
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

                if (this.client.isOpen()) {
                  this.client.close();
                }
              }
            }
          });
    }

    @Override
    public int compareTo(ClientSubscriptionsManager o) {
      return Long.compare(this.id, o.id);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClientSubscriptionsManager that = (ClientSubscriptionsManager) o;
      return id == that.id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }

  private static final class DefaultSubscriptionContext implements SubscriptionContext {

    private volatile OffsetSpecification offsetSpecification;
    private final String name;

    private DefaultSubscriptionContext(
        OffsetSpecification computedOffsetSpecification, String name) {
      this.offsetSpecification = computedOffsetSpecification;
      this.name = name;
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
    public String stream() {
      return this.name;
    }

    @Override
    public String toString() {
      return "SubscriptionContext{" + "offsetSpecification=" + offsetSpecification + '}';
    }
  }

  private static final Predicate<Exception> RETRY_ON_TIMEOUT =
      e -> e instanceof TimeoutStreamException;

  private static class ClientClosedException extends StreamException {

    public ClientClosedException() {
      super("Client already closed");
    }
  }

  private static class DefaultConsumerFlowStrategyContext implements ConsumerFlowStrategy.Context {

    private final byte subscriptionId;
    private final Client client;
    private final long messageCount;

    private DefaultConsumerFlowStrategyContext(
        byte subscriptionId, Client client, long messageCount) {
      this.subscriptionId = subscriptionId;
      this.client = client;
      this.messageCount = messageCount;
    }

    @Override
    public void credits(int credits) {
      try {
        client.credit(subscriptionId, credits);
      } catch (Exception e) {
        LOGGER.info(
            "Error while providing {} credit(s) to subscription {}: {}",
            credits,
            subscriptionId,
            e.getMessage());
      }
    }

    @Override
    public long messageCount() {
      return messageCount;
    }
  }

  static <T> int pickSlot(List<T> list, AtomicInteger sequence) {
    int index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    while (list.get(index) != null) {
      index = Integer.remainderUnsigned(sequence.getAndIncrement(), MAX_SUBSCRIPTIONS_PER_CLIENT);
    }
    return index;
  }

  private static List<Broker> keepReplicasIfPossible(Collection<BrokerWrapper> brokers) {
    if (brokers.size() > 1) {
      return brokers.stream()
          .filter(w -> !w.isLeader())
          .map(BrokerWrapper::broker)
          .collect(toList());
    } else {
      return brokers.stream().map(BrokerWrapper::broker).collect(toList());
    }
  }

  static Broker pickBroker(
      Function<List<Broker>, Broker> picker, Collection<BrokerWrapper> candidates) {
    return picker.apply(keepReplicasIfPossible(candidates));
  }
}
