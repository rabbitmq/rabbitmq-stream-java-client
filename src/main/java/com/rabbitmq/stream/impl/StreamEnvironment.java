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

import static com.rabbitmq.stream.impl.AsyncRetry.asyncRetry;
import static com.rabbitmq.stream.impl.ThreadUtils.threadFactory;
import static com.rabbitmq.stream.impl.Utils.*;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import com.rabbitmq.stream.*;
import com.rabbitmq.stream.MessageHandler.Context;
import com.rabbitmq.stream.compression.CompressionCodecFactory;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Client.ShutdownListener;
import com.rabbitmq.stream.impl.Client.StreamStatsResponse;
import com.rabbitmq.stream.impl.OffsetTrackingCoordinator.Registration;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.StreamEnvironmentBuilder.DefaultTlsConfiguration;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.sasl.CredentialsProvider;
import com.rabbitmq.stream.sasl.UsernamePasswordCredentialsProvider;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.net.ssl.SSLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StreamEnvironment implements Environment {

  private static final Logger LOGGER = LoggerFactory.getLogger(StreamEnvironment.class);

  private final EventLoopGroup eventLoopGroup;
  private final ScheduledExecutorService scheduledExecutorService;
  private final ScheduledExecutorService locatorReconnectionScheduledExecutorService;
  private final boolean privateScheduleExecutorService;
  private final Client.ClientParameters clientParametersPrototype;
  private final List<Address> addresses;
  private final List<StreamProducer> producers = new CopyOnWriteArrayList<>();
  private final List<StreamConsumer> consumers = new CopyOnWriteArrayList<>();
  private final Codec codec;
  private final BackOffDelayPolicy recoveryBackOffDelayPolicy;
  private final BackOffDelayPolicy topologyUpdateBackOffDelayPolicy;
  private final ConsumersCoordinator consumersCoordinator;
  private final ProducersCoordinator producersCoordinator;
  private final OffsetTrackingCoordinator offsetTrackingCoordinator;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AddressResolver addressResolver;
  private final Clock clock = new Clock();
  private final ScheduledFuture<?> clockRefreshFuture;
  private final ByteBufAllocator byteBufAllocator;
  private final AtomicBoolean locatorsInitialized = new AtomicBoolean(false);
  private final Runnable locatorInitializationSequence;
  private final List<Locator> locators;
  private final ExecutorServiceFactory executorServiceFactory;
  private final ObservationCollector<?> observationCollector;

  @SuppressFBWarnings("CT_CONSTRUCTOR_THROW")
  StreamEnvironment(
      ScheduledExecutorService scheduledExecutorService,
      Client.ClientParameters clientParametersPrototype,
      List<URI> uris,
      BackOffDelayPolicy recoveryBackOffDelayPolicy,
      BackOffDelayPolicy topologyBackOffDelayPolicy,
      AddressResolver addressResolver,
      int maxProducersByConnection,
      int maxTrackingConsumersByConnection,
      int maxConsumersByConnection,
      DefaultTlsConfiguration tlsConfiguration,
      ByteBufAllocator byteBufAllocator,
      boolean lazyInit,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory,
      ObservationCollector<?> observationCollector,
      boolean forceReplicaForConsumers,
      boolean forceLeaderForProducers,
      Duration producerNodeRetryDelay,
      Duration consumerNodeRetryDelay,
      int expectedLocatorCount) {
    this.recoveryBackOffDelayPolicy = recoveryBackOffDelayPolicy;
    this.topologyUpdateBackOffDelayPolicy = topologyBackOffDelayPolicy;
    this.byteBufAllocator = byteBufAllocator;
    clientParametersPrototype = clientParametersPrototype.byteBufAllocator(byteBufAllocator);
    clientParametersPrototype = maybeSetUpClientParametersFromUris(uris, clientParametersPrototype);

    this.observationCollector = observationCollector;

    boolean tls;
    if (tlsConfiguration != null && tlsConfiguration.enabled()) {
      tls = true;
      try {
        SslContext sslContext =
            tlsConfiguration.sslContext() == null
                ? SslContextBuilder.forClient().build()
                : tlsConfiguration.sslContext();

        clientParametersPrototype.sslContext(sslContext);
        clientParametersPrototype.tlsHostnameVerification(
            tlsConfiguration.hostnameVerificationEnabled());

      } catch (SSLException e) {
        throw new StreamException("Error while creating Netty SSL context", e);
      }
    } else {
      tls = false;
    }

    if (uris.isEmpty()) {
      this.addresses =
          Collections.singletonList(
              new Address(clientParametersPrototype.host(), clientParametersPrototype.port()));
    } else {
      int defaultPort = tls ? Client.DEFAULT_TLS_PORT : Client.DEFAULT_PORT;
      this.addresses =
          uris.stream()
              .map(
                  uriItem ->
                      new Address(
                          uriItem.getHost() == null ? "localhost" : uriItem.getHost(),
                          uriItem.getPort() == -1 ? defaultPort : uriItem.getPort()))
              .collect(toList());
    }

    AddressResolver addressResolverToUse = addressResolver;
    if (this.addresses.size() == 1
        && "localhost".equals(this.addresses.get(0).host())
        && addressResolver == DEFAULT_ADDRESS_RESOLVER) {
      CredentialsProvider credentialsProvider = clientParametersPrototype.credentialsProvider();
      if (credentialsProvider instanceof UsernamePasswordCredentialsProvider) {
        String username = ((UsernamePasswordCredentialsProvider) credentialsProvider).getUsername();
        if (DEFAULT_USERNAME.equals(username)) {
          Address address = new Address("localhost", clientParametersPrototype.port());
          Set<Address> passedInAddresses = ConcurrentHashMap.newKeySet();
          addressResolverToUse =
              addr -> {
                passedInAddresses.add(addr);
                if (passedInAddresses.size() > 1) {
                  LOGGER.warn("Assumed development environment but it seems incorrect.");
                  passedInAddresses.clear();
                }
                return address;
              };
          LOGGER.info(
              "Connecting to localhost with {} user, assuming development environment",
              DEFAULT_USERNAME);
          LOGGER.info("Using address resolver to always connect to localhost");
        }
      }
    }

    this.addressResolver = addressResolverToUse;

    int locatorCount;
    if (expectedLocatorCount > 0) {
      locatorCount = expectedLocatorCount;
    } else {
      locatorCount = Math.min(this.addresses.size(), 3);
    }
    LOGGER.debug("Using {} locator connection(s)", locatorCount);

    List<Locator> lctrs =
        IntStream.range(0, locatorCount)
            .mapToObj(
                i -> {
                  Address addr = this.addresses.get(i % this.addresses.size());
                  return new Locator(i, addr);
                })
            .collect(toList());
    this.locators = List.copyOf(lctrs);

    this.executorServiceFactory =
        new DefaultExecutorServiceFactory(
            this.addresses.size(), 1, "rabbitmq-stream-locator-connection-");

    if (clientParametersPrototype.eventLoopGroup == null) {
      this.eventLoopGroup = new NioEventLoopGroup();
      this.clientParametersPrototype =
          clientParametersPrototype.duplicate().eventLoopGroup(this.eventLoopGroup);
    } else {
      this.eventLoopGroup = null;
      this.clientParametersPrototype =
          clientParametersPrototype
              .duplicate()
              .eventLoopGroup(clientParametersPrototype.eventLoopGroup);
    }
    ScheduledExecutorService executorService;
    if (scheduledExecutorService == null) {
      int threads = Runtime.getRuntime().availableProcessors();
      LOGGER.debug("Creating scheduled executor service with {} thread(s)", threads);
      ThreadFactory threadFactory = threadFactory("rabbitmq-stream-environment-scheduler-");
      executorService = Executors.newScheduledThreadPool(threads, threadFactory);
      this.privateScheduleExecutorService = true;
    } else {
      executorService = scheduledExecutorService;
      this.privateScheduleExecutorService = false;
    }
    this.scheduledExecutorService = executorService;

    this.producersCoordinator =
        new ProducersCoordinator(
            this,
            maxProducersByConnection,
            maxTrackingConsumersByConnection,
            connectionNamingStrategy,
            coordinatorClientFactory(this, producerNodeRetryDelay),
            forceLeaderForProducers);
    this.consumersCoordinator =
        new ConsumersCoordinator(
            this,
            maxConsumersByConnection,
            connectionNamingStrategy,
            coordinatorClientFactory(this, consumerNodeRetryDelay),
            forceReplicaForConsumers,
            Utils.brokerPicker());
    this.offsetTrackingCoordinator = new OffsetTrackingCoordinator(this);

    ThreadFactory threadFactory = threadFactory("rabbitmq-stream-environment-locator-scheduler-");
    this.locatorReconnectionScheduledExecutorService =
        Executors.newScheduledThreadPool(this.locators.size(), threadFactory);

    ClientParameters clientParametersForInit = locatorParametersCopy();
    Runnable locatorInitSequence =
        () -> {
          RuntimeException lastException = null;
          for (int i = 0; i < locators.size(); i++) {
            Address address = addresses.get(i % addresses.size());
            Locator locator = locator(i);
            address = addressResolver.resolve(address);
            String connectionName = connectionNamingStrategy.apply(ClientConnectionType.LOCATOR);
            Client.ClientParameters locatorParameters =
                clientParametersForInit
                    .duplicate()
                    .host(address.host())
                    .port(address.port())
                    .clientProperty("connection_name", connectionName)
                    .shutdownListener(
                        shutdownListener(locator, connectionNamingStrategy, clientFactory));
            try {
              Client client = clientFactory.apply(locatorParameters);
              locator.client(client);
              LOGGER.debug("Created locator connection '{}'", connectionName);
              LOGGER.debug("Locator connected to {}", address);
            } catch (RuntimeException e) {
              LOGGER.debug("Error while try to connect to {}: {}", address, e.getMessage());
              lastException = e;
            }
          }
          if (this.locators.stream().allMatch(Locator::isNotSet)) {
            throw lastException == null
                ? new StreamException("Not locator available")
                : lastException;
          } else {
            this.locators.forEach(
                l -> {
                  if (l.isNotSet()) {
                    ShutdownListener shutdownListener =
                        shutdownListener(l, connectionNamingStrategy, clientFactory);
                    Client.ClientParameters newLocatorParameters =
                        this.locatorParametersCopy().shutdownListener(shutdownListener);
                    scheduleLocatorConnection(
                        newLocatorParameters,
                        this.addressResolver,
                        l,
                        connectionNamingStrategy,
                        clientFactory,
                        this.locatorReconnectionScheduledExecutorService,
                        this.recoveryBackOffDelayPolicy,
                        l.label());
                  }
                });
          }
        };
    if (lazyInit) {
      this.locatorInitializationSequence = locatorInitSequence;
    } else {
      locatorInitSequence.run();
      locatorsInitialized.set(true);
      this.locatorInitializationSequence = () -> {};
    }
    this.codec =
        clientParametersPrototype.codec() == null
            ? Codecs.DEFAULT
            : clientParametersPrototype.codec();
    this.clockRefreshFuture =
        this.scheduledExecutorService.scheduleAtFixedRate(
            namedRunnable(this.clock::refresh, "Background clock refresh"), 1, 1, SECONDS);
  }

  private ShutdownListener shutdownListener(
      Locator locator,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory) {
    AtomicReference<Client.ShutdownListener> shutdownListenerReference = new AtomicReference<>();
    Client.ShutdownListener shutdownListener =
        shutdownContext -> {
          String label = locator.label();
          LOGGER.debug("Locator {} disconnected", label);
          if (shutdownContext.isShutdownUnexpected()) {
            locator.client(null);
            BackOffDelayPolicy delayPolicy = recoveryBackOffDelayPolicy;
            LOGGER.debug(
                "Unexpected locator disconnection for on '{}', scheduling recovery with {}",
                label,
                delayPolicy);
            Client.ClientParameters newLocatorParameters =
                this.locatorParametersCopy().shutdownListener(shutdownListenerReference.get());
            scheduleLocatorConnection(
                newLocatorParameters,
                this.addressResolver,
                locator,
                connectionNamingStrategy,
                clientFactory,
                this.locatorReconnectionScheduledExecutorService,
                delayPolicy,
                label);
          } else {
            LOGGER.debug("Locator connection '{}' closing normally", label);
          }
        };
    shutdownListenerReference.set(shutdownListener);
    return shutdownListener;
  }

  private static void scheduleLocatorConnection(
      ClientParameters newLocatorParameters,
      AddressResolver addressResolver,
      Locator locator,
      Function<ClientConnectionType, String> connectionNamingStrategy,
      Function<Client.ClientParameters, Client> clientFactory,
      ScheduledExecutorService scheduler,
      BackOffDelayPolicy delayPolicy,
      String locatorLabel) {
    LOGGER.debug(
        "Scheduling locator '{}' connection with delay policy {}", locatorLabel, delayPolicy);
    try {
      asyncRetry(
              () -> {
                LOGGER.debug("Locator reconnection...");
                Address resolvedAddress = addressResolver.resolve(locator.address());
                String connectionName =
                    connectionNamingStrategy.apply(ClientConnectionType.LOCATOR);
                LOGGER.debug(
                    "Trying to reconnect locator on {}, with client connection name '{}'",
                    resolvedAddress,
                    connectionName);
                Client newLocator =
                    clientFactory.apply(
                        newLocatorParameters
                            .host(resolvedAddress.host())
                            .port(resolvedAddress.port())
                            .clientProperty("connection_name", connectionName));
                LOGGER.debug("Created locator connection '{}'", connectionName);
                LOGGER.debug("Locator connected on {}", resolvedAddress);
                return newLocator;
              })
          .description("Locator '%s' connection", locatorLabel)
          .scheduler(scheduler)
          .delayPolicy(delayPolicy)
          .build()
          .thenAccept(locator::client)
          .exceptionally(
              ex -> {
                LOGGER.debug("Locator connection failed", ex);
                return null;
              });
    } catch (Exception e) {
      LOGGER.debug("Error while scheduling locator '{}' reconnection", locatorLabel, e);
    }
  }

  private Locator locator(int i) {
    return this.locators.get(i);
  }

  private static String uriDecode(String s) {
    try {
      // URLDecode decodes '+' to a space, as for
      // form encoding. So protect plus signs.
      return URLDecoder.decode(s.replace("+", "%2B"), "US-ASCII");
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  Client.ClientParameters maybeSetUpClientParametersFromUris(
      List<URI> uris, Client.ClientParameters clientParametersPrototype) {
    if (uris.isEmpty()) {
      return clientParametersPrototype;
    } else {
      URI uri = uris.get(0);
      clientParametersPrototype = clientParametersPrototype.duplicate();
      String host = uri.getHost();
      if (host != null) {
        clientParametersPrototype.host(host);
      }

      int port = uri.getPort();
      if (port != -1) {
        clientParametersPrototype.port(port);
      }

      String userInfo = uri.getRawUserInfo();
      if (userInfo != null) {
        String[] userPassword = userInfo.split(":");
        if (userPassword.length > 2) {
          throw new IllegalArgumentException("Bad user info in URI " + userInfo);
        }

        clientParametersPrototype.username(uriDecode(userPassword[0]));
        if (userPassword.length == 2) {
          clientParametersPrototype.password(uriDecode(userPassword[1]));
        }
      }

      String path = uri.getRawPath();
      if (path != null && path.length() > 0) {
        if (path.indexOf('/', 1) != -1) {
          throw new IllegalArgumentException("Multiple segments in path of URI: " + path);
        }
        clientParametersPrototype.virtualHost(uriDecode(uri.getPath().substring(1)));
      }
      return clientParametersPrototype;
    }
  }

  public ByteBufAllocator byteBufAllocator() {
    return byteBufAllocator;
  }

  void maybeInitializeLocator() {
    if (this.locatorsInitialized.compareAndSet(false, true)) {
      try {
        this.locatorInitializationSequence.run();
      } catch (RuntimeException e) {
        this.locatorsInitialized.set(false);
        throw e;
      }
    }
  }

  @Override
  public StreamCreator streamCreator() {
    checkNotClosed();
    return new StreamStreamCreator(this);
  }

  @Override
  public void deleteStream(String stream) {
    checkNotClosed();
    this.maybeInitializeLocator();
    Client.Response response = this.locator().client().delete(stream);
    if (!response.isOk()) {
      throw new StreamException(
          "Error while deleting stream "
              + stream
              + " ("
              + formatConstant(response.getResponseCode())
              + ")",
          response.getResponseCode());
    }
  }

  @Override
  public void deleteSuperStream(String superStream) {
    checkNotClosed();
    this.maybeInitializeLocator();
    Client.Response response = this.locator().client().deleteSuperStream(superStream);
    if (!response.isOk()) {
      throw new StreamException(
          "Error while deleting super stream "
              + superStream
              + " ("
              + formatConstant(response.getResponseCode())
              + ")",
          response.getResponseCode());
    }
  }

  @Override
  public StreamStats queryStreamStats(String stream) {
    checkNotClosed();
    this.maybeInitializeLocator();
    StreamStatsResponse response =
        locatorOperation(
            Utils.namedFunction(
                client -> client.streamStats(stream), "Query stream stats on stream '%s'", stream));
    if (response.isOk()) {
      Map<String, Long> info = response.getInfo();
      BiFunction<String, String, LongSupplier> offsetSupplierLogic =
          (key, message) -> {
            if (!info.containsKey(key) || info.get(key) == -1) {
              return () -> {
                throw new NoOffsetException(message);
              };
            } else {
              try {
                long offset = info.get(key);
                return () -> offset;
              } catch (NumberFormatException e) {
                return () -> {
                  throw new NoOffsetException(message);
                };
              }
            }
          };
      LongSupplier firstOffsetSupplier =
          offsetSupplierLogic.apply("first_chunk_id", "No first offset for stream " + stream);
      LongSupplier committedOffsetSupplier =
          offsetSupplierLogic.apply(
              "committed_chunk_id", "No committed chunk ID for stream " + stream);
      return new DefaultStreamStats(firstOffsetSupplier, committedOffsetSupplier);
    } else {
      throw convertCodeToException(
          response.getResponseCode(),
          stream,
          () ->
              "Error while querying stream stats: "
                  + formatConstant(response.getResponseCode())
                  + ".");
    }
  }

  @Override
  public boolean streamExists(String stream) {
    checkNotClosed();
    this.maybeInitializeLocator();
    short responseCode =
        locatorOperation(
            Utils.namedFunction(
                client -> {
                  try {
                    return client.streamStats(stream).getResponseCode();
                  } catch (UnsupportedOperationException e) {
                    Map<String, Client.StreamMetadata> metadata = client.metadata(stream);
                    return metadata.get(stream).getResponseCode();
                  }
                },
                "Stream exists for stream '%s'",
                stream));
    if (responseCode == Constants.RESPONSE_CODE_OK) {
      return true;
    } else if (responseCode == Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
      return false;
    } else {
      throw convertCodeToException(
          responseCode,
          stream,
          () ->
              format(
                  "Unexpected result when checking if stream '%s' exists: %s.",
                  stream, formatConstant(responseCode)));
    }
  }

  private static class DefaultStreamStats implements StreamStats {

    private final LongSupplier firstOffsetSupplier, committedOffsetSupplier;

    private DefaultStreamStats(
        LongSupplier firstOffsetSupplier, LongSupplier committedOffsetSupplier) {
      this.firstOffsetSupplier = firstOffsetSupplier;
      this.committedOffsetSupplier = committedOffsetSupplier;
    }

    @Override
    public long firstOffset() {
      return firstOffsetSupplier.getAsLong();
    }

    @Override
    public long committedChunkId() {
      return committedOffsetSupplier.getAsLong();
    }

    @Override
    public String toString() {
      return "StreamStats{"
          + "firstOffset="
          + firstOffset()
          + ", committedOffset="
          + committedChunkId()
          + '}';
    }
  }

  @Override
  public ProducerBuilder producerBuilder() {
    checkNotClosed();
    return new StreamProducerBuilder(this);
  }

  void addProducer(StreamProducer producer) {
    this.producers.add(producer);
  }

  void removeProducer(StreamProducer producer) {
    this.producers.remove(producer);
  }

  void addConsumer(StreamConsumer consumer) {
    this.consumers.add(consumer);
  }

  void removeConsumer(StreamConsumer consumer) {
    this.consumers.remove(consumer);
  }

  @Override
  public ConsumerBuilder consumerBuilder() {
    checkNotClosed();
    return new StreamConsumerBuilder(this);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      for (StreamProducer producer : producers) {
        try {
          producer.closeFromEnvironment();
        } catch (Exception e) {
          LOGGER.warn("Error while closing producer, moving on to the next one", e);
        }
      }

      for (StreamConsumer consumer : consumers) {
        try {
          consumer.closeFromEnvironment();
        } catch (Exception e) {
          LOGGER.warn("Error while closing consumer, moving on to the next one", e);
        }
      }

      this.producersCoordinator.close();
      this.consumersCoordinator.close();
      this.offsetTrackingCoordinator.close();

      for (Locator locator : this.locators) {
        try {
          if (locator.isSet()) {
            locator.client().close();
            locator.client(null);
          }
        } catch (Exception e) {
          LOGGER.warn("Error while closing locator client", e);
        }
      }

      try {
        this.executorServiceFactory.close();
      } catch (Exception e) {
        LOGGER.info("Error while closing executor service factory: {}", e.getMessage());
      }

      this.clockRefreshFuture.cancel(false);
      if (privateScheduleExecutorService) {
        this.scheduledExecutorService.shutdownNow();
      }
      if (this.locatorReconnectionScheduledExecutorService != null) {
        this.locatorReconnectionScheduledExecutorService.shutdownNow();
      }
      try {
        if (this.eventLoopGroup != null
            && (!this.eventLoopGroup.isShuttingDown() || !this.eventLoopGroup.isShutdown())) {
          LOGGER.debug("Closing Netty event loop group");
          this.eventLoopGroup.shutdownGracefully(1, 10, SECONDS).get(10, SECONDS);
        }
      } catch (InterruptedException e) {
        LOGGER.info("Event loop group closing has been interrupted");
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        LOGGER.info("Event loop group closing failed", e);
      } catch (TimeoutException e) {
        LOGGER.info("Could not close event loop group in 10 seconds");
      }
    }
  }

  ScheduledExecutorService scheduledExecutorService() {
    return this.scheduledExecutorService;
  }

  void execute(Runnable task, String description, Object... args) {
    this.scheduledExecutorService().execute(namedRunnable(task, description, args));
  }

  BackOffDelayPolicy recoveryBackOffDelayPolicy() {
    return this.recoveryBackOffDelayPolicy;
  }

  BackOffDelayPolicy topologyUpdateBackOffDelayPolicy() {
    return this.topologyUpdateBackOffDelayPolicy;
  }

  CompressionCodecFactory compressionCodecFactory() {
    return this.clientParametersPrototype.compressionCodecFactory;
  }

  ObservationCollector<?> observationCollector() {
    return this.observationCollector;
  }

  Runnable registerConsumer(
      StreamConsumer consumer,
      String stream,
      OffsetSpecification offsetSpecification,
      String trackingReference,
      SubscriptionListener subscriptionListener,
      Runnable trackingClosingCallback,
      MessageHandler messageHandler,
      Map<String, String> subscriptionProperties,
      ConsumerFlowStrategy flowStrategy) {
    return this.consumersCoordinator.subscribe(
        consumer,
        stream,
        offsetSpecification,
        trackingReference,
        subscriptionListener,
        trackingClosingCallback,
        messageHandler,
        subscriptionProperties,
        flowStrategy);
  }

  Runnable registerProducer(StreamProducer producer, String reference, String stream) {
    return producersCoordinator.registerProducer(producer, reference, stream);
  }

  Locator locator() {
    if (LOGGER.isDebugEnabled()) {
      try {
        LOGGER.debug(
            "Locators: {}",
            this.locators.stream()
                .map(l -> l.label() + " is set " + l.isSet())
                .collect(Collectors.joining(", ")));
      } catch (Exception e) {
        LOGGER.debug("Error while listing locators: {}", e.getMessage());
      }
    }
    return this.locators.stream()
        .filter(Locator::isSet)
        .findAny()
        .orElseThrow(LocatorNotAvailableException::new);
  }

  <T> T locatorOperation(Function<Client, T> operation) {
    return locatorOperation(operation, this::locator, this.recoveryBackOffDelayPolicy);
  }

  static <T> T locatorOperation(
      Function<Client, T> operation,
      Supplier<Locator> locatorSupplier,
      BackOffDelayPolicy backOffDelayPolicy) {
    int maxAttempt = 3;
    int attempt = 0;
    boolean executed = false;
    Exception lastException = null;
    T result = null;
    LOGGER.debug("Starting locator operation '{}'", operation);
    long start = System.nanoTime();
    while (attempt < maxAttempt) {
      try {
        Locator locator = locatorSupplier.get();
        Client client = locator.client();
        LOGGER.debug(
            "Using locator {} on {}:{} to run operation '{}'",
            locator.id(),
            client.getHost(),
            client.getPort(),
            operation);
        result = operation.apply(client);
        LOGGER.debug(
            "Locator operation '{}' succeeded in {}",
            operation,
            Duration.ofNanos(System.nanoTime() - start));
        executed = true;
        break;
      } catch (LocatorNotAvailableException e) {
        Duration waitTime = backOffDelayPolicy.delay(attempt);
        LOGGER.debug(
            "No locator available for operation '{}', waiting for {} before retrying",
            operation,
            waitTime);
        attempt++;
        try {
          Thread.sleep(waitTime.toMillis());
        } catch (InterruptedException ex) {
          lastException = ex;
          Thread.currentThread().interrupt();
          break;
        }
      } catch (Exception e) {
        LOGGER.debug("Exception during locator operation '{}': {}", operation, exceptionMessage(e));
        lastException = e;
        break;
      }
    }
    if (!executed) {
      if (lastException == null) {
        throw new LocatorNotAvailableException();
      } else {
        throw new StreamException(
            "Could not execute operation after " + maxAttempt + " attempts", lastException);
      }
    }
    return result;
  }

  boolean filteringSupported() {
    return this.locatorOperation(Client::filteringSupported);
  }

  Clock clock() {
    return this.clock;
  }

  AddressResolver addressResolver() {
    return this.addressResolver;
  }

  Codec codec() {
    return this.codec;
  }

  Client.ClientParameters clientParametersCopy() {
    return this.clientParametersPrototype.duplicate();
  }

  private Client.ClientParameters locatorParametersCopy() {
    return this.clientParametersPrototype
        .duplicate()
        .executorServiceFactory(this.executorServiceFactory)
        .dispatchingExecutorServiceFactory(Utils.NO_OP_EXECUTOR_SERVICE_FACTORY);
  }

  TrackingConsumerRegistration registerTrackingConsumer(
      StreamConsumer streamConsumer, TrackingConfiguration configuration) {
    Runnable closingCallable = this.producersCoordinator.registerTrackingConsumer(streamConsumer);
    Registration offsetTrackingRegistration;
    if (this.offsetTrackingCoordinator.needTrackingRegistration(configuration)) {
      offsetTrackingRegistration =
          this.offsetTrackingCoordinator.registerTrackingConsumer(streamConsumer, configuration);
    } else {
      offsetTrackingRegistration = null;
    }

    Runnable closingSequence;
    if (offsetTrackingRegistration == null) {
      closingSequence = closingCallable;
    } else {
      closingSequence =
          () -> {
            try {
              LOGGER.debug("Executing offset tracking registration closing sequence ");
              offsetTrackingRegistration.closingCallback().run();
              LOGGER.debug("Offset tracking registration closing sequence executed");
            } catch (Exception e) {
              LOGGER.warn(
                  "Error while executing offset tracking registration closing sequence: {}",
                  e.getMessage());
            }
            closingCallable.run();
          };
    }

    return new TrackingConsumerRegistration(
        closingSequence,
        offsetTrackingRegistration == null
            ? null
            : offsetTrackingRegistration.postMessageProcessingCallback(),
        offsetTrackingRegistration == null
            ? Utils.NO_OP_LONG_CONSUMER
            : offsetTrackingRegistration.trackingCallback(),
        offsetTrackingRegistration == null
            ? Utils.NO_OP_LONG_SUPPLIER
            : offsetTrackingRegistration::flush);
  }

  @Override
  public String toString() {
    return "{ \"locators\" : ["
        + this.locators.stream().map(l -> quote(l.label())).collect(Collectors.joining(","))
        + "], "
        + Utils.jsonField("producer_client_count", this.producersCoordinator.clientCount())
        + ","
        + Utils.jsonField("consumer_client_count", this.consumersCoordinator.managerCount())
        + ","
        + "\"producers\" : "
        + this.producersCoordinator
        + ", \"consumers\" : "
        + this.consumersCoordinator
        + ", \"offset_tracking\" : "
        + this.offsetTrackingCoordinator
        + "}";
  }

  static class TrackingConsumerRegistration {

    private final Runnable closingCallback;
    private final Consumer<Context> postMessageProcessingCallback;
    private final LongConsumer trackingCallback;
    private final LongSupplier flushOperation;

    TrackingConsumerRegistration(
        Runnable closingCallback,
        Consumer<Context> postMessageProcessingCallback,
        LongConsumer trackingCallback,
        LongSupplier flushOperation) {
      this.closingCallback = closingCallback;
      this.postMessageProcessingCallback = postMessageProcessingCallback;
      this.trackingCallback = trackingCallback;
      this.flushOperation = flushOperation;
    }

    Runnable closingCallback() {
      return closingCallback;
    }

    LongConsumer trackingCallback() {
      return trackingCallback;
    }

    Consumer<Context> postMessageProcessingCallback() {
      return postMessageProcessingCallback;
    }

    long flush() {
      return this.flushOperation.getAsLong();
    }
  }

  static class LocatorNotAvailableException extends StreamException {

    public LocatorNotAvailableException() {
      super("Locator not available");
    }

    public LocatorNotAvailableException(long id) {
      super(String.format("Locator %d not available", id));
    }
  }

  private void checkNotClosed() {
    if (this.closed.get()) {
      throw new IllegalStateException("This environment instance has been closed");
    }
  }

  static class Locator {

    private final long id;
    private final Address address;
    private volatile Optional<Client> client;
    private volatile LocalDateTime lastChanged;

    Locator(long id, Address address) {
      this.id = id;
      this.address = address;
      this.client = Optional.empty();
      lastChanged = LocalDateTime.now();
      LOGGER.debug(
          "Locator wrapper '{}' created with no connection at {}", this.label(), lastChanged);
    }

    Locator client(Client client) {
      Client previous = this.nullableClient();
      this.client = Optional.ofNullable(client);
      LocalDateTime now = LocalDateTime.now();
      LOGGER.debug(
          "Locator wrapper '{}' updated from {} to {}, last changed {}, {} ago",
          this.label(),
          previous,
          client,
          this.lastChanged,
          Duration.between(this.lastChanged, now));
      lastChanged = now;
      return this;
    }

    private long id() {
      return this.id;
    }

    private boolean isNotSet() {
      return !this.isSet();
    }

    private boolean isSet() {
      return this.client.isPresent();
    }

    private Client client() {
      return this.client.orElseThrow(() -> new LocatorNotAvailableException(id));
    }

    private Client nullableClient() {
      return this.client.orElse(null);
    }

    private Address address() {
      return this.address;
    }

    private String label() {
      Client c = this.nullableClient();
      if (c == null) {
        return String.format("%s:%d (id %d)", address.host(), address.port(), this.id);
      } else {
        return String.format(
            "%s:%d [id %d, advertised %s:%d]",
            c.getHost(),
            c.getPort(),
            this.id(),
            c.serverAdvertisedHost(),
            c.serverAdvertisedPort());
      }
    }

    @Override
    public String toString() {
      return "Locator{" + "address=" + address + ", client=" + client + '}';
    }
  }
}
