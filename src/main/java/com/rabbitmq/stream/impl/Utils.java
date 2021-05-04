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

import com.rabbitmq.stream.Address;
import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.impl.Client.ClientParameters;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class Utils {

  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

  static final LongConsumer NO_OP_LONG_CONSUMER = someLong -> {};

  private Utils() {}

  static Runnable makeIdempotent(Runnable action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return () -> {
      if (executed.compareAndSet(false, true)) {
        action.run();
      }
    };
  }

  static <T> Consumer<T> makeIdempotent(Consumer<T> action) {
    AtomicBoolean executed = new AtomicBoolean(false);
    return t -> {
      if (executed.compareAndSet(false, true)) {
        action.accept(t);
      }
    };
  }

  private static final Map<Short, String> CONSTANT_LABELS;

  static {
    Map<Short, String> labels = new HashMap<>();
    Arrays.stream(Constants.class.getDeclaredFields())
        .filter(f -> f.getName().startsWith("RESPONSE_CODE_") || f.getName().startsWith("CODE_"))
        .forEach(
            field -> {
              try {
                labels.put(
                    field.getShort(null),
                    field.getName().replace("RESPONSE_CODE_", "").replace("CODE_", ""));
              } catch (IllegalAccessException e) {
                LOGGER.info("Error while trying to access field Constants." + field.getName());
              }
            });
    CONSTANT_LABELS = Collections.unmodifiableMap(labels);
  }

  static String formatConstant(short value) {
    return value + " (" + CONSTANT_LABELS.getOrDefault(value, "UNKNOWN") + ")";
  }

  static short encodeRequestCode(Short code) {
    return code;
  }

  static short extractResponseCode(Short code) {
    return (short) (code & 0B0111_1111_1111_1111);
  }

  static short encodeResponseCode(Short code) {
    return (short) (code | 0B1000_0000_0000_0000);
  }

  static ClientFactory coordinatorClientFactory(StreamEnvironment environment) {
    return context -> {
      ClientParameters parametersCopy = context.parameters().duplicate();
      Address address = new Address(parametersCopy.host, parametersCopy.port);
      address = environment.addressResolver().resolve(address);
      parametersCopy.host(address.host()).port(address.port());

      if (context.key() == null) {
        throw new IllegalArgumentException("A key is necessary to create the client connection");
      }

      return Utils.connectToAdvertisedNodeClientFactory(
              context.key(), context1 -> new Client(context1.parameters()))
          .client(Utils.ClientFactoryContext.fromParameters(parametersCopy).key(context.key()));
    };
  }

  static ClientFactory connectToAdvertisedNodeClientFactory(
      String expectedAdvertisedHostPort, ClientFactory clientFactory) {
    return connectToAdvertisedNodeClientFactory(
        expectedAdvertisedHostPort, clientFactory, ExactNodeRetryClientFactory.RETRY_INTERVAL);
  }

  static ClientFactory connectToAdvertisedNodeClientFactory(
      String expectedAdvertisedHostPort, ClientFactory clientFactory, Duration retryInterval) {
    return new ExactNodeRetryClientFactory(
        clientFactory,
        client -> {
          String currentKey = client.serverAdvertisedHost() + ":" + client.serverAdvertisedPort();
          boolean success = expectedAdvertisedHostPort.equals(currentKey);
          LOGGER.debug(
              "Expected client {}, got {}: {}",
              expectedAdvertisedHostPort,
              currentKey,
              success ? "success" : "failure");
          return success;
        },
        retryInterval);
  }

  static class ExactNodeRetryClientFactory implements ClientFactory {

    private static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);

    private final ClientFactory delegate;
    private final Predicate<Client> condition;
    private final Duration retryInterval;

    ExactNodeRetryClientFactory(
        ClientFactory delegate, Predicate<Client> condition, Duration retryInterval) {
      this.delegate = delegate;
      this.condition = condition;
      this.retryInterval = retryInterval;
    }

    @Override
    public Client client(ClientFactoryContext context) {
      while (true) {
        Client client = this.delegate.client(context);
        if (condition.test(client)) {
          return client;
        } else {
          try {
            client.close();
          } catch (Exception e) {
            LOGGER.warn("Error while trying to close client", e);
          }
        }
        try {
          Thread.sleep(this.retryInterval.toMillis());
        } catch (InterruptedException e) {
          Thread.interrupted();
          return null;
        }
      }
    }
  }

  interface ClientFactory {

    Client client(ClientFactoryContext context);
  }

  static class ClientFactoryContext {

    private ClientParameters parameters;
    private String key;

    static ClientFactoryContext fromParameters(ClientParameters parameters) {
      return new ClientFactoryContext().parameters(parameters);
    }

    ClientParameters parameters() {
      return parameters;
    }

    ClientFactoryContext parameters(ClientParameters parameters) {
      this.parameters = parameters;
      return this;
    }

    String key() {
      return key;
    }

    ClientFactoryContext key(String key) {
      this.key = key;
      return this;
    }
  }
}
