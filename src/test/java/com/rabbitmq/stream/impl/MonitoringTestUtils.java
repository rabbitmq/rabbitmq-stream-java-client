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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class MonitoringTestUtils {

  private static final Gson GSON = new Gson();

  private static <T> List<T> arrayToList(T[] array) {
    if (array == null || array.length == 0) {
      return Collections.emptyList();
    } else {
      return Arrays.asList(array);
    }
  }

  static List<ProducersPoolInfo> extract(ProducersCoordinator coordinator) {
    Type type = new TypeToken<List<ProducersPoolInfo>>() {}.getType();
    return GSON.fromJson(coordinator.toString(), type);
  }

  static ConsumerCoordinatorInfo extract(ConsumersCoordinator coordinator) {
    return GSON.fromJson(coordinator.toString(), ConsumerCoordinatorInfo.class);
  }

  static EnvironmentInfo extract(Environment environment) {
    return GSON.fromJson(environment.toString(), EnvironmentInfo.class);
  }

  static OffsetTrackingInfo extract(OffsetTrackingCoordinator coordinator) {
    return GSON.fromJson(coordinator.toString(), OffsetTrackingInfo.class);
  }

  static ProducerInfo extract(Producer producer) {
    return GSON.fromJson(producer.toString(), ProducerInfo.class);
  }

  static ConsumerInfo extract(Consumer consumer) {
    return GSON.fromJson(consumer.toString(), ConsumerInfo.class);
  }

  public static class EnvironmentInfo {

    private final String[] locators;
    private final ProducersPoolInfo[] producers;
    private final ConsumerCoordinatorInfo consumers;

    public EnvironmentInfo(
        String[] locators, ProducersPoolInfo[] producers, ConsumerCoordinatorInfo consumers) {
      this.locators = locators;
      this.producers = producers;
      this.consumers = consumers;
    }

    public String[] getLocators() {
      return locators;
    }

    public ConsumerCoordinatorInfo getConsumers() {
      return this.consumers;
    }

    public List<ProducersPoolInfo> getProducers() {
      return arrayToList(this.producers);
    }

    @Override
    public String toString() {
      return "EnvironmentInfo{"
          + "locators='"
          + Arrays.toString(locators)
          + '\''
          + ", producers="
          + Arrays.toString(producers)
          + ", consumers="
          + consumers
          + '}';
    }
  }

  public static class ConsumerCoordinatorInfo {

    private final int subscription_count;
    private final ConsumerManager[] clients;

    public ConsumerCoordinatorInfo(int subscription_count, ConsumerManager[] clients) {
      this.subscription_count = subscription_count;
      this.clients = clients;
    }

    boolean isEmpty() {
      return this.clients.length == 0;
    }

    Set<String> nodesConnected() {
      return Arrays.stream(this.clients).map(m -> m.node).collect(Collectors.toSet());
    }

    List<ConsumerManager> clients() {
      return Arrays.asList(this.clients);
    }

    int consumerCount() {
      return Arrays.stream(this.clients).mapToInt(ConsumerManager::getConsumerCount).sum();
    }
  }

  public static class ConsumerManager {

    private final long id;
    private final String node;
    private final int consumer_count;

    public ConsumerManager(long id, String node, int consumer_count) {
      this.id = id;
      this.node = node;
      this.consumer_count = consumer_count;
    }

    public int getConsumerCount() {
      return consumer_count;
    }

    @Override
    public String toString() {
      return "ConsumerManager{"
          + "id="
          + id
          + ", node='"
          + node
          + '\''
          + ", consumer_count="
          + consumer_count
          + '}';
    }
  }

  public static class ProducersPoolInfo {

    private final String broker;
    private final ProducerManager[] clients;

    public ProducersPoolInfo(String broker, ProducerManager[] clients) {
      this.broker = broker;
      this.clients = clients;
    }

    public String getBroker() {
      return broker;
    }

    public List<ProducerManager> getClients() {
      return arrayToList(this.clients);
    }

    @Override
    public String toString() {
      return "ProducersPoolInfo{"
          + "broker='"
          + broker
          + '\''
          + ", clients="
          + Arrays.toString(clients)
          + '}';
    }
  }

  public static class ProducerManager {

    private final int producer_count;
    private final int tracking_consumer_count;

    public ProducerManager(int producerCount, int tracking_consumer_count) {
      this.producer_count = producerCount;
      this.tracking_consumer_count = tracking_consumer_count;
    }

    public int getProducerCount() {
      return producer_count;
    }

    public int getTrackingConsumerCount() {
      return this.tracking_consumer_count;
    }

    @Override
    public String toString() {
      return "ProducerManager{"
          + "producerCount="
          + producer_count
          + ", trackingConsumerCount= "
          + tracking_consumer_count
          + '}';
    }
  }

  static class ProducerInfo {

    private final long id;
    private final String stream;
    private final String publishing_client;

    ProducerInfo(long id, String stream, String publishing_client) {
      this.id = id;
      this.stream = stream;
      this.publishing_client = publishing_client;
    }

    public long getId() {
      return id;
    }

    public String getStream() {
      return stream;
    }

    public String getPublishingClient() {
      return publishing_client;
    }
  }

  static class ConsumerInfo {

    private final long id;
    private final String stream;
    private final String subscription_client;
    private final String tracking_client;

    ConsumerInfo(long id, String stream, String subscription_client, String tracking_client) {
      this.id = id;
      this.stream = stream;
      this.subscription_client = subscription_client;
      this.tracking_client = tracking_client;
    }

    public long getId() {
      return id;
    }

    public String getStream() {
      return stream;
    }

    public String getSubscriptionClient() {
      return subscription_client;
    }

    public String getTrackingClient() {
      return tracking_client;
    }
  }

  static class OffsetTrackingInfo {

    private final int tracker_count;

    OffsetTrackingInfo(int tracker_count) {
      this.tracker_count = tracker_count;
    }

    int getTrackerCount() {
      return tracker_count;
    }
  }
}
