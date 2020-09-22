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

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.rabbitmq.stream.Environment;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MonitoringTestUtils {

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

  static List<ConsumersPoolInfo> extract(ConsumersCoordinator coordinator) {
    Type type = new TypeToken<List<ConsumersPoolInfo>>() {}.getType();
    return GSON.fromJson(coordinator.toString(), type);
  }

  static EnvironmentInfo extract(Environment environment) {
    return GSON.fromJson(environment.toString(), EnvironmentInfo.class);
  }

  public static class EnvironmentInfo {

    private final String locator;
    private final ProducersPoolInfo[] producers;
    private final ConsumersPoolInfo[] consumers;

    public EnvironmentInfo(
        String locator, ProducersPoolInfo[] producers, ConsumersPoolInfo[] consumers) {
      this.locator = locator;
      this.producers = producers;
      this.consumers = consumers;
    }

    public String getLocator() {
      return locator;
    }

    public List<ConsumersPoolInfo> getConsumers() {
      return arrayToList(this.consumers);
    }

    public List<ProducersPoolInfo> getProducers() {
      return arrayToList(this.producers);
    }

    @Override
    public String toString() {
      return "EnvironmentInfo{"
          + "locator='"
          + locator
          + '\''
          + ", producers="
          + Arrays.toString(producers)
          + ", consumers="
          + Arrays.toString(consumers)
          + '}';
    }
  }

  public static class ConsumersPoolInfo {

    private final String broker;
    private final ConsumerManager[] clients;

    public ConsumersPoolInfo(String broker, ConsumerManager[] clients) {
      this.broker = broker;
      this.clients = clients;
    }

    public String getBroker() {
      return broker;
    }

    public List<ConsumerManager> getClients() {
      return arrayToList(this.clients);
    }

    public int consumerCount() {
      return getClients().stream()
          .map(manager -> manager.getConsumerCount())
          .reduce(0, (acc, count) -> acc + count);
    }

    @Override
    public String toString() {
      return "ConsumerPoolInfo{"
          + "broker='"
          + broker
          + '\''
          + ", clients="
          + Arrays.toString(clients)
          + '}';
    }
  }

  public static class ConsumerManager {

    private final int consumer_count;

    public ConsumerManager(int consumerCount) {
      this.consumer_count = consumerCount;
    }

    public int getConsumerCount() {
      return consumer_count;
    }

    @Override
    public String toString() {
      return "ConsumerManager{" + "consumerCount=" + consumer_count + '}';
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
    private final int committing_consumer_count;

    public ProducerManager(int producerCount, int committing_consumer_count) {
      this.producer_count = producerCount;
      this.committing_consumer_count = committing_consumer_count;
    }

    public int getProducerCount() {
      return producer_count;
    }

    public int getCommittingConsumerCount() {
      return this.committing_consumer_count;
    }

    @Override
    public String toString() {
      return "ProducerManager{"
          + "producerCount="
          + producer_count
          + ", committingConsumerCount= "
          + committing_consumer_count
          + '}';
    }
  }
}
