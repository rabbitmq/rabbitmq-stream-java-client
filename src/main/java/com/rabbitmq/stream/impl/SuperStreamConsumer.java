// Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.impl.StreamConsumerBuilder.TrackingConfiguration;
import com.rabbitmq.stream.impl.Utils.CompositeConsumerUpdateListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SuperStreamConsumer implements Consumer {

  private static final Logger LOGGER = LoggerFactory.getLogger(SuperStreamConsumer.class);
  private final String superStream;
  private final Map<String, Consumer> consumers = new ConcurrentHashMap<>();

  SuperStreamConsumer(
      StreamConsumerBuilder builder,
      String superStream,
      StreamEnvironment environment,
      TrackingConfiguration trackingConfiguration) {
    this.superStream = superStream;
    List<String> partitions = environment.locatorOperation(c -> c.partitions(superStream));

    // for manual offset tracking strategy only
    ConsumerState[] states = new ConsumerState[partitions.size()];
    Map<String, ConsumerState> partitionToStates = new HashMap<>(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
      ConsumerState state = new ConsumerState();
      states[i] = state;
      partitionToStates.put(partitions.get(i), state);
    }
    // end of manual offset tracking strategy

    for (String partition : partitions) {
      ConsumerState state = partitionToStates.get(partition);
      MessageHandler messageHandler;
      if (trackingConfiguration.enabled() && trackingConfiguration.manual()) {
        messageHandler =
            new ManualOffsetTrackingMessageHandler(builder.messageHandler(), states, state);
      } else {
        messageHandler = builder.messageHandler();
      }
      StreamConsumerBuilder subConsumerBuilder = builder.duplicate();

      // to ease testing
      // we need to duplicate the composite consumer update listener,
      // otherwise a unique instance would get the listeners of all the sub-consumers
      if (subConsumerBuilder.consumerUpdateListener() instanceof CompositeConsumerUpdateListener) {
        subConsumerBuilder.consumerUpdateListener(
            ((CompositeConsumerUpdateListener) subConsumerBuilder.consumerUpdateListener())
                .duplicate());
      }

      if (trackingConfiguration.enabled() && trackingConfiguration.auto()) {
        subConsumerBuilder =
            (StreamConsumerBuilder)
                subConsumerBuilder
                    .autoTrackingStrategy()
                    .messageCountBeforeStorage(
                        trackingConfiguration.autoMessageCountBeforeStorage() / partitions.size())
                    .builder();
      }

      Consumer consumer =
          subConsumerBuilder.lazyInit(true).superStream(null).messageHandler(messageHandler).stream(
                  partition)
              .build();
      consumers.put(partition, consumer);
      state.consumer = consumer;
      LOGGER.debug("Created consumer on stream '{}' for super stream '{}'", partition, superStream);
    }

    consumers.values().forEach(c -> ((StreamConsumer) c).start());
  }

  private static final class ConsumerState {

    private volatile long offset = 0;
    private volatile Consumer consumer;
  }

  private static final class ManualOffsetTrackingMessageHandler implements MessageHandler {

    private final MessageHandler delegate;
    private final ConsumerState[] consumerStates;
    private final ConsumerState consumerState;

    private ManualOffsetTrackingMessageHandler(
        MessageHandler delegate, ConsumerState[] consumerStates, ConsumerState consumerState) {
      this.delegate = delegate;
      this.consumerStates = consumerStates;
      this.consumerState = consumerState;
    }

    @Override
    public void handle(Context context, Message message) {
      Context ctx =
          new Context() {
            @Override
            public long offset() {
              return context.offset();
            }

            @Override
            public long timestamp() {
              return context.timestamp();
            }

            @Override
            public long committedChunkId() {
              return context.committedChunkId();
            }

            @Override
            public void storeOffset() {
              for (ConsumerState state : consumerStates) {
                if (ManualOffsetTrackingMessageHandler.this.consumerState == state) {
                  context.storeOffset();
                } else if (state.offset != 0) {
                  state.consumer.store(state.offset);
                }
              }
            }

            @Override
            public String stream() {
              return context.stream();
            }

            @Override
            public Consumer consumer() {
              return context.consumer();
            }
          };
      this.delegate.handle(ctx, message);
      consumerState.offset = context.offset();
    }
  }

  @Override
  public void store(long offset) {
    throw new UnsupportedOperationException(
        "Consumer#store(long) does not work for super streams, use MessageHandler.Context#storeOffset() instead");
  }

  @Override
  public void close() {
    for (Entry<String, Consumer> entry : consumers.entrySet()) {
      LOGGER.debug(
          "Closing consumer for partition '{}' of super stream {}",
          entry.getKey(),
          this.superStream);
      try {
        entry.getValue().close();
      } catch (Exception e) {
        LOGGER.info(
            "Error while closing consumer for partition {} of super stream {}: {}",
            entry.getKey(),
            this.superStream,
            e.getMessage());
      }
    }
  }
}
