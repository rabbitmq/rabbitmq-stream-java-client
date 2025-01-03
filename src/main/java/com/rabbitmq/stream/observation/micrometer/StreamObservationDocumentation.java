// Copyright (c) 2007-2025 Broadcom. All Rights Reserved.
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
package com.rabbitmq.stream.observation.micrometer;

import io.micrometer.common.docs.KeyName;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.docs.ObservationDocumentation;

/**
 * {@link ObservationDocumentation} for RabbitMQ Stream.
 *
 * @since 0.12.0
 */
public enum StreamObservationDocumentation implements ObservationDocumentation {

  /** Observation for publishing a message. */
  PUBLISH_OBSERVATION {

    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultPublishObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
      return LowCardinalityTags.values();
    }
  },

  /** Observation for processing a message. */
  PROCESS_OBSERVATION {

    @Override
    public Class<? extends ObservationConvention<? extends Observation.Context>>
        getDefaultConvention() {
      return DefaultProcessObservationConvention.class;
    }

    @Override
    public KeyName[] getLowCardinalityKeyNames() {
      return LowCardinalityTags.values();
    }
  };

  /** Low cardinality tags. */
  public enum LowCardinalityTags implements KeyName {

    /** A string identifying the messaging system. */
    MESSAGING_SYSTEM {

      @Override
      public String asString() {
        return "messaging.system";
      }
    },

    /** A string identifying the kind of messaging operation. */
    MESSAGING_OPERATION {

      @Override
      public String asString() {
        return "messaging.operation";
      }
    },

    /** A string identifying the protocol (RabbitMQ Stream). */
    NET_PROTOCOL_NAME {

      @Override
      public String asString() {
        return "net.protocol.name";
      }
    },

    /** A string identifying the protocol version (1.0). */
    NET_PROTOCOL_VERSION {

      @Override
      public String asString() {
        return "net.protocol.version";
      }
    },
  }

  /** High cardinality tags. */
  public enum HighCardinalityTags implements KeyName {

    /** The message destination name. */
    MESSAGING_DESTINATION_NAME {

      @Override
      public String asString() {
        return "messaging.destination.name";
      }
    },

    /** The message destination name. */
    MESSAGING_SOURCE_NAME {

      @Override
      public String asString() {
        return "messaging.source.name";
      }
    },

    MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES {

      @Override
      public String asString() {
        return "messaging.message.payload_size_bytes";
      }
    },

    NET_SOCK_PEER_PORT {
      @Override
      public String asString() {
        return "net.sock.peer.port";
      }
    },

    NET_SOCK_PEER_ADDR {
      @Override
      public String asString() {
        return "net.sock.peer.addr";
      }
    }
  }
}
