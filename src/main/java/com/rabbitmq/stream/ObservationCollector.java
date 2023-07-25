// Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream;

public interface ObservationCollector {

  ObservationCollector NO_OP =
      new ObservationCollector() {
        @Override
        public Object prePublish(String stream, Message message) {
          return null;
        }

        @Override
        public void published(Object context, Message message) {}

        @Override
        public MessageHandler subscribe(MessageHandler handler) {
          return handler;
        }
      };

  void published(Object context, Message message);

  Object prePublish(String stream, Message message);

  MessageHandler subscribe(MessageHandler handler);
}
