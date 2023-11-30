// Copyright (c) 2020-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom
// Inc. and/or its subsidiaries.
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
package com.rabbitmq.stream.sasl;

import java.util.List;

/** Contract to choose a {@link SaslMechanism} from the ones supported by the server. */
public interface SaslConfiguration {

  /**
   * Pick mechanism according to the ones passed in.
   *
   * @param mechanisms supported mechanisms by the server
   * @return the mechanism to use for authentication
   */
  SaslMechanism getSaslMechanism(List<String> mechanisms);
}
