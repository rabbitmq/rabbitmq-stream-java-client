// Copyright (c) 2022 VMware, Inc. or its affiliates.  All rights reserved.
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
package com.rabbitmq.stream.perf;

import picocli.CommandLine;

public class AggregatingCommandForAutoComplete {

  @CommandLine.Mixin private final StreamPerfTest streamPerfTest = new StreamPerfTest();

  @CommandLine.Mixin
  private final DebugEndpointMonitoring monitoring = new DebugEndpointMonitoring();

  @CommandLine.Mixin
  private final PrometheusEndpointMonitoring prometheusEndpointMonitoring =
      new PrometheusEndpointMonitoring();
}
