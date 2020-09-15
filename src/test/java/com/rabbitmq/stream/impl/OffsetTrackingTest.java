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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class OffsetTrackingTest {

  String stream;
  TestUtils.ClientFactory cf;

  @ParameterizedTest
  @CsvSource({
    "ref,ref,true,10,10,committed offset should be read",
    "ref,ref,true,-1,0,query offset should return 0 if not tracked offset for the reference",
    "ref,ref,false,-1,0,query offset should return 0 if stream does not exist",
    "ref,foo,false,-1,0,query offset should return 0 if stream does not exist",
  })
  void commitAndQuery(
      String commitReference,
      String queryReference,
      boolean streamExists,
      long committedOffset,
      long expectedOffset,
      String message)
      throws Exception {
    int messageCount = 500;
    TestUtils.publishAndWaitForConfirms(cf, messageCount, stream);
    Client client = cf.get();

    String s = streamExists ? this.stream : UUID.randomUUID().toString();
    if (committedOffset >= 0) {
      client.commitOffset(commitReference, s, committedOffset);
    }
    Thread.sleep(100L); // commit offset is fire-and-forget
    long offset = client.queryOffset(queryReference, s);
    assertThat(offset).as(message).isEqualTo(expectedOffset);
  }
}
