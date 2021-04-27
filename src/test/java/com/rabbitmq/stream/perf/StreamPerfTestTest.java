// Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.impl.TestUtils.waitAtMost;
import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.impl.Client;
import com.rabbitmq.stream.impl.TestUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestUtils.StreamTestInfrastructureExtension.class)
public class StreamPerfTestTest {

  static ExecutorService executor = Executors.newSingleThreadExecutor();
  TestUtils.ClientFactory cf;

  @BeforeAll
  static void init() {
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterAll
  static void tearDown() {
    executor.shutdownNow();
  }

  @Test
  void streamsShouldNotBeDeletedByDefault(TestInfo info) throws Exception {
    String s = TestUtils.streamName(info);
    Client client = cf.get();

    try {
      AtomicInteger exitCode = new AtomicInteger(-1);
      Future<?> run =
          executor.submit(
              () -> exitCode.set(StreamPerfTest.run(("--rate 100 --streams " + s).split(" "))));

      waitAtMost(() -> client.metadata(s).get(s).isResponseOk());
      Thread.sleep(1000L);
      run.cancel(true);

      waitAtMost(() -> exitCode.get() == 0);

      assertThat(client.metadata(s).get(s).isResponseOk()).isTrue();
    } finally {
      client.delete(s);
    }
  }

  @Test
  void streamsShouldBeDeletedWithFlag(TestInfo info) throws Exception {
    String s = TestUtils.streamName(info);
    Client client = cf.get();

    try {
      AtomicInteger exitCode = new AtomicInteger(-1);
      Future<?> run =
          executor.submit(
              () ->
                  exitCode.set(
                      StreamPerfTest.run(
                          ("--rate 100 --delete-streams --streams " + s).split(" "))));

      waitAtMost(() -> client.metadata(s).get(s).isResponseOk());
      Thread.sleep(1000L);
      run.cancel(true);

      waitAtMost(() -> exitCode.get() == 0);

      assertThat(client.metadata(s).get(s).getResponseCode())
          .isEqualTo(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST);
    } finally {
      client.delete(s);
    }
  }
}
