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
package com.rabbitmq.stream.impl;

import static com.rabbitmq.stream.impl.DefaultExecutorServiceFactory.maybeResize;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.impl.DefaultExecutorServiceFactory.Executor;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DefaultExecutorServiceFactoryTest {

  int minSize = 4;
  int clientsPerExecutor = 10;

  @Mock Supplier<Executor> factory;
  @Mock ExecutorService executorService;
  AutoCloseable mocks;
  List<Executor> executors;

  @BeforeEach
  void init() {
    mocks = MockitoAnnotations.openMocks(this);
    when(factory.get()).thenReturn(new Executor(executorService));
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  void resizeShouldDoNothingWhenClientsPerExecutorIsOkForAll() {
    executors =
        executors(
            executor().addUsage(5),
            executor().addUsage(5),
            executor().addUsage(5),
            executor().addUsage(5));
    maybeResize(executors, minSize, clientsPerExecutor, factory);
    assertThat(executors).hasSize(4);
    verify(factory, never()).get();
    verify(executorService, never()).shutdownNow();
  }

  @Test
  void resizeShouldIncreaseIfMoreClientsPerExecutorForOneExecutor() {
    executors =
        executors(
            executor().addUsage(clientsPerExecutor),
            executor().addUsage(clientsPerExecutor),
            executor().addUsage(clientsPerExecutor + 1),
            executor().addUsage(clientsPerExecutor));
    maybeResize(executors, minSize, clientsPerExecutor, factory);
    assertThat(executors).hasSize(5);
    verify(factory, times(1)).get();
    verify(executorService, never()).shutdownNow();
  }

  @RepeatedTest(10)
  void resizeShouldDecreaseIfFewerClientsPerExecutorAndUnusedExecutor() {
    // repeated because the initial list is shuffled
    executors =
        executors(
            executor().addUsage(5),
            executor().addUsage(5),
            executor().addUsage(5),
            executor(),
            executor().addUsage(5));
    maybeResize(executors, minSize, clientsPerExecutor, factory);
    assertThat(executors).hasSize(4);
    verify(factory, never()).get();
    verify(executorService, times(1)).shutdownNow();
  }

  @Test
  void resizeShouldNotResizeIfNoUnusedExecutor() {
    executors =
        executors(
            executor().addUsage(5),
            executor().addUsage(5),
            executor().addUsage(5),
            executor().addUsage(1),
            executor().addUsage(5));
    maybeResize(executors, minSize, clientsPerExecutor, factory);
    assertThat(executors).hasSize(5);
    verify(factory, never()).get();
    verify(executorService, never()).shutdownNow();
  }

  private Executor executor() {
    return new Executor(executorService);
  }

  private List<Executor> executors(Executor... executors) {
    List<Executor> l = asList(executors);
    Collections.shuffle(l);
    return new CopyOnWriteArrayList<>(l);
  }
}
