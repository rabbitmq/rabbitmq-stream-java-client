// Copyright (c) 2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

import static com.rabbitmq.stream.Constants.CODE_MESSAGE_ENQUEUEING_FAILED;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_OK;
import static com.rabbitmq.stream.Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import com.rabbitmq.stream.impl.Utils.ExactNodeRetryClientFactory;
import java.time.Duration;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

public class UtilsTest {

  @Test
  void formatConstantOk() {
    assertThat(formatConstant(RESPONSE_CODE_OK)).isEqualTo("1 (OK)");
    assertThat(formatConstant(RESPONSE_CODE_STREAM_DOES_NOT_EXIST))
        .isEqualTo("2 (STREAM_DOES_NOT_EXIST)");
    assertThat(formatConstant(CODE_MESSAGE_ENQUEUEING_FAILED))
        .isEqualTo("10001 (MESSAGE_ENQUEUEING_FAILED)");
  }

  @Test
  void exactNodeRetryClientFactoryShouldReturnImmediatelyIfConditionOk() {
    Client client = mock(Client.class);
    ClientFactory cf = mock(ClientFactory.class);
    when(cf.client(any())).thenReturn(client);
    Predicate<Client> condition = c -> true;
    Client result =
        new ExactNodeRetryClientFactory(cf, condition, Duration.ofMillis(1))
            .client(ClientFactoryContext.fromParameters(new ClientParameters()));
    assertThat(result).isEqualTo(client);
    verify(cf, times(1)).client(any());
    verify(client, never()).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void exactNodeRetryClientFactoryShouldRetryUntilConditionOk() {
    Client client = mock(Client.class);
    ClientFactory cf = mock(ClientFactory.class);
    when(cf.client(any())).thenReturn(client);
    Predicate<Client> condition = mock(Predicate.class);
    when(condition.test(any())).thenReturn(false).thenReturn(false).thenReturn(true);
    Client result =
        new ExactNodeRetryClientFactory(cf, condition, Duration.ofMillis(1))
            .client(ClientFactoryContext.fromParameters(new ClientParameters()));
    assertThat(result).isEqualTo(client);
    verify(cf, times(3)).client(any());
    verify(client, times(2)).close();
  }
}
