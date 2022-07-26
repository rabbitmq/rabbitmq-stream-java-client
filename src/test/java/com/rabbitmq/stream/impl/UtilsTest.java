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
import static com.rabbitmq.stream.impl.Utils.defaultConnectionNamingStrategy;
import static com.rabbitmq.stream.impl.Utils.formatConstant;
import static com.rabbitmq.stream.impl.Utils.offsetBefore;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rabbitmq.stream.impl.Client.ClientParameters;
import com.rabbitmq.stream.impl.Utils.ClientConnectionType;
import com.rabbitmq.stream.impl.Utils.ClientFactory;
import com.rabbitmq.stream.impl.Utils.ClientFactoryContext;
import com.rabbitmq.stream.impl.Utils.ExactNodeRetryClientFactory;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @Test
  void defaultConnectionNamingStrategyShouldIncrement() {
    Function<ClientConnectionType, String> strategy =
        defaultConnectionNamingStrategy("rabbitmq-stream-");
    for (ClientConnectionType type : ClientConnectionType.values()) {
      IntStream.range(0, 10)
          .forEach(
              i -> {
                assertThat(strategy.apply(type)).endsWith("-" + i);
              });
    }
  }

  @Test
  void testOffsetBefore() {
    assertThat(offsetBefore(1, 2)).isTrue();
    assertThat(offsetBefore(2, 2)).isFalse();
    assertThat(offsetBefore(Long.MAX_VALUE - 1, Long.MAX_VALUE)).isTrue();
    assertThat(offsetBefore(Long.MAX_VALUE, Long.MAX_VALUE)).isFalse();
    assertThat(offsetBefore(Long.MAX_VALUE, Long.MAX_VALUE + 1)).isTrue();
    assertThat(offsetBefore(Long.MAX_VALUE + 10, Long.MAX_VALUE + 10)).isFalse();
    assertThat(offsetBefore(Long.MAX_VALUE + 10, Long.MAX_VALUE + 20)).isTrue();
  }

  @ParameterizedTest
  @CsvSource({
    "3.8.0+rc.1.2186.g95f3fde,false",
    "3.9.21,false",
    "3.9.22-alpha.13,false",
    "3.10.6,false",
    "3.11.0-alpha.15,true",
    "3.11.0,true",
    "3.11.1,true",
    "4.0.0-alpha.15,true",
    "4.0.0,true",
    "4.0.1,true",
    "4.1.0-alpha.15,true",
    "4.1.0,true",
    "4.1.1,true",
  })
  void is_3_11_OrMore(String input, boolean expected) {
    assertThat(Utils.is3_11_OrMore(input)).isEqualTo(expected);
  }
}
