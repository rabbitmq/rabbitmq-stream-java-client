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

import com.rabbitmq.stream.Constants;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.StreamDoesNotExistException;
import com.rabbitmq.stream.codec.WrapperMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class DefaultClientSubscriptionsTest {

    @Mock
    StreamEnvironment environment;
    @Mock
    StreamConsumer consumer;
    @Mock
    Client locator;
    @Mock
    Function<Client.ClientParameters, Client> clientFactory;

    DefaultClientSubscriptions clientSubscriptions;

    @BeforeEach
    void init() {
        MockitoAnnotations.initMocks(this);
        clientSubscriptions = new DefaultClientSubscriptions(environment, clientFactory);
    }

    @Test
    void subscribeShouldThrowExceptionWhenNoMetadataForTheStream() {
        when(environment.locator()).thenReturn(locator);
        assertThatThrownBy(() -> clientSubscriptions.subscribe(consumer, "stream", OffsetSpecification.first(), (offset, message) -> {
        })).isInstanceOf(StreamDoesNotExistException.class);
    }

    @Test
    void subscribeShouldThrowExceptionWhenStreamDoesNotExist() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, null, null)));
        assertThatThrownBy(() -> clientSubscriptions.subscribe(consumer, "stream", OffsetSpecification.first(), (offset, message) -> {
        })).isInstanceOf(StreamDoesNotExistException.class);
    }

    @Test
    void subscribeShouldThrowExceptionWhenMetadataResponseIsNotOk() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_ACCESS_REFUSED, null, null)));
        assertThatThrownBy(() -> clientSubscriptions.subscribe(consumer, "stream", OffsetSpecification.first(), (offset, message) -> {
        })).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void subscribeShouldThrowExceptionIfNodeAvailableForStream() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, null)));
        assertThatThrownBy(() -> clientSubscriptions.subscribe(consumer, "stream", OffsetSpecification.first(), (offset, message) -> {
        })).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void findBrokersForStreamShouldReturnLeaderIfNoReplicas() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, leader(), null)));
        assertThat(clientSubscriptions.findBrokersForStream("stream"))
                .hasSize(1)
                .contains(leader());
    }

    @Test
    void findBrokersForStreamShouldReturnReplicasIfThereAreSome() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));
        assertThat(clientSubscriptions.findBrokersForStream("stream"))
                .hasSize(2)
                .hasSameElementsAs(replicas());
    }

    @Test
    void subscribeShouldSubscribeToStreamAndDispatchesMessage_UnsubscribeShouldUnsubscribe() {
        AtomicReference<Client.MessageListener> messageListenerReference = new AtomicReference<>();
        Client.ClientParameters cp = new Client.ClientParameters() {
            @Override
            public Client.ClientParameters messageListener(Client.MessageListener messageListener) {
                messageListenerReference.set(messageListener);
                return super.messageListener(messageListener);
            }
        };
        when(environment.locator()).thenReturn(locator);
        when(environment.clientParametersCopy()).thenReturn(cp);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));

        Client client = mock(Client.class);
        ArgumentCaptor<Integer> subscriptionIdCaptor = ArgumentCaptor.forClass(Integer.class);
        when(clientFactory.apply(any(Client.ClientParameters.class))).thenReturn(client);
        when(client.subscribe(subscriptionIdCaptor.capture(), anyString(), any(OffsetSpecification.class), anyInt()))
                .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

        AtomicInteger messageHandlerCalls = new AtomicInteger();
        long subscriptionGlobalId = clientSubscriptions.subscribe(consumer, "stream", OffsetSpecification.first(), (offset, message) -> {
            messageHandlerCalls.incrementAndGet();
        });
        verify(clientFactory, times(1)).apply(any(Client.ClientParameters.class));
        verify(client, times(1)).subscribe(anyInt(), anyString(), any(OffsetSpecification.class), anyInt());

        assertThat(messageHandlerCalls.get()).isEqualTo(0);
        messageListenerReference.get().handle(subscriptionIdCaptor.getValue(), 0, new WrapperMessageBuilder().build());
        assertThat(messageHandlerCalls.get()).isEqualTo(1);

        when(client.unsubscribe(subscriptionIdCaptor.getValue()))
                .thenReturn(new Client.Response(Constants.RESPONSE_CODE_OK));

        clientSubscriptions.unsubscribe(subscriptionGlobalId);
        verify(client, times(1)).unsubscribe(subscriptionIdCaptor.getValue());

        messageListenerReference.get().handle(subscriptionIdCaptor.getValue(), 0, new WrapperMessageBuilder().build());
        assertThat(messageHandlerCalls.get()).isEqualTo(1);
    }

    Client.Broker leader() {
        return new Client.Broker("leader", -1);
    }

    List<Client.Broker> replicas() {
        return Arrays.asList(new Client.Broker("replica1", -1), new Client.Broker("replica2", -1));
    }

}
