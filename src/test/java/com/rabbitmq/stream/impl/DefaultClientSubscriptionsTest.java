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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class DefaultClientSubscriptionsTest {

    @Mock
    StreamEnvironment environment;
    @Mock
    StreamConsumer consumer;
    @Mock
    Client locator;

    DefaultClientSubscriptions clientSubscriptions;

    @BeforeEach
    void init() {
        MockitoAnnotations.initMocks(this);
        clientSubscriptions = new DefaultClientSubscriptions(environment);
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
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, new Client.Broker("leader", -1), null)));
        assertThat(clientSubscriptions.findBrokersForStream("stream"))
                .hasSize(1)
                .contains(new Client.Broker("leader", -1));
    }

    @Test
    void findBrokersForStreamShouldReturnReplicasIfThereAreSome() {
        when(environment.locator()).thenReturn(locator);
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, Arrays.asList(
                                new Client.Broker("replica1", -1), new Client.Broker("replica2", -1)
                        ))));
        assertThat(clientSubscriptions.findBrokersForStream("stream"))
                .hasSize(2)
                .contains(new Client.Broker("replica1", -1), new Client.Broker("replica2", -1));
    }

}
