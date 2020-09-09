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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

public class ProducersCoordinatorTest {

    @Mock
    StreamEnvironment environment;
    @Mock
    Client locator;
    @Mock
    StreamProducer producer;
    @Mock
    Function<Client.ClientParameters, Client> clientFactory;
    @Mock
    Client client;
    AutoCloseable mocks;
    ProducersCoordinator producersCoordinator;

    @BeforeEach
    void init() {
        mocks = MockitoAnnotations.openMocks(this);
        when(environment.locator()).thenReturn(locator);
        producersCoordinator = new ProducersCoordinator(environment, clientFactory);
    }

    @AfterEach
    void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    void registerShouldThrowExceptionWhenNoMetadataForTheStream() {
        assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
                .isInstanceOf(StreamDoesNotExistException.class);
    }

    @Test
    void registerShouldThrowExceptionWhenStreamDoesNotExist() {
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, null, null)));
        assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
                .isInstanceOf(StreamDoesNotExistException.class);
    }

    @Test
    void registerShouldThrowExceptionWhenMetadataResponseIsNotOk() {
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_ACCESS_REFUSED, null, null)));
        assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void registerShouldThrowExceptionWhenNoLeader() {
        when(locator.metadata("stream"))
                .thenReturn(Collections.singletonMap("stream",
                        new Client.StreamMetadata("stream", Constants.RESPONSE_CODE_OK, null, replicas())));
        assertThatThrownBy(() -> producersCoordinator.registerProducer(producer, "stream"))
                .isInstanceOf(IllegalStateException.class);
    }

    Client.Broker leader() {
        return new Client.Broker("leader", -1);
    }

    List<Client.Broker> replicas() {
        return Arrays.asList(new Client.Broker("replica1", -1), new Client.Broker("replica2", -1));
    }

}
