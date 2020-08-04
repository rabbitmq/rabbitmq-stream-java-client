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

import com.rabbitmq.stream.RecoveryBackOffDelayPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class StreamEnvironmentUnitTest {

    @Mock
    RecoveryBackOffDelayPolicy recoveryBackOffDelayPolicy;
    @Mock
    Function<Client.ClientParameters, Client> cf;
    @Mock
    Client client;

    StreamEnvironment environment;
    ScheduledExecutorService scheduledExecutorService;
    volatile Client.ShutdownListener shutdownListener;


    @BeforeEach
    void init() {
        AtomicReference<Client.ClientParameters> cpReference = new AtomicReference<>();
        Client.ClientParameters clientParameters = new Client.ClientParameters() {
            @Override
            public Client.ClientParameters shutdownListener(Client.ShutdownListener shutdownListener) {
                StreamEnvironmentUnitTest.this.shutdownListener = shutdownListener;
                return super.shutdownListener(shutdownListener);
            }

            @Override
            Client.ClientParameters duplicate() {
                return cpReference.get();
            }
        };
        cpReference.set(clientParameters);
        MockitoAnnotations.initMocks(this);
        when(cf.apply(any(Client.ClientParameters.class)))
                .thenReturn(client);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        environment = new StreamEnvironment(
                scheduledExecutorService,
                clientParameters,
                Collections.emptyList(),
                recoveryBackOffDelayPolicy,
                cf
        );
    }

    @AfterEach
    void tearDown() {
        environment.close();
        scheduledExecutorService.shutdownNow();
    }

    @Test
    void locatorRecoversAfterDisconnection() throws Exception {
        verify(cf, times(1)).apply(any(Client.ClientParameters.class));
        when(recoveryBackOffDelayPolicy.delay(anyInt())).thenReturn(Duration.ofMillis(50));
        shutdownListener.handle(new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.HEARTBEAT_FAILURE));
        Thread.sleep(50 * 3);
        verify(cf, times(1 + 1)).apply(any(Client.ClientParameters.class));
    }

    @Test
    void retryLocatorRecovery() throws Exception {
        verify(cf, times(1)).apply(any(Client.ClientParameters.class));
        when(cf.apply(any(Client.ClientParameters.class)))
                .thenThrow(new RuntimeException())
                .thenThrow(new RuntimeException())
                .thenReturn(client);
        when(recoveryBackOffDelayPolicy.delay(anyInt())).thenReturn(Duration.ofMillis(50));
        shutdownListener.handle(new Client.ShutdownContext(Client.ShutdownContext.ShutdownReason.HEARTBEAT_FAILURE));
        Thread.sleep(50 * 5);
        verify(cf, times(1 + 3)).apply(any(Client.ClientParameters.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldTryUrisOnInitializationFailure() throws Exception {
        reset(cf);
        when(cf.apply(any(Client.ClientParameters.class)))
                .thenThrow(new RuntimeException())
                .thenThrow(new RuntimeException())
                .thenReturn(client);

        URI uri = new URI("rabbitmq-stream://localhost:5555");
        environment = new StreamEnvironment(
                scheduledExecutorService,
                new Client.ClientParameters(),
                Arrays.asList(uri, uri, uri),
                recoveryBackOffDelayPolicy,
                cf
        );
        verify(cf, times(3)).apply(any(Client.ClientParameters.class));
    }

}
