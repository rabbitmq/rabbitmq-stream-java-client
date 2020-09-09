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

package com.rabbitmq.stream;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.stream.IntStream;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BackOffDelayPolicyTest {

    static Duration TIMEOUT = BackOffDelayPolicy.TIMEOUT;

    @Test
    void fixed() {
        BackOffDelayPolicy policy = BackOffDelayPolicy.fixed(ofSeconds(1));
        IntStream.range(0, 10).forEach(attempt -> assertThat(policy.delay(attempt)).isEqualTo(ofSeconds(1)));
    }

    @Test
    void fixedWithInitialDelay() {
        BackOffDelayPolicy policy = BackOffDelayPolicy.fixedWithInitialDelay(ofSeconds(2), ofSeconds(1));
        assertThat(policy.delay(0)).isEqualTo(ofSeconds(2));
        IntStream.range(1, 10).forEach(attempt -> assertThat(policy.delay(attempt)).isEqualTo(ofSeconds(1)));
    }

    @Test
    void fixedWithInitialDelayAndTimeoutShouldThrowExceptionIfInitialDelayLongerThanTimeout() {
        assertThatThrownBy(() -> BackOffDelayPolicy.fixedWithInitialDelay(ofSeconds(10), ofSeconds(2), ofSeconds(5)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fixedWithInitialDelayAndTimeout() {
        BackOffDelayPolicy policy = BackOffDelayPolicy.fixedWithInitialDelay(ofSeconds(5), ofSeconds(2), ofSeconds(20));
        assertThat(policy.delay(0)).isEqualTo(ofSeconds(5));
        assertThat(policy.delay(1)).isEqualTo(ofSeconds(2));
        assertThat(policy.delay(2)).isEqualTo(ofSeconds(2));
        assertThat(policy.delay(7)).isEqualTo(ofSeconds(2));
        assertThat(policy.delay(8)).isEqualTo(TIMEOUT);
        assertThat(policy.delay(9)).isEqualTo(TIMEOUT);
        assertThat(policy.delay(100)).isEqualTo(TIMEOUT);

        policy = BackOffDelayPolicy.fixedWithInitialDelay(ofSeconds(10), ofSeconds(5), ofSeconds(12));
        assertThat(policy.delay(0)).isEqualTo(ofSeconds(10));
        assertThat(policy.delay(1)).isEqualTo(TIMEOUT);
    }

}
