// Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is dual-licensed under the
// Mozilla Public License 1.1 ("MPL"), and the Apache License version 2 ("ASL").
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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DefaultSaslConfigurationTest {

    @Test
    void supportsOnlyPlainAndExternal() {
        assertThatThrownBy(() -> new DefaultSaslConfiguration("dummy"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void getSaslMechanismShouldPickSpecifiedMechanismWhenAvailable() {
        SaslConfiguration configuration = new DefaultSaslConfiguration(PlainSaslMechanism.INSTANCE.getName());
        assertThat(configuration.getSaslMechanism(asList("EXTERNAL", "DUMMY", "PLAIN")).getName())
                .isEqualTo(PlainSaslMechanism.INSTANCE.getName());
    }

    @Test
    void getSaslMechanismShouldThrowExceptionIfNoMatch() {
        SaslConfiguration configuration = new DefaultSaslConfiguration(PlainSaslMechanism.INSTANCE.getName());
        assertThatThrownBy(() -> configuration.getSaslMechanism(asList("EXTERNAL", "DUMMY")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unable to agree on a SASL mechanism. Client: PLAIN / server EXTERNAL, DUMMY.");
    }

    @Test
    void getSaslMechanismReturnFirstMatchIfNoMechanismSpecified() {
        SaslConfiguration configuration = new DefaultSaslConfiguration();
        assertThat(configuration.getSaslMechanism(asList("DUMMY", "EXTERNAL", "PLAIN")).getName())
                .isEqualTo(ExternalSaslMechanism.INSTANCE.getName());
    }

    @Test
    void getSaslMechanismShouldThrowExceptionIfNoMechanismSpecifiedAndNoMatch() {
        SaslConfiguration configuration = new DefaultSaslConfiguration();
        assertThatThrownBy(() -> configuration.getSaslMechanism(asList("FOO", "BAR")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unable to agree on a SASL mechanism. Client: PLAIN, EXTERNAL / server FOO, BAR.");
    }

}
