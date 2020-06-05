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

package com.rabbitmq.stream.sasl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * {@link SaslConfiguration} that supports our built-in mechanisms.
 */
public class DefaultSaslConfiguration implements SaslConfiguration {

    public static final SaslConfiguration PLAIN = new DefaultSaslConfiguration(PlainSaslMechanism.INSTANCE.getName());
    public static final SaslConfiguration EXTERNAL = new DefaultSaslConfiguration(ExternalSaslMechanism.INSTANCE.getName());

    private final Map<String, SaslMechanism> mechanisms = Collections.unmodifiableMap(new HashMap<String, SaslMechanism>() {{
        put(PlainSaslMechanism.INSTANCE.getName(), PlainSaslMechanism.INSTANCE);
        put(ExternalSaslMechanism.INSTANCE.getName(), ExternalSaslMechanism.INSTANCE);
    }});
    private final String mechanism;

    public DefaultSaslConfiguration() {
        this(null);
    }

    public DefaultSaslConfiguration(String mechanism) {
        if (mechanism != null && !mechanisms.containsKey(mechanism)) {
            throw new IllegalArgumentException(format(
                    "SASL mechanism not supported: %s. Supported mechanisms: %s.",
                    mechanism, String.join(", ", mechanisms.keySet())
            ));
        }
        this.mechanism = mechanism;
    }

    @Override
    public SaslMechanism getSaslMechanism(List<String> mechanisms) {
        mechanisms = mechanisms == null ? Collections.emptyList() : mechanisms;
        if (this.mechanism == null) {
            for (String serverMechanism : mechanisms) {
                SaslMechanism match = this.mechanisms.get(serverMechanism);
                if (match != null) {
                    return match;
                }
            }
            throw new IllegalStateException(format(
                    "Unable to agree on a SASL mechanism. Client: %s / server %s.",
                    String.join(", ", this.mechanisms.keySet()), String.join(", ", mechanisms)
            ));
        } else {
            if (mechanisms.contains(mechanism)) {
                return this.mechanisms.get(mechanism);
            } else {
                throw new IllegalStateException(format(
                        "Unable to agree on a SASL mechanism. Client: %s / server %s.",
                        this.mechanism, String.join(", ", mechanisms)
                ));
            }
        }
    }

}
