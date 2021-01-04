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

package com.rabbitmq.stream.docs;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;

import java.time.Duration;
import java.util.Arrays;

public class EnvironmentUsage {

    void environmentCreation() throws Exception {
        // tag::environment-creation[]
        Environment environment = Environment.builder().build();  // <1>
        // ...
        environment.close(); // <2>
        // end::environment-creation[]
    }

    void environmentCreationWithUri() {
        // tag::environment-creation-with-uri[]
        Environment environment = Environment.builder()
                .uri("rabbitmq-stream://guest:guest@localhost:5555/%2f")  // <1>
                .build();
        // end::environment-creation-with-uri[]
    }

    void environmentCreationWithUris() {
        // tag::environment-creation-with-uris[]
        Environment environment = Environment.builder()
                .uris(Arrays.asList(                     // <1>
                        "rabbitmq-stream://host1:5555",
                        "rabbitmq-stream://host2:5555",
                        "rabbitmq-stream://host3:5555")
                )
                .build();
        // end::environment-creation-with-uris[]
    }

    void createStream() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation[]
        environment.streamCreator().stream("my-stream").create();  // <1>
        // end::stream-creation[]
    }

    void createStreamWithRetention() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation-retention[]
        environment.streamCreator()
                .stream("my-stream")
                .maxLengthBytes(ByteCapacity.GB(10))  // <1>
                .maxSegmentSizeBytes(ByteCapacity.MB(500))  // <2>
                .create();
        // end::stream-creation-retention[]
    }

    void createStreamWithTimeBasedRetention() {
        Environment environment = Environment.builder().build();
        // tag::stream-creation-time-based-retention[]
        environment.streamCreator()
                .stream("my-stream")
                .maxAge(Duration.ofHours(6))  // <1>
                .maxSegmentSizeBytes(ByteCapacity.MB(500))  // <2>
                .create();
        // end::stream-creation-time-based-retention[]
    }

    void deleteStream() {
        Environment environment = Environment.builder().build();
        // tag::stream-deletion[]
        environment.deleteStream("my-stream");  // <1>
        // end::stream-deletion[]
    }

}
