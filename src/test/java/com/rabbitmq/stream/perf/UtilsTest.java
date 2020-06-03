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

package com.rabbitmq.stream.perf;

import com.rabbitmq.stream.OffsetSpecification;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.of;

public class UtilsTest {

    CommandLine.ITypeConverter<OffsetSpecification> offsetSpecificationConverter = new Utils.OffsetSpecificationTypeConverter();

    static Stream<Arguments> offsetSpecificationTypeConverterOkArguments() {
        return Stream.of(
                of("", OffsetSpecification.first()),
                of("first", OffsetSpecification.first()), of("FIRST", OffsetSpecification.first()),
                of("last", OffsetSpecification.last()), of("LAST", OffsetSpecification.last()),
                of("next", OffsetSpecification.next()), of("NEXT", OffsetSpecification.next()),
                of("0", OffsetSpecification.offset(0)),
                of("1000", OffsetSpecification.offset(1000)),
                of("9223372036854775817", OffsetSpecification.offset(Long.MAX_VALUE + 10)),
                of("2020-06-03T08:54:57Z", OffsetSpecification.timestamp(1591174497000L)),
                of("2020-06-03T10:54:57+02:00", OffsetSpecification.timestamp(1591174497000L))
        );
    }

    @ParameterizedTest
    @MethodSource("offsetSpecificationTypeConverterOkArguments")
    void offsetSpecificationTypeConverterOk(String value, OffsetSpecification expected) throws Exception {
        assertThat(offsetSpecificationConverter.convert(value)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"foo", "-1", "2020-06-03"})
    void offsetSpecificationTypeConverterKo(String value) {
        assertThatThrownBy(() -> offsetSpecificationConverter.convert(value))
                .isInstanceOf(CommandLine.TypeConversionException.class);
    }

}
