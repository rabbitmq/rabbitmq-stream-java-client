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
package com.rabbitmq.stream.perf;

import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.OffsetSpecification;
import picocli.CommandLine;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class Utils {

    static class ByteCapacityTypeConverter implements CommandLine.ITypeConverter<ByteCapacity> {

        @Override
        public ByteCapacity convert(String value) {
            try {
                return ByteCapacity.from(value);
            } catch (IllegalArgumentException e) {
                throw new CommandLine.TypeConversionException(
                        "'" + value + "' is not valid, valid example values: 100gb, 50mb");
            }
        }
    }

    static class OffsetSpecificationTypeConverter implements CommandLine.ITypeConverter<OffsetSpecification> {

        private static final Map<String, OffsetSpecification> SPECS = Collections.unmodifiableMap(new HashMap<String, OffsetSpecification>() {{
            put("first", OffsetSpecification.first());
            put("last", OffsetSpecification.last());
            put("next", OffsetSpecification.next());
        }});

        @Override
        public OffsetSpecification convert(String value) throws Exception {
            if (value == null || value.trim().isEmpty()) {
                return OffsetSpecification.first();
            }

            if (SPECS.containsKey(value.toLowerCase())) {
                return SPECS.get(value.toLowerCase());
            }

            try {
                long offset = Long.parseUnsignedLong(value);
                return OffsetSpecification.offset(offset);
            } catch (NumberFormatException e) {
                // trying next
            }

            try {
                TemporalAccessor accessor = DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(value);
                return OffsetSpecification.timestamp(Instant.from(accessor).toEpochMilli());
            } catch (DateTimeParseException e) {
                throw new CommandLine.TypeConversionException(
                        "'" + value + "' is not a valid offset value, valid values are 'first', 'last', 'next', " +
                                "an unsigned long, or an ISO 8601 formatted timestamp (eg. 2020-06-03T07:45:54Z)");
            }

        }
    }

    static class PositiveIntegerTypeConverter implements CommandLine.ITypeConverter<Integer> {

        @Override
        public Integer convert(String input) {
            try {
                Integer value = Integer.valueOf(input);
                if (value <= 0) {
                    throw new IllegalArgumentException();
                }
                return value;
            } catch (Exception e) {
                throw new CommandLine.TypeConversionException(input + " is not a positive integer");
            }
        }
    }

    static class NotNegativeIntegerTypeConverter implements CommandLine.ITypeConverter<Integer> {

        @Override
        public Integer convert(String input) {
            try {
                Integer value = Integer.valueOf(input);
                if (value < 0) {
                    throw new IllegalArgumentException();
                }
                return value;
            } catch (Exception e) {
                throw new CommandLine.TypeConversionException(input + " is not a non-negative integer");
            }
        }
    }

}
