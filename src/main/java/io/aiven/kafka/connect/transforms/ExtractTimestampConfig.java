/*
 * Copyright 2020 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.transforms;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

final class ExtractTimestampConfig extends AbstractConfig {

    public static final String FIELD_NAME_CONFIG = "field.name";
    private static final String FIELD_NAME_DOC = "The name of the field is to be used as the source of timestamp. "
            + "The field must have INT64 or org.apache.kafka.connect.data.Timestamp type "
            + "and must mot be null.";

    public static final String EPOCH_RESOLUTION_CONFIG = "timestamp.resolution";
    private static final String EPOCH_RESOLUTION_DOC = "Time resolution used for INT64 type field. "
            + "Valid values are \"seconds\" for seconds since epoch and \"milliseconds\" for "
            + "milliseconds since epoch. Default is \"milliseconds\" and ignored for "
            + "org.apache.kafka.connect.data.Timestamp type.";


    public enum TimestampResolution {

        MILLISECONDS("milliseconds"),
        SECONDS("seconds");

        final String resolution;

        private static final String RESOLUTIONS =
                Arrays.stream(values()).map(TimestampResolution::resolution).collect(Collectors.joining(", "));

        private TimestampResolution(final String resolution) {
            this.resolution = resolution;
        }

        public String resolution() {
            return resolution;
        }

        public static TimestampResolution fromString(final String value) {
            for (final var r : values()) {
                if (r.resolution.equals(value)) {
                    return r;
                }
            }
            throw new IllegalArgumentException(
                    "Unsupported resolution type '" + value + "'. Supported are: " + RESOLUTIONS);
        }

    }

    ExtractTimestampConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(
                        FIELD_NAME_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        FIELD_NAME_DOC)
                .define(
                        EPOCH_RESOLUTION_CONFIG,
                        ConfigDef.Type.STRING,
                        TimestampResolution.MILLISECONDS.resolution,
                        new ConfigDef.Validator() {
                            @Override
                            public void ensureValid(final String name, final Object value) {
                                assert value instanceof String;
                                try {
                                    TimestampResolution.fromString((String) value);
                                } catch (final IllegalArgumentException e) {
                                    throw new ConfigException(EPOCH_RESOLUTION_CONFIG, value, e.getMessage());
                                }
                            }
                        },
                        ConfigDef.Importance.LOW,
                        EPOCH_RESOLUTION_DOC);
    }

    final String fieldName() {
        return getString(FIELD_NAME_CONFIG);
    }

    final TimestampResolution timestampResolution() {
        return TimestampResolution.fromString(getString(EPOCH_RESOLUTION_CONFIG));
    }

}
