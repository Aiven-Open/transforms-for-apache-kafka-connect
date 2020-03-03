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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public final class TombstoneHandlerConfig extends AbstractConfig {

    public static final String TOMBSTONE_HANDLER_BEHAVIOR = "behavior";

    public TombstoneHandlerConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
            .define(
                TOMBSTONE_HANDLER_BEHAVIOR,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        assert value instanceof String;

                        final String strValue = (String) value;

                        if (Objects.isNull(strValue) || strValue.isEmpty()) {
                            throw new ConfigException(
                                TOMBSTONE_HANDLER_BEHAVIOR,
                                value,
                                "String must be non-empty");
                        }

                        try {
                            Behavior.of(strValue);
                        } catch (final IllegalArgumentException e) {
                            throw new ConfigException(
                                TOMBSTONE_HANDLER_BEHAVIOR,
                                value,
                                e.getMessage());
                        }
                    }
                },
                ConfigDef.Importance.MEDIUM,
                String.format(
                    "Specifies the behavior on encountering tombstone messages. Possible values are: %s",
                    Behavior.BEHAVIOR_NAMES
                )
            );
    }

    public Behavior getBehavior() {
        return Behavior.of(getString(TOMBSTONE_HANDLER_BEHAVIOR));
    }

    public enum Behavior {

        DROP_SILENT,
        DROP_WARN,
        FAIL;

        private static final List<String> BEHAVIOR_NAMES =
            Arrays.stream(Behavior.values())
                .map(b -> b.name().toLowerCase())
                .collect(Collectors.toList());

        public static Behavior of(final String v) {

            for (final Behavior b : Behavior.values()) {
                if (b.name().equalsIgnoreCase(v)) {
                    return b;
                }
            }
            throw new IllegalArgumentException(
                String.format(
                    "Unsupported behavior name: %s. Supported are: %s", v,
                        String.join(",", BEHAVIOR_NAMES))
            );

        }

    }

}
