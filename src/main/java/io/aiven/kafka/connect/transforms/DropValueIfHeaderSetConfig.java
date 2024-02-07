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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

final class DropValueIfHeaderSetConfig extends AbstractConfig {

    public static final String HEADER_KEY_CONFIG = "header.key";
    private static final String HEADER_KEY_DOC = "The value of the header with this key is to be checked against the configured string value.";

    public static final String HEADER_VALUE_CONFIG = "header.string_value";
    private static final String HEADER_VALUE_DOC = "The value of the header must match this provided string value in order for the record to be dropped." +
            "If the value does not match (because the value is not a string, the header is not set or the two strings a different)," +
            "the record will not be modified.";

    DropValueIfHeaderSetConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(
                        HEADER_KEY_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        HEADER_KEY_DOC)
                .define(
                        HEADER_VALUE_CONFIG,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        HEADER_VALUE_DOC);
    }

    public String headerKey() {
        return getString(HEADER_KEY_CONFIG);
    }

    public String headerValue() {
        return getString(HEADER_VALUE_CONFIG);
    }
}