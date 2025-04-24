/*
 * Copyright 2025 Aiven Oy
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

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

/**
 * <p>Configure the SMT to copy fields from the key into the value.</p>
 *
 * @link org.apache.kafka.connect.transforms.ValueToKey transform for a similar approach
 */
public class KeyToValueConfig extends AbstractConfig {


    public static final String KEY_FIELDS_CONFIG = "key.fields";
    public static final String VALUE_FIELDS_CONFIG = "value.fields";
    /**
     * If the value (destination) column isn't specified and the wildcard '*' is used, this will be the name of the
     * column.
     */
    public static final String DEFAULT_WHOLE_KEY_FIELD = "_key";
    private static final String KEY_FIELDS_DOC = "Comma-separated list of field names on the record key to copy into "
            + "the record value (or * to copy the entire key).";
    private static final String VALUE_FIELDS_DOC = "Corresponding destination field names to be added or replaced on "
            + "the record value, or empty if the same field name should be used as the key.";

    KeyToValueConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef().define(KEY_FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        KEY_FIELDS_DOC)
                .define(VALUE_FIELDS_CONFIG,
                        ConfigDef.Type.LIST,
                        "",
                        ConfigDef.Importance.HIGH,
                        VALUE_FIELDS_DOC);
    }

    final List<String> keyFields() {
        return getList(KEY_FIELDS_CONFIG);
    }

    final List<String> valueFields() {
        return getList(VALUE_FIELDS_CONFIG);
    }
}
