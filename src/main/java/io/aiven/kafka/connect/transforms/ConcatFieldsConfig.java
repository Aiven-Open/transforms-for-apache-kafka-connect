/*
 * Copyright 2021 Aiven Oy
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
import java.util.stream.Collectors;

import io.aiven.kafka.connect.transforms.utils.CursorField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static java.util.stream.Collectors.toList;

final class ConcatFieldsConfig extends AbstractConfig {
    public static final String FIELD_NAMES_CONFIG = "field.names";
    private static final String FIELD_NAMES_DOC =
        "A comma-separated list of fields to concatenate.";
    public static final String OUTPUT_FIELD_NAME_CONFIG = "output.field.name";
    private static final String OUTPUT_FIELD_NAME_DOC =
        "The name of field the concatenated value should be placed into.";
    public static final String DELIMITER_CONFIG = "delimiter";
    private static final String DELIMITER_DOC =
        "The string which should be used to join the extracted fields.";
    public static final String FIELD_REPLACE_MISSING_CONFIG = "field.replace.missing";
    private static final String FIELD_REPLACE_MISSING_DOC =
        "The string which should be used when a field is not found or its value is null.";

    ConcatFieldsConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
            .define(
                FIELD_NAMES_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                FIELD_NAMES_DOC)
            .define(
                OUTPUT_FIELD_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OUTPUT_FIELD_NAME_DOC)
            .define(
                FIELD_REPLACE_MISSING_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                FIELD_REPLACE_MISSING_DOC)
            .define(
                DELIMITER_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                DELIMITER_DOC);
    }

    final List<CursorField> fields() {
        return getList(FIELD_NAMES_CONFIG).stream().map(CursorField::new)
                .collect(toList());
    }

    final String outputFieldName() {
        return getString(OUTPUT_FIELD_NAME_CONFIG);
    }

    final String fieldReplaceMissing() {
        return getString(FIELD_REPLACE_MISSING_CONFIG);
    }

    final String delimiter() {
        return getString(DELIMITER_CONFIG);
    }
}
