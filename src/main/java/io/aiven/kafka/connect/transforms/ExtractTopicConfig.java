/*
 * Copyright 2019 Aiven Oy
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

import java.util.Map;
import java.util.Optional;

import io.aiven.kafka.connect.transforms.utils.CursorField;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

class ExtractTopicConfig extends AbstractConfig {
    public static final String FIELD_NAME_CONFIG = "field.name";
    private static final String FIELD_NAME_DOC =
        "The name of the field which should be used as the topic name. "
        + "If null or empty, the entire key or value is used (and assumed to be a string).";

    public static final String SKIP_MISSING_OR_NULL_CONFIG = "skip.missing.or.null";
    private static final String SKIP_MISSING_OR_NULL_DOC =
        "In case the source of the new topic name is null or missing, "
        + "should a record be silently passed without transformation.";

    public static final String APPEND_TO_ORIGINAL_TOPIC_NAME_CONFIG = "append.to.topic";
    private static final String APPEND_TO_ORIGINAL_TOPIC_NAME_DOC =
        "Appends the selected value to the existing topic name to derive the new topic name.";

    public static final String APPEND_DELIMITER_CONFIG = "append.to.topic.delimiter";
    private static final String APPEND_DELIMITER_DOC =
        "Appends the selected value with the given delimiter to the existing topic name.";

    ExtractTopicConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef()
            .define(
                FIELD_NAME_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                FIELD_NAME_DOC)
            .define(
                SKIP_MISSING_OR_NULL_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                SKIP_MISSING_OR_NULL_DOC)
            .define(
                APPEND_TO_ORIGINAL_TOPIC_NAME_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                APPEND_TO_ORIGINAL_TOPIC_NAME_DOC)
            .define(
                APPEND_DELIMITER_CONFIG,
                ConfigDef.Type.STRING,
                "-",
                ConfigDef.Importance.LOW,
                APPEND_DELIMITER_DOC);
    }

    Optional<CursorField> field() {
        final String rawFieldName = getString(FIELD_NAME_CONFIG);
        if (null == rawFieldName || "".equals(rawFieldName)) {
            return Optional.empty();
        }
        return Optional.of(new CursorField(rawFieldName));
    }

    boolean skipMissingOrNull() {
        return getBoolean(SKIP_MISSING_OR_NULL_CONFIG);
    }

    boolean appendToExisting() {
        return getBoolean(APPEND_TO_ORIGINAL_TOPIC_NAME_CONFIG);
    }

    String appendDelimiter() {
        return getString(APPEND_DELIMITER_CONFIG);
    }
}
