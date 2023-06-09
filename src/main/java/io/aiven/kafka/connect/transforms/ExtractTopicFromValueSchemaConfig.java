/*
 * Copyright 2023 Aiven Oy
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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class ExtractTopicFromValueSchemaConfig extends AbstractConfig {
    public static final String SCHEMA_NAME_TO_TOPIC = "schema.name.topic-map";
    public static final String REGEX_SCHEMA_NAME_TO_TOPIC = "schema.name.regex";

    public static final String SCHEMA_NAME_TO_TOPIC_DOC = "Map of schema name (key), "
            + "new topic name (value) in String format \"key1:value1,key2:value2\"";

    public static final String REGEX_SCHEMA_NAME_TO_TOPIC_DOC = "Regular expression that is used to find the "
            + "first desired new topic value from value schema name "
            + "(for example (?:[.\\t]|^)([^.\\t]*)$ which parses the name after last .";

    public ExtractTopicFromValueSchemaConfig(final Map<?, ?> originals) {
        super(config(), originals);
    }

    static ConfigDef config() {
        return new ConfigDef().define(
                        SCHEMA_NAME_TO_TOPIC,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        SCHEMA_NAME_TO_TOPIC_DOC)
                .define(REGEX_SCHEMA_NAME_TO_TOPIC,
                        ConfigDef.Type.STRING,
                        null,
                        ConfigDef.Importance.LOW,
                        REGEX_SCHEMA_NAME_TO_TOPIC_DOC
                );
    }

    Map<String, String> schemaNameToTopicMap() {
        final String schemaNameToTopicValue = getString(SCHEMA_NAME_TO_TOPIC);
        if (null == schemaNameToTopicValue) {
            return Collections.emptyMap();
        }
        return Arrays.stream(schemaNameToTopicValue.split(",")).map(entry -> entry.split(":"))
            .collect(Collectors.toMap(key -> key[0], value -> value[1]));
    }

    Optional<String> regEx() {
        final String rawFieldName = getString(REGEX_SCHEMA_NAME_TO_TOPIC);
        if (null == rawFieldName || rawFieldName.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(rawFieldName);
    }
}
