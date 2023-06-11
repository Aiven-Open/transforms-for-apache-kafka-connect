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
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class ExtractTopicFromSchemaNameConfig extends AbstractConfig {
    public static final String SCHEMA_NAME_TO_TOPIC_MAP = "schema.name.topic-map";
    public static final String REGEX_SCHEMA_NAME_TO_TOPIC = "schema.name.regex";

    public static final String SCHEMA_NAME_TO_TOPIC_DOC = "Map of schema name (key), "
            + "new topic name (value) in String format \"key1:value1,key2:value2\"";

    public static final String REGEX_SCHEMA_NAME_TO_TOPIC_DOC = "Regular expression that is used to find the "
            + "first desired new topic value from value schema name "
            + "(for example (?:[.\\t]|^)([^.\\t]*)$ which parses the name after last .";

    public ExtractTopicFromSchemaNameConfig(final Map<?, ?> originals) {
        super(config(), originals);

        if (originals.containsKey(SCHEMA_NAME_TO_TOPIC_MAP) && originals.containsKey(REGEX_SCHEMA_NAME_TO_TOPIC)) {
            throw new ConfigException(SCHEMA_NAME_TO_TOPIC_MAP + " and "
                    +  REGEX_SCHEMA_NAME_TO_TOPIC + " should not be defined together.");
        }

        if (originals.containsKey(REGEX_SCHEMA_NAME_TO_TOPIC)) {
            final String regex = (String) originals.get(REGEX_SCHEMA_NAME_TO_TOPIC);
            try {
                Pattern.compile(regex);
            } catch (final PatternSyntaxException e) {
                throw new ConfigException(regex  + " set as " + REGEX_SCHEMA_NAME_TO_TOPIC + " is not valid regex.");
            }
        }

        if (originals.containsKey(SCHEMA_NAME_TO_TOPIC_MAP)) {
            final String mapString = (String) originals.get(SCHEMA_NAME_TO_TOPIC_MAP);
            if (!mapString.contains(":")) {
                throw new ConfigException(SCHEMA_NAME_TO_TOPIC_MAP + " is not valid. Format should be: "
                        + "\"SchemaValue1:NewValue1,SchemaValue2:NewValue2\"");
            }
        }
    }

    static ConfigDef config() {
        return new ConfigDef().define(
                        SCHEMA_NAME_TO_TOPIC_MAP,
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
        final String schemaNameToTopicValue = getString(SCHEMA_NAME_TO_TOPIC_MAP);
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
