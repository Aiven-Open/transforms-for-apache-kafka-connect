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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ExtractTopicFromSchemaNameConfigTest {

    @Test
    void defaults() {
        final Map<String, String> configs = new HashMap<>();
        assertNotNull(new ExtractTopicFromSchemaNameConfig(configs));
    }

    @Test
    void testAllConfigsSet() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromSchemaNameConfig.REGEX_SCHEMA_NAME_TO_TOPIC, "regex");
        configs.put(ExtractTopicFromSchemaNameConfig.SCHEMA_NAME_TO_TOPIC_MAP, "map:value");
        final Throwable e = assertThrows(ConfigException.class,
            () -> new ExtractTopicFromSchemaNameConfig(configs));
        assertEquals("schema.name.topic-map and schema.name.regex should not be defined together.",
            e.getMessage());
    }

    @Test
    void testRegExConfigSetWithNameToTopicMap() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromSchemaNameConfig.SCHEMA_NAME_TO_TOPIC_MAP,
            "com.acme.schema.SchemaNameToTopic1:Name1,com.acme.schema.SchemaNameToTopic2:Name2");
        final ExtractTopicFromSchemaNameConfig extractTopicFromSchemaNameConfig
            = new ExtractTopicFromSchemaNameConfig(configs);
        assertEquals(2,
            extractTopicFromSchemaNameConfig.schemaNameToTopicMap().size());
    }

    @Test
    void testRegExConfigSetWithInvalidNameToTopicMap() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromSchemaNameConfig.SCHEMA_NAME_TO_TOPIC_MAP,
            "com.acme.schema.SchemaNameToTopic1TheNameToReplace1");
        final Throwable e = assertThrows(ConfigException.class,
            () -> new ExtractTopicFromSchemaNameConfig(configs));
        assertEquals("schema.name.topic-map is not valid. Format should be: "
            + "\"SchemaValue1:NewValue1,SchemaValue2:NewValue2\"", e.getMessage());
    }

    @Test
    void testRegExConfigSetWithInvalidRegEx() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromSchemaNameConfig.REGEX_SCHEMA_NAME_TO_TOPIC, "***");
        final Throwable e = assertThrows(ConfigException.class,
            () -> new ExtractTopicFromSchemaNameConfig(configs));
        assertEquals("*** set as schema.name.regex is not valid regex.", e.getMessage());
    }

    @Test
    void testRegExConfigSetWithValidRegEx() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromSchemaNameConfig.REGEX_SCHEMA_NAME_TO_TOPIC, "(?:[.]|^)([^.]*)$");
        final ExtractTopicFromSchemaNameConfig extractTopicFromSchemaNameConfig
            = new ExtractTopicFromSchemaNameConfig(configs);
        assertEquals("(?:[.]|^)([^.]*)$", extractTopicFromSchemaNameConfig.regEx().get());
    }
}
