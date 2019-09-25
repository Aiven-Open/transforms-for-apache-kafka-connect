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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ExtractTimestampConfigTest {
    @Test
    void emptyConfig() {
        final Map<String, String> props = new HashMap<>();
        final Throwable e = assertThrows(ConfigException.class, () -> new ExtractTimestampConfig(props));
        assertEquals("Missing required configuration \"field.name\" which has no default value.",
            e.getMessage());
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "");
        final Throwable e = assertThrows(ConfigException.class, () -> new ExtractTimestampConfig(props));
        assertEquals("Invalid value  for configuration field.name: String must be non-empty",
            e.getMessage());
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "test");
        final ExtractTimestampConfig config = new ExtractTimestampConfig(props);
        assertEquals("test", config.fieldName());
    }
}
