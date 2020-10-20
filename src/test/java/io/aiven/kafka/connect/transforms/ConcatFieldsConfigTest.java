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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.DELIMITER_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_NAMES_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_REPLACE_MISSING_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.OUTPUT_FIELD_NAME_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConcatFieldsConfigTest {
    @Test
    void emptyConfig() {
        final Map<String, String> props = new HashMap<>();
        final Throwable e = assertThrows(ConfigException.class, () -> new ConcatFieldsConfig(props));
        assertEquals("Missing required configuration \"field.names\" which has no default value.",
            e.getMessage());
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put(FIELD_NAMES_CONFIG, "");
        final Throwable e = assertThrows(ConfigException.class, () -> new ConcatFieldsConfig(props));
        assertEquals("Missing required configuration \"output.field.name\" which has no default value.",
            e.getMessage());
    }

    @Test
    void definedFieldName() {
        final Map props = new HashMap<>();
        props.put(FIELD_NAMES_CONFIG, Arrays.asList("test", "foo", "bar"));
        props.put(OUTPUT_FIELD_NAME_CONFIG, "combined");
        props.put(DELIMITER_CONFIG, "-");
        props.put(FIELD_REPLACE_MISSING_CONFIG, "*");
        final ConcatFieldsConfig config = new ConcatFieldsConfig(props);
        assertEquals(Arrays.asList("test", "foo", "bar"), config.fieldNames());
        assertEquals("combined", config.outputFieldName());
        assertEquals("-", config.delimiter());
        assertEquals("*", config.fieldReplaceMissing());
    }
}
