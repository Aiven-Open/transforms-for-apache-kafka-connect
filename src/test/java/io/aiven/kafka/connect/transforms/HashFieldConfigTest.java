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
import java.util.Optional;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HashFieldConfigTest {
    @Test
    void defaults() {
        final Map<String, String> props = new HashMap<>();
        final Throwable e = assertThrows(ConfigException.class,
            () -> new HashFieldConfig(props));
        assertEquals("Missing required configuration \"function\" which has no default value.",
              e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void skipMissingOrNull(final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        props.put(HashFieldConfig.SKIP_MISSING_OR_NULL_CONFIG, Boolean.toString(skipMissingOrNull));
        props.put(HashFieldConfig.FUNCTION_CONFIG, HashFieldConfig.HashFunction.SHA256.toString());
        final HashFieldConfig config = new HashFieldConfig(props);
        assertEquals(skipMissingOrNull, config.skipMissingOrNull());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void hashFunction(final String hashFunction) {
        final Map<String, String> props = new HashMap<>();
        props.put(HashFieldConfig.FUNCTION_CONFIG, hashFunction);
        final HashFieldConfig config = new HashFieldConfig(props);
        assertEquals(hashFunction, config.hashFunction().getAlgorithm());
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put(HashFieldConfig.FIELD_NAME_CONFIG, "");
        props.put(HashFieldConfig.FUNCTION_CONFIG, HashFieldConfig.HashFunction.SHA256.toString());
        final HashFieldConfig config = new HashFieldConfig(props);
        assertEquals(Optional.empty(), config.fieldName());
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put(HashFieldConfig.FIELD_NAME_CONFIG, "test");
        props.put(HashFieldConfig.FUNCTION_CONFIG, HashFieldConfig.HashFunction.SHA256.toString());
        final HashFieldConfig config = new HashFieldConfig(props);
        assertEquals(Optional.of("test"), config.fieldName());
        assertEquals(HashFieldConfig.HashFunction.SHA256.toString(), config.hashFunction().getAlgorithm());
    }
}
