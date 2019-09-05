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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ExtractTopicConfigTest {
    @Test
    void defaults() {
        final Map<String, String> props = new HashMap<>();
        final ExtractTopicConfig config = new ExtractTopicConfig(props);
        assertEquals(Optional.empty(), config.fieldName());
        assertFalse(config.skipMissingOrNull());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void skipMissingOrNull(final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
        final ExtractTopicConfig config = new ExtractTopicConfig(props);
        assertEquals(skipMissingOrNull, config.skipMissingOrNull());
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "");
        final ExtractTopicConfig config = new ExtractTopicConfig(props);
        assertEquals(Optional.empty(), config.fieldName());
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "test");
        final ExtractTopicConfig config = new ExtractTopicConfig(props);
        assertEquals(Optional.of("test"), config.fieldName());
    }
}
