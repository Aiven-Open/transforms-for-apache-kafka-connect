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

import io.aiven.kafka.connect.transforms.utils.CursorField;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.DELIMITER_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_NAMES_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_REPLACE_MISSING_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.OUTPUT_FIELD_NAME_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConcatFieldsConfigTest {
    @Test
    void emptyConfig() {
        final Map<String, String> props = new HashMap<>();
        assertThatThrownBy(() -> new ConcatFieldsConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Missing required configuration \"field.names\" which has no default value.");
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put(FIELD_NAMES_CONFIG, "");
        assertThatThrownBy(() -> new ConcatFieldsConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Missing required configuration \"output.field.name\" which has no default value.");
    }

    @Test
    void definedFieldName() {
        final Map props = new HashMap<>();
        props.put(FIELD_NAMES_CONFIG, Arrays.asList("test", "foo", "bar"));
        props.put(OUTPUT_FIELD_NAME_CONFIG, "combined");
        props.put(DELIMITER_CONFIG, "-");
        props.put(FIELD_REPLACE_MISSING_CONFIG, "*");
        final ConcatFieldsConfig config = new ConcatFieldsConfig(props);
        assertThat(config.fields()
                         .stream().map(CursorField::getCursor)).containsOnly("test", "foo", "bar");
        assertThat(config.outputFieldName()).isEqualTo("combined");
        assertThat(config.delimiter()).isEqualTo("-");
        assertThat(config.fieldReplaceMissing()).isEqualTo("*");
    }
}
