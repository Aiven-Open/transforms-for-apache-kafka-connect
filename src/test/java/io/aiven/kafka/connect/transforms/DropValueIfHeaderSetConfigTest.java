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

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static io.aiven.kafka.connect.transforms.DropValueIfHeaderSetConfig.HEADER_KEY_CONFIG;
import static io.aiven.kafka.connect.transforms.DropValueIfHeaderSetConfig.HEADER_VALUE_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DropValueIfHeaderSetConfigTest {

    @Test
    void emptyConfig() {
        final Map<String, String> props = new HashMap<>();
        assertThatThrownBy(() -> new DropValueIfHeaderSetConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Missing required configuration \"header.key\" which has no default value.");
    }

    @Test
    void emptyHeaderKey() {
        final Map<String, String> props = new HashMap<>();
        props.put(HEADER_KEY_CONFIG, "");
        assertThatThrownBy(() -> new DropValueIfHeaderSetConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration header.key: String must be non-empty");
    }

    @Test
    void nullHeaderValue() {
        final Map<String, String> props = new HashMap<>();
        props.put(HEADER_KEY_CONFIG, "archived");
        assertThatThrownBy(() -> new DropValueIfHeaderSetConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Missing required configuration \"header.string_value\" which has no default value.");
    }

    @Test
    void emptyHeaderValue() {
        final Map<String, String> props = new HashMap<>();
        props.put(HEADER_KEY_CONFIG, "archived");
        props.put(HEADER_VALUE_CONFIG, "");
        assertThatThrownBy(() -> new DropValueIfHeaderSetConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value  for configuration header.string_value: String must be non-empty");
    }

    @Test
    void definedFieldName() {
        final Map<String, Object> props = new HashMap<>();
        props.put(HEADER_KEY_CONFIG, "archived");
        props.put(HEADER_VALUE_CONFIG, "True");
        final DropValueIfHeaderSetConfig config = new DropValueIfHeaderSetConfig(props);
        assertThat(config.headerKey()).isEqualTo("archived");
        assertThat(config.headerValue()).isEqualTo("True");
    }
}
