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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExtractTimestampConfigTest {
    @Test
    void emptyConfig() {
        final Map<String, String> props = new HashMap<>();
        assertThatThrownBy(() -> new ExtractTimestampConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"field.name\" which has no default value.");
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "");
        assertThatThrownBy(() -> new ExtractTimestampConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration field.name: String must be non-empty");
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "test");
        final ExtractTimestampConfig config = new ExtractTimestampConfig(props);
        assertThat(config.fieldName()).isEqualTo("test");
    }

    @Test
    void emptyTimestampResolution() {
        final var props = new HashMap<>();
        props.put("field.name", "test");
        final var config = new ExtractTimestampConfig(props);
        assertThat(config.timestampResolution()).isEqualTo(ExtractTimestampConfig.TimestampResolution.MILLISECONDS);
    }

    @Test
    void definedTimestampResolutionInSeconds() {
        final var props = new HashMap<>();
        props.put("field.name", "test");
        props.put(
                ExtractTimestampConfig.EPOCH_RESOLUTION_CONFIG,
                ExtractTimestampConfig.TimestampResolution.SECONDS.resolution
        );
        final var config = new ExtractTimestampConfig(props);
        assertThat(config.timestampResolution()).isEqualTo(ExtractTimestampConfig.TimestampResolution.SECONDS);
    }

    @Test
    void definedTimestampResolutionInMillis() {
        final var props = new HashMap<>();
        props.put("field.name", "test");
        props.put(
                ExtractTimestampConfig.EPOCH_RESOLUTION_CONFIG,
                ExtractTimestampConfig.TimestampResolution.MILLISECONDS.resolution
        );
        final var config = new ExtractTimestampConfig(props);
        assertThat(config.timestampResolution()).isEqualTo(ExtractTimestampConfig.TimestampResolution.MILLISECONDS);
    }

    @Test
    void wrongTimestampResolution() {
        final var props = new HashMap<>();
        props.put("field.name", "test");
        props.put(
                ExtractTimestampConfig.EPOCH_RESOLUTION_CONFIG,
                "foo"
        );
        assertThatThrownBy(() -> new ExtractTimestampConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value foo for configuration timestamp.resolution: "
                + "Unsupported resolution type 'foo'. Supported are: milliseconds, seconds");
    }

}
