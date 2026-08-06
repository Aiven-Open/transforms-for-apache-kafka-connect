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

import io.aiven.kafka.connect.transforms.utils.CursorField;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HashConfigTest {
    @Test
    void defaults() {
        final Map<String, String> props = new HashMap<>();
        assertThatThrownBy(() -> new HashConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"function\" which has no default value.");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void skipMissingOrNull(final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
        props.put("function", "sha256");
        final HashConfig config = new HashConfig(props);
        assertThat(config.skipMissingOrNull()).isEqualTo(skipMissingOrNull);
    }

    @Test
    void hashFunctionMd5() {
        final Map<String, String> props = new HashMap<>();
        props.put("function", "md5");
        final HashConfig config = new HashConfig(props);
        assertThat(config.hashFunction()).isEqualTo(HashConfig.HashFunction.MD5);
    }

    @Test
    void hashFunctionSha1() {
        final Map<String, String> props = new HashMap<>();
        props.put("function", "sha1");
        final HashConfig config = new HashConfig(props);
        assertThat(config.hashFunction()).isEqualTo(HashConfig.HashFunction.SHA1);
    }

    @Test
    void hashFunctionSha256() {
        final Map<String, String> props = new HashMap<>();
        props.put("function", "sha256");
        final HashConfig config = new HashConfig(props);
        assertThat(config.hashFunction()).isEqualTo(HashConfig.HashFunction.SHA256);
    }

    @Test
    void emptyFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "");
        props.put("function", "sha256");
        final HashConfig config = new HashConfig(props);
        assertThat(config.field()).isNotPresent();
    }

    @Test
    void definedFieldName() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "test");
        props.put("function", "sha256");
        final HashConfig config = new HashConfig(props);
        assertThat(config.field().map(CursorField::getCursor)).hasValue("test");
        assertThat(config.hashFunction()).isEqualTo(HashConfig.HashFunction.SHA256);
    }
}
