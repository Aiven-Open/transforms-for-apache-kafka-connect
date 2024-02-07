/*
 * Copyright 2020 Aiven Oy
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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.aiven.kafka.connect.transforms.DropValueIfHeaderSetConfig.HEADER_KEY_CONFIG;
import static io.aiven.kafka.connect.transforms.DropValueIfHeaderSetConfig.HEADER_VALUE_CONFIG;
import static java.time.LocalTime.now;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class DropValueIfHeaderSetTest {


    DropValueIfHeaderSet<SourceRecord> underTest;
    @BeforeEach
    void setup() {
        underTest = new DropValueIfHeaderSet<>();
        underTest.configure(Map.of(
                HEADER_KEY_CONFIG, "archived",
                HEADER_VALUE_CONFIG, "True"
        ));
    }

    @Test
    void shouldNotChangeRecordWhenHeaderNotSet() {
        final var unrelatedHeaders = new ConnectHeaders().addString("span id", "517CFD6B-CA19-44AB-B729-C249CFD7C1C9");
        final var record = new SourceRecord(null,
                null,
                "some_topic",
                0,
                Schema.STRING_SCHEMA,
                "dummy key",
                Schema.STRING_SCHEMA,
                "dummy value",
                0L,
                unrelatedHeaders
                );

        final var actual = underTest.apply(record);

        assertThat(actual).isEqualTo(record);
    }

    @Test
    void shouldNotChangeRecordWhenHeaderIsSetToOtherValue() {
        final var headers = new ConnectHeaders().addString("archived", "yes");
        final var record = new SourceRecord(null,
                null,
                "some_topic",
                0,
                Schema.STRING_SCHEMA,
                "dummy key",
                Schema.STRING_SCHEMA,
                "dummy value",
                0L,
                headers
        );

        final var actual = underTest.apply(record);

        assertThat(actual).isEqualTo(record);
    }

    @Test
    void shouldDropValueWhenHeaderIsSetToConfiguredValue() {
        final var headers = new ConnectHeaders().addString("archived", "True");
        final var record = new SourceRecord(null,
                null,
                "some_topic",
                0,
                Schema.STRING_SCHEMA,
                "dummy key",
                Schema.STRING_SCHEMA,
                "dummy value",
                0L,
                headers
        );

        final var actual = underTest.apply(record);

        assertThat(actual.value()).isNull();
        assertThat(actual.valueSchema()).isNull();
    }
}
