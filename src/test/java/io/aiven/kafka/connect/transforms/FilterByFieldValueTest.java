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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class FilterByFieldValueTest {

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .field("before", Schema.OPTIONAL_STRING_SCHEMA)
            .field("after", SchemaBuilder.struct()
                    .field("pk", Schema.STRING_SCHEMA)
                    .field("value", Schema.STRING_SCHEMA)
                    .build())
            .field("source", SchemaBuilder.struct().optional())
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.STRING_SCHEMA)
            .field("transaction", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    @Test
    void shouldFilterOutRecordsEqualsToReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
                "field.name", "op",
                "field.value", "r",
                "field.value.matches", "false"
        ));

        final Struct after = new Struct(VALUE_SCHEMA.field("after").schema())
                .put("pk", "1")
                .put("value", "New data");

        final Struct value = new Struct(VALUE_SCHEMA)
                .put("before", null)
                .put("after", after)
                .put("source", null)
                .put("op", "r")
                .put("ts_ms", "1620393591654")
                .put("transaction", null);

        final var record = new SourceRecord(null, null, "some_topic", VALUE_SCHEMA, value);

        final var actual = filter.apply(record);
        assertNull(actual, "Record with op 'r' should be filtered out");
    }

    @Test
    void shouldKeepRecordsNotEqualsToReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
                "field.name", "op",
                "field.value", "r",
                "field.value.matches", "false"
        ));

        final Struct after = new Struct(VALUE_SCHEMA.field("after").schema())
                .put("pk", "1")
                .put("value", "New data");

        final Struct value = new Struct(VALUE_SCHEMA)
                .put("before", null)
                .put("after", after)
                .put("source", null)
                .put("op", "u")
                .put("ts_ms", "1620393591654")
                .put("transaction", null);

        final var record = new SourceRecord(null, null, "some_topic", VALUE_SCHEMA, value);

        final var actual = filter.apply(record);
        assertEquals(record, actual, "Record with op not equal to 'r' should be kept");
    }

    @Test
    void shouldFilterOutRecordsNotEqualsReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
                "field.name", "op",
                "field.value", "r",
                "field.value.matches", "true"
        ));

        final Struct after = new Struct(VALUE_SCHEMA.field("after").schema())
                .put("pk", "1")
                .put("value", "New data");

        final Struct value = new Struct(VALUE_SCHEMA)
                .put("before", null)
                .put("after", after)
                .put("source", null)
                .put("op", "u")
                .put("ts_ms", "1620393591654")
                .put("transaction", null);

        final var record = new SourceRecord(null, null, "some_topic", VALUE_SCHEMA, value);

        final var actual = filter.apply(record);
        assertNull(actual, "Record with op not equal to 'r' should be filtered out");
    }

    @Test
    void shouldFilterOutMapFieldValue() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "language");
        configs.put("field.value.pattern", ".*Java.*");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        value.put("language", "Java");

        final var record = new SourceRecord(null, null, "some_topic", null, value);

        final var actual = filterByFieldValue.apply(record);
        assertNull(actual, "The record should be filtered out");
    }
}
