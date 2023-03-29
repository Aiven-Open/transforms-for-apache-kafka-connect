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
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FilterByFieldValueTest {


    @Test
    void shouldFilterOutValueRecordsEqualsToReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
                "field.name", "op",
                "field.value", "r",
                "field.value.matches", "false"
        ));

        assertNull(filter.apply(
                        prepareStructRecord(
                                struct -> {
                                },
                                struct -> struct.put("op", "r")
                        )),
                "Record with op 'r' should be filtered out");
        SourceRecord record = prepareStructRecord(
                struct -> {
                },
                struct -> struct.put("op", "u")
        );
        assertEquals(record, filter.apply(record),
                "Record with op not 'r' should be not filtered out");
    }

    @Test
    void shouldFilterOutKeyRecordsEqualsToId() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Key<>();
        filter.configure(Map.of(
                "field.name", "id",
                "field.value", "123",
                "field.value.matches", "false"
        ));

        assertNull(filter.apply(prepareStructRecord(
                struct -> struct.put("id", "123"),
                struct -> {
                })), "Record with id '123' should be filtered out");
        SourceRecord record = prepareStructRecord(
                struct -> struct.put("id", "111"),
                struct -> {
                });
        assertEquals(record, filter.apply(record), "Record with id not '123' should not be filtered out");
    }

    @Test
    void shouldFilterOutValueRecordsNotEqualsReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
                "field.name", "op",
                "field.value", "r",
                "field.value.matches", "true"
        ));

        assertNull(filter.apply(prepareStructRecord(
                        struct -> {
                        },
                        struct -> struct.put("op", "u")
                )),
                "Record with op not equal to 'r' should be filtered out");
        SourceRecord record = prepareStructRecord(
                struct -> {
                },
                struct -> struct.put("op", "r")
        );
        assertEquals(record, filter.apply(record), "Record with op equal to 'r' should not be filtered out");
    }

    @Test
    void shouldFilterOutKeyRecordsNotEqualsToId() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Key<>();
        filter.configure(Map.of(
                "field.name", "id",
                "field.value", "123",
                "field.value.matches", "true"
        ));

        assertNull(filter.apply(prepareStructRecord(
                struct -> struct.put("id", "111"),
                struct -> {
                }
        )), "Record with id not equal to '123' should be filtered out");
        SourceRecord record = prepareStructRecord(
                struct -> struct.put("id", "123"),
                struct -> {
                }
        );
        assertEquals(record, filter.apply(record), "Record with id equal to '123' should not be filtered out");
    }

    @Test
    void shouldFilterOutMapValueRecordsWithRegex() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "language");
        configs.put("field.value.pattern", ".*Java.*");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertNull(filterByFieldValue.apply(prepareRecord(() -> "42", () -> Map.of("language", "Javascript"))), "The record should be filtered out");
        SourceRecord record = prepareRecord(() -> "42", () -> Map.of("language", "Rust"));
        assertEquals(record, filterByFieldValue.apply(record), "The record should not be filtered out");
    }

    @Test
    void shouldFilterOutMapKeyRecordsWithRegex() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Key<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "language");
        configs.put("field.value.pattern", ".*Java.*");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertNull(filterByFieldValue.apply(prepareRecord(() -> Map.of("language", "Javascript"), () -> "42")), "The record should be filtered out");
        SourceRecord record = prepareRecord(() -> Map.of("language", "Rust"), () -> "42");
        assertEquals(record, filterByFieldValue.apply(record), "The record should not be filtered out");
    }

    @Test
    void shouldFilterOutRawKeyRecords() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Key<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.value", "42");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertNull(filterByFieldValue.apply(prepareRecord(() -> "42", () -> Map.of("language", "Javascript"))), "The record should be filtered out");
        SourceRecord record = prepareRecord(() -> "43", () -> Map.of("language", "Rust"));
        assertEquals(record, filterByFieldValue.apply(record), "The record should be filtered out");
    }

    @Test
    void shouldFilterOutRawValueRecords() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.value", "42");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertNull(filterByFieldValue.apply(prepareRecord(() -> Map.of("language", "Javascript"), () -> "42")), "The record should be filtered out");
        SourceRecord record = prepareRecord(() -> Map.of("language", "Rust"), () -> "43");
        assertEquals(record, filterByFieldValue.apply(record), "The record should be filtered out");
    }

    private SourceRecord prepareRecord(Supplier<Object> keySupplier, Supplier<Object> valueSupplier) {
         return new SourceRecord(null, null, "some_topic",
                null, keySupplier.get(),
                null, valueSupplier.get());
    }

    private SourceRecord prepareStructRecord(Consumer<Struct> keyChanges, Consumer<Struct> valueChanges) {
        final Schema KEY_VALUE = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .build();
        final Schema VALUE_SCHEMA = SchemaBuilder.struct()
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

        final Struct key = new Struct(KEY_VALUE)
                .put("id", "123");
        keyChanges.accept(key);

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
        valueChanges.accept(value);

        return new SourceRecord(
                null, null, "some_topic",
                KEY_VALUE, key,
                VALUE_SCHEMA, value
        );
    }
}
