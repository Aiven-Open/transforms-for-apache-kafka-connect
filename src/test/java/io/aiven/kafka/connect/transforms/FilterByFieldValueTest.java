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

import static org.assertj.core.api.Assertions.assertThat;

class FilterByFieldValueTest {


    @Test
    void shouldFilterOutValueRecordsEqualsToReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
            "field.name", "op",
            "field.value", "r",
            "field.value.matches", "false"
        ));

        assertThat(filter.apply(
            prepareStructRecord(
                struct -> {
                },
                struct -> struct.put("op", "r")
            )))
            .as("Record with op 'r' should be filtered out")
            .isNull();
        final SourceRecord record = prepareStructRecord(
            struct -> {
            },
            struct -> struct.put("op", "u")
        );
        assertThat(filter.apply(record))
            .as("Record with op not 'r' should be not filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutKeyRecordsEqualsToId() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Key<>();
        filter.configure(Map.of(
            "field.name", "id",
            "field.value", "A123",
            "field.value.matches", "false"
        ));

        assertThat(filter.apply(prepareStructRecord(
            struct -> struct.put("id", "A123"),
            struct -> {
            })))
            .as("Record with id 'A132' should be filtered out")
            .isNull();
        final SourceRecord record = prepareStructRecord(
            struct -> struct.put("id", "A111"),
            struct -> {
            });
        assertThat(filter.apply(record))
            .as("Record with id not 'A132' should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutValueRecordsNotEqualsReadEvents() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
            "field.name", "op",
            "field.value", "r",
            "field.value.matches", "true"
        ));

        assertThat(filter.apply(
            prepareStructRecord(
                struct -> {
                },
                struct -> struct.put("op", "u")
            )))
            .as("Record with op not equal to 'r' should be filtered out")
            .isNull();
        final SourceRecord record = prepareStructRecord(
            struct -> {
            },
            struct -> struct.put("op", "r")
        );
        assertThat(filter.apply(record))
            .as("Record with op equal to 'r' should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutKeyRecordsNotEqualsToId() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Key<>();
        filter.configure(Map.of(
            "field.name", "id",
            "field.value", "A123",
            "field.value.matches", "true"
        ));

        assertThat(filter.apply(
            prepareStructRecord(
                struct -> struct.put("id", "111"),
                struct -> {
                }
            )))
            .as("Record with id not equal to 'A132' should be filtered out")
            .isNull();
        final SourceRecord record = prepareStructRecord(
            struct -> struct.put("id", "A123"),
            struct -> {
            }
        );
        assertThat(filter.apply(record))
            .as("Record with id equal to 'A132' should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutRecordNotEqualsToIntId() {
        final FilterByFieldValue<SourceRecord> filter = new FilterByFieldValue.Value<>();
        filter.configure(Map.of(
            "field.name", "EventId",
            "field.value", "4690",
            "field.value.matches", "true"
        ));

        assertThat(filter.apply(prepareRecord(() -> "A42", () -> Map.of("EventId", 4663))))
            .as("Record with id not equal to 'A132' should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> "A42", () -> Map.of("EventId", 4690));
        assertThat(filter.apply(record))
            .as("Record with id equal to '4690' should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutMapValueRecordsWithRegex() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "language");
        configs.put("field.value.pattern", ".*Java.*");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertThat(filterByFieldValue.apply(prepareRecord(() -> "A42", () -> Map.of("language", "Javascript"))))
            .as("The record should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> "A42", () -> Map.of("language", "Rust"));
        assertThat(filterByFieldValue.apply(record))
            .as("The record should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutMapKeyRecordsWithRegex() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Key<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.name", "language");
        configs.put("field.value.pattern", ".*Java.*");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertThat(filterByFieldValue.apply(prepareRecord(() -> Map.of("language", "Javascript"), () -> "A42")))
            .as("The record should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> Map.of("language", "Rust"), () -> "A42");
        assertThat(filterByFieldValue.apply(record))
            .as("The record should not be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutRawKeyRecords() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Key<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.value", "A42");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertThat(filterByFieldValue.apply(prepareRecord(() -> "A42", () -> Map.of("language", "Javascript"))))
            .as("The record should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> "43", () -> Map.of("language", "Rust"));
        assertThat(filterByFieldValue.apply(record))
            .as("The record should be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutRawValueRecords() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.value", "A42");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertThat(filterByFieldValue.apply(prepareRecord(() -> Map.of("language", "Javascript"), () -> "A42")))
            .as("The record should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> Map.of("language", "Rust"), () -> "43");
        assertThat(filterByFieldValue.apply(record))
            .as("The record should be filtered out")
            .isEqualTo(record);
    }

    @Test
    void shouldFilterOutRawNumericValueRecords() {
        final FilterByFieldValue<SourceRecord> filterByFieldValue = new FilterByFieldValue.Value<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("field.value", "42");
        configs.put("field.value.matches", "false");
        filterByFieldValue.configure(configs);

        assertThat(filterByFieldValue.apply(prepareRecord(() -> Map.of("language", "Javascript"), () -> (byte) 42)))
            .as("The record should be filtered out")
            .isNull();
        final SourceRecord record = prepareRecord(() -> Map.of("language", "Rust"), () -> (byte) 43);
        assertThat(filterByFieldValue.apply(record))
            .as("The record should be filtered out")
            .isEqualTo(record);
    }

    private SourceRecord prepareRecord(
        final Supplier<Object> keySupplier,
        final Supplier<Object> valueSupplier
    ) {
        return new SourceRecord(null, null, "some_topic",
            null, keySupplier.get(),
            null, valueSupplier.get());
    }

    private SourceRecord prepareStructRecord(
        final Consumer<Struct> keyChanges,
        final Consumer<Struct> valueChanges
    ) {
        final Schema keySchema = SchemaBuilder.struct()
            .field("id", Schema.STRING_SCHEMA)
            .build();
        final Schema valueSchema = SchemaBuilder.struct()
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

        final Struct key = new Struct(keySchema)
            .put("id", "A123");
        keyChanges.accept(key);

        final Struct after = new Struct(valueSchema.field("after").schema())
            .put("pk", "1")
            .put("value", "New data");

        final Struct value = new Struct(valueSchema)
            .put("before", null)
            .put("after", after)
            .put("source", null)
            .put("op", "u")
            .put("ts_ms", "1620393591654")
            .put("transaction", null);
        valueChanges.accept(value);

        return new SourceRecord(
            null, null, "some_topic",
            keySchema, key,
            valueSchema, value
        );
    }
}
