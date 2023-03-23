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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FilterByValueRegexTest {

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
        final FilterByValueRegex<SourceRecord> filter = new FilterByValueRegex<>();
        filter.configure(Map.of(
                "fieldName", "op",
                "pattern", "r",
                "matches", "false"
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
        final FilterByValueRegex<SourceRecord> filter = new FilterByValueRegex<>();
        filter.configure(Map.of(
                "fieldName", "op",
                "pattern", "r",
                "matches", "false"
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
        final FilterByValueRegex<SourceRecord> filter = new FilterByValueRegex<>();
        filter.configure(Map.of(
                "fieldName", "op",
                "pattern", "r",
                "matches", "true"
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
    void shouldFilterMatchingArrayFieldValue() {
        final FilterByValueRegex<SourceRecord> filterByValueRegex = new FilterByValueRegex<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("fieldName", "tags");
        configs.put("pattern", ".*apple.*");
        configs.put("matches", "true");
        filterByValueRegex.configure(configs);

        final Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA))
                .build();
        final List<String> tags = Arrays.asList("apple", "orange", "mango");
        final Struct value = new Struct(schema)
                .put("name", "John Doe")
                .put("tags", tags);

        final var record = new SourceRecord(null, null, "some_topic", schema, value);

        final var actual = filterByValueRegex.apply(record);
        assertEquals(record, actual, "The record contains the matching pattern");
    }

    @Test
    void shouldFilterOutMapFieldValue() {
        final FilterByValueRegex<SourceRecord> filterByValueRegex = new FilterByValueRegex<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("fieldName", "language");
        configs.put("pattern", ".*Java.*");
        configs.put("matches", "false");
        filterByValueRegex.configure(configs);

        final Map<String, Object> value = new HashMap<>();
        value.put("name", "John Doe");
        value.put("language", "Java");

        final var record = new SourceRecord(null, null, "some_topic", Schema.STRING_SCHEMA, value);

        final var actual = filterByValueRegex.apply(record);
        assertNull(actual, "The record should be filtered out");
    }

    @Test
    void shouldFilterArrayFieldValue() {
        final FilterByValueRegex<SourceRecord> filterByValueRegex = new FilterByValueRegex<>();
        final Map<String, String> configs = new HashMap<>();
        configs.put("fieldName", "tags");
        configs.put("pattern", ".*apple.*");
        configs.put("matches", "true");
        filterByValueRegex.configure(configs);

        // Test with a list
        final List<String> tagsList = Arrays.asList("apple", "orange", "mango");
        final var listRecord = new SourceRecord(null, null, "some_topic", null, tagsList);
        final var listActual = filterByValueRegex.apply(listRecord);
        assertNotNull(listActual, "The record should not be filtered out and return the record itself");

        // Test with an array
        final String[] tagsArray = {"apple", "orange", "mango"};
        final var arrayRecord = new SourceRecord(null, null, "some_topic", null, tagsArray);
        final var arrayActual = filterByValueRegex.apply(arrayRecord);
        assertNotNull(arrayActual, "The record should not be filtered out and return the record itself");
    }
}
