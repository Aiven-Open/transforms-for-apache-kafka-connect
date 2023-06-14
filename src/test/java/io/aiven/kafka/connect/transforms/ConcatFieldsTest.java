/*
 * Copyright 2021 Aiven Oy
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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.DELIMITER_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_NAMES_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.FIELD_REPLACE_MISSING_CONFIG;
import static io.aiven.kafka.connect.transforms.ConcatFieldsConfig.OUTPUT_FIELD_NAME_CONFIG;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

abstract class ConcatFieldsTest {
    private static final String FIELD = "combined";
    private static final String FIELD_NAMES = "test,foo,bar,age";
    private static final String TEST_FIELD = "test";
    private static final long TIMESTAMP = 11363151277L;
    private static final Date TEST_VALUE = new Date(TIMESTAMP);
    private static final String FOO_FIELD = "foo";
    private static final Boolean FOO_VALUE = false;
    private static final String BAR_FIELD = "bar";
    private static final String BAR_VALUE = "Baz";
    private static final String AGE_FIELD = "age";
    private static final Long AGE_VALUE = 100L;
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
        .field(BAR_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(TEST_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(AGE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
        .field(FOO_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
    private static final Schema NEW_VALUE_SCHEMA = SchemaBuilder.struct()
        .field(BAR_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(TEST_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .field(AGE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
        .field(FOO_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field(FIELD, Schema.OPTIONAL_STRING_SCHEMA)
        .build();

    @Test
    void recordNotStructOrMap() {
        final SinkRecord originalRecord = record(Schema.INT8_SCHEMA, (byte) 123);
        assertThatThrownBy(() -> transformation().apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("Value type must be STRUCT or MAP: " + originalRecord);
    }

    @Test
    void recordStructNull() {
        final Schema schema = SchemaBuilder.struct().schema();
        final SinkRecord originalRecord = record(schema, null);
        assertThatThrownBy(() -> transformation().apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " Value can't be null: " + originalRecord);
    }

    @Test
    void recordMapNull() {
        final SinkRecord originalRecord = record(null, null);
        assertThatThrownBy(() -> transformation().apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " Value can't be null: " + originalRecord);
    }

    @Test
    void structWithSchemaMissingField() {
        final Schema schema = SchemaBuilder.struct()
            .field(TEST_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FOO_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(BAR_FIELD, Schema.STRING_SCHEMA)
            .field(AGE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        assertThatThrownBy(() -> transformation().apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("Invalid value: null used for required field: \"bar\", schema type: STRING");
    }

    @Test
    void structWithMissingField() {
        final Schema schema = SchemaBuilder.struct()
            .field(TEST_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FOO_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field(BAR_FIELD, Schema.STRING_SCHEMA)
            .field(AGE_FIELD, Schema.OPTIONAL_INT64_SCHEMA)
            .build();
        final SinkRecord originalRecord = record(null, new Struct(schema));
        assertThatThrownBy(() -> transformation().apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("Invalid value: null used for required field: \"bar\", schema type: STRING");
    }

    @Test
    void mapWithMissingField() {
        final SinkRecord originalRecord = record(null, new HashMap<>());
        assertThatNoException()
            .describedAs(FIELD + " field must be present and its value can't be null: " + originalRecord)
            .isThrownBy(() -> transformation().apply(originalRecord));
    }

    @Test
    void mapWithoutSchema() {
        final Map<Object, Object> valueMap = new HashMap<>();
        valueMap.put(BAR_FIELD, BAR_VALUE);
        valueMap.put(TEST_FIELD, TEST_VALUE);
        valueMap.put(AGE_FIELD, AGE_VALUE);
        valueMap.put(FOO_FIELD, FOO_VALUE);
        final Map<Object, Object> newValueMap = new HashMap<>();
        newValueMap.put(BAR_FIELD, BAR_VALUE);
        newValueMap.put(FIELD, TEST_VALUE + "-" + FOO_VALUE + "-" + BAR_VALUE + "-" + AGE_VALUE);
        newValueMap.put(TEST_FIELD, TEST_VALUE);
        newValueMap.put(AGE_FIELD, AGE_VALUE);
        newValueMap.put(FOO_FIELD, FOO_VALUE);

        final SinkRecord originalRecord = record(null, valueMap);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, null, newValueMap);
        assertEquals(expectedRecord, transformedRecord);
    }

    @Test
    void mapWithoutSchemaMissingField() {
        final HashMap<Object, Object> valueMap = new HashMap<>();
        valueMap.put(TEST_FIELD, TEST_VALUE);
        valueMap.put(BAR_FIELD, BAR_VALUE);
        valueMap.put(AGE_FIELD, AGE_VALUE);
        final HashMap<Object, Object> newValueMap = new HashMap<>();
        newValueMap.put(TEST_FIELD, TEST_VALUE);
        newValueMap.put(BAR_FIELD, BAR_VALUE);
        newValueMap.put(AGE_FIELD, AGE_VALUE);
        newValueMap.put(FIELD, TEST_VALUE + "-*-" + "Baz" + "-" + AGE_VALUE);

        final SinkRecord originalRecord = record(null, valueMap);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, null, newValueMap);
        assertEquals(expectedRecord, transformedRecord);
    }

    @Test
    void mapWithSchema() {
        final HashMap<Object, Object> valueMap = new HashMap<>();
        valueMap.put(BAR_FIELD, BAR_VALUE);
        valueMap.put(TEST_FIELD, TEST_VALUE);
        valueMap.put(AGE_FIELD, AGE_VALUE);
        valueMap.put(FOO_FIELD, FOO_VALUE);
        final HashMap<Object, Object> newValueMap = new HashMap<>();
        newValueMap.put(BAR_FIELD, BAR_VALUE);
        newValueMap.put(FIELD, TEST_VALUE + "-" + FOO_VALUE + "-" + BAR_VALUE + "-" + AGE_VALUE);
        newValueMap.put(TEST_FIELD, TEST_VALUE);
        newValueMap.put(AGE_FIELD, AGE_VALUE);
        newValueMap.put(FOO_FIELD, FOO_VALUE);

        final SinkRecord originalRecord = record(VALUE_SCHEMA, valueMap);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, NEW_VALUE_SCHEMA, newValueMap);
        assertEquals(expectedRecord, transformedRecord);
    }

    @Test
    void mapWithSchemaMissingField() {
        final HashMap<Object, Object> valueMap = new HashMap<>();
        valueMap.put(TEST_FIELD, TEST_VALUE);
        valueMap.put(BAR_FIELD, BAR_VALUE);
        valueMap.put(AGE_FIELD, AGE_VALUE);
        final HashMap<Object, Object> newValueMap = new HashMap<>();
        newValueMap.put(TEST_FIELD, TEST_VALUE);
        newValueMap.put(BAR_FIELD, BAR_VALUE);
        newValueMap.put(AGE_FIELD, AGE_VALUE);
        newValueMap.put(FIELD, TEST_VALUE + "-*-" + "Baz" + "-" + AGE_VALUE);

        final SinkRecord originalRecord = record(VALUE_SCHEMA, valueMap);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, NEW_VALUE_SCHEMA, newValueMap);
        assertEquals(expectedRecord, transformedRecord);
    }

    @Test
    void structWithoutSchema() {
        final Struct valueStruct =
            new Struct(VALUE_SCHEMA).put(TEST_FIELD, TEST_VALUE.toString()).put(BAR_FIELD, BAR_VALUE)
                .put(AGE_FIELD, AGE_VALUE);
        final Struct newValueStruct =
            new Struct(NEW_VALUE_SCHEMA).put(TEST_FIELD, TEST_VALUE.toString()).put(BAR_FIELD, BAR_VALUE)
                .put(AGE_FIELD, AGE_VALUE)
                .put(FIELD, TEST_VALUE + "-*-" + "Baz" + "-" + AGE_VALUE);
        final SinkRecord originalRecord = record(null, valueStruct);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, NEW_VALUE_SCHEMA, newValueStruct);
        assertEquals(expectedRecord, transformedRecord);
    }

    @Test
    void structWithSchema() {
        final Struct valueStruct =
            new Struct(VALUE_SCHEMA).put(BAR_FIELD, BAR_VALUE).put(TEST_FIELD, TEST_VALUE.toString())
                .put(AGE_FIELD, AGE_VALUE);
        final Struct newValueStruct =
            new Struct(NEW_VALUE_SCHEMA).put(BAR_FIELD, BAR_VALUE)
                .put(TEST_FIELD, TEST_VALUE.toString())
                .put(AGE_FIELD, AGE_VALUE)
                .put(FIELD, TEST_VALUE + "-*-" + "Baz" + "-" + AGE_VALUE);
        final SinkRecord originalRecord = record(VALUE_SCHEMA, valueStruct);
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        final SinkRecord expectedRecord = setNewValue(originalRecord, NEW_VALUE_SCHEMA, newValueStruct);
        assertEquals(expectedRecord, transformedRecord);
    }

    private ConcatFields<SinkRecord> transformation() {
        final Map<String, String> props = new HashMap<>();
        props.put(FIELD_NAMES_CONFIG, FIELD_NAMES);
        props.put(OUTPUT_FIELD_NAME_CONFIG, FIELD);
        props.put(DELIMITER_CONFIG, "-");
        props.put(FIELD_REPLACE_MISSING_CONFIG, "*");
        final ConcatFields<SinkRecord> transform = createTransformationObject();
        transform.configure(props);
        return transform;
    }

    protected abstract String dataPlace();

    protected abstract ConcatFields<SinkRecord> createTransformationObject();

    protected abstract SinkRecord record(final Schema schema, final Object data);

    protected abstract SinkRecord setNewValue(final SinkRecord record,
                                              final Schema newValueSchema,
                                              final Object newValue);

    protected SinkRecord record(final Schema keySchema,
                                final Object key,
                                final Schema valueSchema,
                                final Object value) {
        return new SinkRecord("original_topic", 0,
            keySchema, key,
            valueSchema, value,
            123L,
            456L, TimestampType.CREATE_TIME);
    }
}
