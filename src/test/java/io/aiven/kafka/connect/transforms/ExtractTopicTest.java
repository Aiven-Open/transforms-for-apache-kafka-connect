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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class ExtractTopicTest {

    private static final String FIELD = "test_field";
    private static final String NEW_TOPIC = "new_topic";

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void nullSchema(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, skipMissingOrNull).apply(originalRecord));
        assertEquals(dataPlace() + " schema can't be null: " + originalRecord, e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void noFieldName_UnsupportedType(final boolean skipMissingOrNull) {
        final Schema schema = SchemaBuilder.struct().build();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, skipMissingOrNull).apply(originalRecord));
        assertEquals(dataPlace()
                + " schema type must be "
                + "[INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING]"
                + " if field name is not specified: "
                + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_NoSkip(final String value) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, false).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null or empty: " + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_Skip(final String value) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        final SinkRecord result = transformation(null, true).apply(originalRecord);
        assertEquals(originalRecord, result);
    }

    @Test
    void noFieldName_NormalInt64Value() {
        final Schema schema = SchemaBuilder.INT64_SCHEMA;
        final SinkRecord originalRecord = record(schema, 123L);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, "123"), result);
    }

    @Test
    void noFieldName_NormalBooleanValue() {
        final Schema schema = SchemaBuilder.BOOLEAN_SCHEMA;
        final SinkRecord originalRecord = record(schema, false);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, "false"), result);
    }

    @Test
    void noFieldName_NormalStringValue() {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, NEW_TOPIC);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, NEW_TOPIC), result);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void fieldName_NonStruct(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(SchemaBuilder.INT8_SCHEMA, "some");
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, skipMissingOrNull).apply(originalRecord));
        assertEquals(dataPlace() + " schema type must be STRUCT if field name is specified: "
                + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void fieldName_NullStruct(final boolean skipMissingOrNull) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.STRING_SCHEMA)
            .schema();
        final SinkRecord originalRecord = record(schema, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, skipMissingOrNull).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null if field name is specified: " + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void fieldName_UnsupportedTypeInField(final boolean skipMissingOrNull) {
        final Schema innerSchema = SchemaBuilder.struct().build();
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, innerSchema)
            .schema();
        final SinkRecord originalRecord = record(
            schema, new Struct(schema).put(FIELD, new Struct(innerSchema)));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, skipMissingOrNull).apply(originalRecord));
        assertEquals(FIELD + " schema type in " + dataPlace() + " must be "
            + "[INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING]"
            + ": " + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValue_NoSkip(final String value) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .schema();
        final Struct struct = new Struct(schema);
        if (!"missing".equals(value)) {
            struct.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(schema, struct);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, false).apply(originalRecord));
        assertEquals(FIELD + " in " + dataPlace() + " can't be null or empty: " + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValueOrMissingField_Skip(final String value) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .schema();
        final Struct struct = new Struct(schema);
        if (!"missing".equals(value)) {
            struct.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(schema, struct);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertEquals(originalRecord, result);
    }

    @Test
    void fieldName_MissingFieldInSchema_NoSkip() {
        final Schema schema = SchemaBuilder.struct().schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, false).apply(originalRecord));
        assertEquals(FIELD + " in " + dataPlace() + " schema can't be missing: " + originalRecord,
            e.getMessage());
    }

    @Test
    void fieldName_MissingFieldInSchema_Skip() {
        final Schema schema = SchemaBuilder.struct().schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertEquals(originalRecord, result);
    }

    @Test
    void fieldName_NormalIntValue() {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.INT64_SCHEMA)
            .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, 123L));
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, "123"), result);
    }

    @Test
    void fieldName_NormalBooleanValue() {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.BOOLEAN_SCHEMA)
            .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, false));
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, "false"), result);
    }

    @Test
    void fieldName_NormalStringValue() {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, SchemaBuilder.STRING_SCHEMA)
            .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, NEW_TOPIC));
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertEquals(setNewTopic(originalRecord, NEW_TOPIC), result);
    }

    private ExtractTopic<SinkRecord> transformation(final String fieldName, final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put("field.name", fieldName);
        }
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
        final ExtractTopic<SinkRecord> transform = createTransformationObject();
        transform.configure(props);
        return transform;
    }

    protected abstract String dataPlace();

    protected abstract ExtractTopic<SinkRecord> createTransformationObject();

    protected abstract SinkRecord record(final Schema schema, final Object data);

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

    private SinkRecord setNewTopic(final SinkRecord record, final String newTopic) {
        return record.newRecord(newTopic,
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            record.timestamp(),
            record.headers()
            );
    }
}
