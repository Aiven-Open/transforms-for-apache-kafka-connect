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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class HashFieldTest {

    private static final String FIELD = "email";
    private static final String FIELD_VALUE = "jerry@all_your_bases.com";
    private static final String DEFAULT_HASH_FUNCTION = HashFieldConfig.HashFunction.SHA256.toString();

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_NoSkip(final String value) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, false, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null or empty: " + originalRecord,
            e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void nullSchema(final String hashFunction) {
        final SinkRecord originalRecord = record(null, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, hashFunction).apply(originalRecord));
        assertEquals(dataPlace() + " schema can't be null: " + originalRecord, e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void noFieldName_UnsupportedType(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct().build();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, true, hashFunction).apply(originalRecord));
        assertEquals(dataPlace()
                        + " schema type must be "
                        + "[STRING]"
                        + " if field name is not specified: "
                        + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue(final String value) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        assertDoesNotThrow(
            () -> transformation(null, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void fieldName_NonStruct(final String hashFunction) {
        final SinkRecord originalRecord = record(SchemaBuilder.INT8_SCHEMA, "some");
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, hashFunction).apply(originalRecord));
        assertEquals(dataPlace() + " schema type must be STRUCT if field name is specified: "
                        + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void fieldName_NullStruct(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, hashFunction).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null if field name is specified: " + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void fieldName_UnsupportedTypeInField(final String hashFunction) {
        final Schema innerSchema = SchemaBuilder.struct().build();
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, innerSchema)
                .schema();
        final SinkRecord originalRecord = record(
                schema, new Struct(schema).put(FIELD, new Struct(innerSchema)));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, hashFunction).apply(originalRecord));
        assertEquals(FIELD + " schema type in " + dataPlace() + " must be "
                        + "[STRING]"
                        + ": " + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"MD5", "SHA-1", "SHA-256"})
    void fieldName_NormalStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, FIELD_VALUE));
        final HashField<SinkRecord> transform = transformation(FIELD, true, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final String newValue = HashField.hashString(transform.getConfig().hashFunction(), FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    private HashField<SinkRecord> transformation(
            final String fieldName,
            final boolean skipMissingOrNull,
            final String hashFunction) {
        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put(HashFieldConfig.FIELD_NAME_CONFIG, fieldName);
        }
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
        props.put(HashFieldConfig.FUNCTION_CONFIG, hashFunction);
        final HashField<SinkRecord> transform = createTransformationObject();
        transform.configure(props);
        return transform;
    }

    protected abstract String dataPlace();

    protected abstract HashField<SinkRecord> createTransformationObject();

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

    private SinkRecord setNewValue(final SinkRecord record, final String newValue) {
        return record.newRecord(record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                newValue,
                record.timestamp(),
                record.headers()
        );
    }
}
