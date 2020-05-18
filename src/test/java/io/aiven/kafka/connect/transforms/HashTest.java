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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
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
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class HashTest {

    private static final String FIELD = "email";
    private static final String EMPTY_FIELD_VALUE = "";
    private static final String NON_EMPTY_FIELD_VALUE = "jerry@all_your_bases.com";
    private static final String DEFAULT_HASH_FUNCTION = HashConfig.HashFunction.SHA256.toString();

    @Test
    void noFieldName_NullValue_NoSkip() {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, false, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null: " + originalRecord,
            e.getMessage());
    }

    @Test
    void noFieldName_NullValue_Skip() {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, null);
        final Hash<SinkRecord> transform = transformation(null, true, DEFAULT_HASH_FUNCTION);
        final SinkRecord result = transform.apply(originalRecord);
        // No changes.
        assertEquals(originalRecord, result);
    }

    @Test
    void nullSchema() {
        final SinkRecord originalRecord = record(null, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace() + " schema can't be null: " + originalRecord, e.getMessage());
    }

    @Test
    void noFieldName_UnsupportedType() {
        final Schema schema = SchemaBuilder.struct().build();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(null, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace()
                        + " schema type must be STRING if field name is not specified: "
                        + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void noFieldName_NormalStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, NON_EMPTY_FIELD_VALUE);
        final Hash<SinkRecord> transform = transformation(null, false, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final String newValue = hash(hashFunction, NON_EMPTY_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void noFieldName_EmptyStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, EMPTY_FIELD_VALUE);
        final Hash<SinkRecord> transform = transformation(null, false, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final String newValue = hash(hashFunction, EMPTY_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    @Test
    void fieldName_NullValue_NoSkip() {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, null));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, false, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(FIELD + " in " + dataPlace() + " can't be null: " + originalRecord,
                e.getMessage());
    }

    @Test
    void fieldName_MissingValue_NoSkip() {
        final Schema schema = SchemaBuilder.struct()
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, false, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(FIELD + " in " + dataPlace() + " schema can't be missing: " + originalRecord,
                e.getMessage());
    }

    @Test
    void fieldName_NullValue_Skip() {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, null));
        final Hash<SinkRecord> transform = transformation(FIELD, true, DEFAULT_HASH_FUNCTION);
        final SinkRecord result = transform.apply(originalRecord);
        // No changes.
        assertEquals(originalRecord, result);
    }

    @Test
    void fieldName_MissingValue_Skip() {
        final Schema schema = SchemaBuilder.struct()
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final Hash<SinkRecord> transform = transformation(FIELD, true, DEFAULT_HASH_FUNCTION);
        final SinkRecord result = transform.apply(originalRecord);
        // No changes.
        assertEquals(originalRecord, result);
    }

    @Test
    void fieldName_NonStruct() {
        final SinkRecord originalRecord = record(SchemaBuilder.INT8_SCHEMA, "some");
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace() + " schema type must be STRUCT if field name is specified: "
                        + originalRecord,
                e.getMessage());
    }

    @Test
    void fieldName_NullStruct() {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, null);
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(dataPlace() + " can't be null if field name is specified: " + originalRecord,
                e.getMessage());
    }

    @Test
    void fieldName_UnsupportedTypeInField() {
        final Schema innerSchema = SchemaBuilder.struct().build();
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, innerSchema)
                .schema();
        final SinkRecord originalRecord = record(
                schema, new Struct(schema).put(FIELD, new Struct(innerSchema)));
        final Throwable e = assertThrows(DataException.class,
            () -> transformation(FIELD, true, DEFAULT_HASH_FUNCTION).apply(originalRecord));
        assertEquals(FIELD + " schema type in " + dataPlace() + " must be STRING: "
                        + originalRecord,
                e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void fieldName_NormalStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, NON_EMPTY_FIELD_VALUE));
        final Hash<SinkRecord> transform = transformation(FIELD, true, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final String newValue = hash(hashFunction, NON_EMPTY_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void fieldName_EmptyStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema).put(FIELD, EMPTY_FIELD_VALUE));
        final Hash<SinkRecord> transform = transformation(FIELD, true, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final String newValue = hash(hashFunction, EMPTY_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    private Hash<SinkRecord> transformation(
            final String fieldName,
            final boolean skipMissingOrNull,
            final String hashFunction) {
        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put("field.name", fieldName);
        }
        props.put("skip.missing.or.null", Boolean.toString(skipMissingOrNull));
        props.put("function", hashFunction);
        final Hash<SinkRecord> transform = createTransformationObject();
        transform.configure(props);
        return transform;
    }

    protected abstract String dataPlace();

    protected abstract Hash<SinkRecord> createTransformationObject();

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

    private String hash(final String function, final String value) {
        try {
            final MessageDigest md;
            switch (function) {
                case "md5":
                    md = MessageDigest.getInstance("MD5");
                    break;
                case "sha1":
                    md = MessageDigest.getInstance("SHA1");
                    break;
                case "sha256":
                    md = MessageDigest.getInstance("SHA-256");
                    break;
                default:
                    throw new IllegalArgumentException(function);
            }
            return Base64.getEncoder().encodeToString(md.digest(value.getBytes()));
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
