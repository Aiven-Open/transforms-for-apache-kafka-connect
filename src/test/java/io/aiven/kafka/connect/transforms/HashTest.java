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
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

abstract class HashTest {

    private static final String FIELD = "email";
    private static final String EMPTY_FIELD_VALUE = "";
    private static final String NON_EMPTY_FIELD_VALUE = "jerry@big-corp.com";

    private static final Map<String, Map<String, String>> HASHED_VALUES = new HashMap<>();

    static {
        HASHED_VALUES.put("md5", new HashMap<>());
        // echo -n "" | md5sum -t
        HASHED_VALUES.get("md5").put(EMPTY_FIELD_VALUE, "d41d8cd98f00b204e9800998ecf8427e");
        // echo -n "jerry@big-corp.com" | md5sum -t
        HASHED_VALUES.get("md5").put(NON_EMPTY_FIELD_VALUE, "10e5756d5d4c9c1cadd5e1b952071378");

        HASHED_VALUES.put("sha1", new HashMap<>());
        // echo -n "" | sha1sum -t
        HASHED_VALUES.get("sha1").put(EMPTY_FIELD_VALUE, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
        // echo -n "jerry@big-corp.com" | sha1sum -t
        HASHED_VALUES.get("sha1").put(NON_EMPTY_FIELD_VALUE, "dd9ab6e93603bf618db0894a82da64f1623a94b6");

        HASHED_VALUES.put("sha256", new HashMap<>());
        // echo -n "" | sha256sum -t
        HASHED_VALUES.get("sha256").put(EMPTY_FIELD_VALUE,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        // echo -n "jerry@big-corp.com" | sha256sum -t
        HASHED_VALUES.get("sha256").put(NON_EMPTY_FIELD_VALUE,
                "20e85b05e7349963fc64746fbc7f3f4fdf31507921360847ebef333b229cf2d6");
    }

    private static final String DEFAULT_HASH_FUNCTION = HashConfig.HashFunction.SHA256.toString();
    private static final String UNAFFECTED_FIELD = "name";
    private static final String UNAFFECTED_FIELD_VALUE = "jerry";

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
                .field(UNAFFECTED_FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final Struct originalStruct = new Struct(schema)
                .put(FIELD, null)
                .put(UNAFFECTED_FIELD, UNAFFECTED_FIELD_VALUE);
        final SinkRecord originalRecord = record(schema, originalStruct);
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
                .field(UNAFFECTED_FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final Struct originalStruct = new Struct(schema)
                .put(FIELD, NON_EMPTY_FIELD_VALUE)
                .put(UNAFFECTED_FIELD, UNAFFECTED_FIELD_VALUE);
        final SinkRecord originalRecord = record(schema, originalStruct);
        final Hash<SinkRecord> transform = transformation(FIELD, true, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final Struct newValue = new Struct(schema)
                .put(FIELD, hash(hashFunction, NON_EMPTY_FIELD_VALUE))
                .put(UNAFFECTED_FIELD, UNAFFECTED_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void fieldName_EmptyStringValue(final String hashFunction) {
        final Schema schema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .field(UNAFFECTED_FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        final Struct originalStruct = new Struct(schema)
                .put(FIELD, EMPTY_FIELD_VALUE)
                .put(UNAFFECTED_FIELD, UNAFFECTED_FIELD_VALUE);
        final SinkRecord originalRecord = record(schema, originalStruct);
        final Hash<SinkRecord> transform = transformation(FIELD, true, hashFunction);
        final SinkRecord result = transform.apply(originalRecord);
        final Struct newValue = new Struct(schema)
                .put(FIELD, hash(hashFunction, EMPTY_FIELD_VALUE))
                .put(UNAFFECTED_FIELD, UNAFFECTED_FIELD_VALUE);
        assertEquals(setNewValue(originalRecord, newValue), result);
    }

    @ParameterizedTest
    @ValueSource(strings = {"md5", "sha1", "sha256"})
    void sameValueSameHash(final String hashFunction) {
        final Schema schema = SchemaBuilder.STRING_SCHEMA;
        final Hash<SinkRecord> transform = transformation(null, false, hashFunction);

        for (int i = 0; i < 10; i++) {
            final SinkRecord originalRecord = record(schema, NON_EMPTY_FIELD_VALUE);
            final SinkRecord result = transform.apply(originalRecord);
            final String newValue = hash(hashFunction, NON_EMPTY_FIELD_VALUE);
            assertEquals(setNewValue(originalRecord, newValue), result);
        }
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

    private SinkRecord setNewValue(final SinkRecord record, final Object newValue) {
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
        return HASHED_VALUES.get(function).get(value);
    }
}
