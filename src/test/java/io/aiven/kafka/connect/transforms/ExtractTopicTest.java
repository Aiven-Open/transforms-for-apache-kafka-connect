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

import static io.aiven.kafka.connect.transforms.ExtractTopicConfig.APPEND_DELIMITER_CONFIG;
import static io.aiven.kafka.connect.transforms.ExtractTopicConfig.APPEND_TO_ORIGINAL_TOPIC_NAME_CONFIG;
import static io.aiven.kafka.connect.transforms.ExtractTopicConfig.FIELD_NAME_CONFIG;
import static io.aiven.kafka.connect.transforms.ExtractTopicConfig.SKIP_MISSING_OR_NULL_CONFIG;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

abstract class ExtractTopicTest {

    private static final String FIELD = "test_field";
    private static final String NEW_TOPIC = "new_topic";

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void nullSchema(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null, null);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_UnsupportedSchemaType(final boolean skipMissingOrNull) {
        final Schema schema = SchemaBuilder.struct().build();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        assertThatThrownBy(() -> transformation(null, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " schema type must be "
                + "[INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING] "
                + "if field name is not specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_UnsupportedValueType(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null, new HashMap<String, Object>());
        assertThatThrownBy(() -> transformation(null, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage("type in " + dataPlace() + " {} must be "
                + "[class java.lang.Byte, class java.lang.Short, class java.lang.Integer, "
                + "class java.lang.Long, class java.lang.Double, class java.lang.Float, "
                + "class java.lang.Boolean, class java.lang.String]: " + originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_NoSkip_WithSchema(final String value) {
        final Schema schema = STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        assertThatThrownBy(() -> transformation(null, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_NoSkip_Schemaless(final String value) {
        final SinkRecord originalRecord = record(null, value);
        assertThatThrownBy(() -> transformation(null, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_Skip_WithSchema(final String value) {
        final Schema schema = STRING_SCHEMA;
        final SinkRecord originalRecord = record(schema, value);
        final SinkRecord result = transformation(null, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void noFieldName_NullOrEmptyValue_Skip_Schemaless(final String value) {
        final SinkRecord originalRecord = record(null, value);
        final SinkRecord result = transformation(null, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalInt64Value(final boolean withSchema) {
        final Schema schema = withSchema ? Schema.INT64_SCHEMA : null;
        final SinkRecord originalRecord = record(schema, 123L);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "123"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalBooleanValue(final boolean withSchema) {
        final Schema schema = withSchema ? Schema.BOOLEAN_SCHEMA : null;
        final SinkRecord originalRecord = record(schema, false);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "false"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void noFieldName_NormalStringValue(final boolean withSchema) {
        final Schema schema = withSchema ? STRING_SCHEMA : null;
        final SinkRecord originalRecord = record(schema, NEW_TOPIC);
        final SinkRecord result = transformation(null, false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_WithSchema_NonStruct(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(Schema.INT8_SCHEMA, "some");
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " schema type must be STRUCT if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_Schemaless_NonMap(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null, "some");
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " type must be Map if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_WithSchema_NullStruct(final boolean skipMissingOrNull) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, STRING_SCHEMA)
            .schema();
        final SinkRecord originalRecord = record(schema, null);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_Schemaless_NullStruct(final boolean skipMissingOrNull) {
        final SinkRecord originalRecord = record(null, null);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(dataPlace() + " can't be null if field name is specified: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_WithSchema_UnsupportedSchemaTypeInField(final boolean skipMissingOrNull) {
        final Schema innerSchema = SchemaBuilder.struct().build();
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, innerSchema)
            .schema();
        final SinkRecord originalRecord = record(
            schema, new Struct(schema).put(FIELD, new Struct(innerSchema)));
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " schema type in " + dataPlace()
                + " must be [INT8, INT16, INT32, INT64, FLOAT32, FLOAT64, BOOLEAN, STRING]: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_Schemaless_UnsupportedSchemaTypeInField(final boolean skipMissingOrNull) {
        final var field = Map.of(FIELD, Map.of());
        final SinkRecord originalRecord = record(null, field);
        assertThatThrownBy(() -> transformation(FIELD, skipMissingOrNull).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " type in " + dataPlace()
                + " " + field + " must be "
                + "[class java.lang.Byte, class java.lang.Short, class java.lang.Integer, "
                + "class java.lang.Long, class java.lang.Double, class java.lang.Float, "
                + "class java.lang.Boolean, class java.lang.String]: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValue_NoSkip_WithSchema(final String value) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .schema();
        final Struct struct = new Struct(schema);
        if (!"missing".equals(value)) {
            struct.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(schema, struct);
        assertThatThrownBy(() -> transformation(FIELD, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " in " + dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValue_NoSkip_Schemaless(final String value) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("another", "value");
        if (!"missing".equals(value)) {
            valueMap.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(null, valueMap);
        assertThatThrownBy(() -> transformation(FIELD, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " in " + dataPlace() + " can't be null or empty: " + originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValueOrMissingField_Skip_WithSchema(final String value) {
        final Schema schema = SchemaBuilder.struct()
            .field(FIELD, Schema.OPTIONAL_STRING_SCHEMA)
            .schema();
        final Struct struct = new Struct(schema);
        if (!"missing".equals(value)) {
            struct.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(schema, struct);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @ValueSource(strings = "missing")
    @NullAndEmptySource
    void fieldName_NullOrEmptyValueOrMissingField_Skip_Schemaless(final String value) {
        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put("another", "value");
        if (!"missing".equals(value)) {
            valueMap.put(FIELD, value);
        }
        final SinkRecord originalRecord = record(null, valueMap);
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @Test
    void fieldName_MissingFieldInSchema_NoSkip() {
        final Schema schema = SchemaBuilder.struct().schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        assertThatThrownBy(() -> transformation(FIELD, false).apply(originalRecord))
            .isInstanceOf(DataException.class)
            .hasMessage(FIELD + " in " + dataPlace() + " schema can't be missing: " + originalRecord);
    }

    @Test
    void fieldName_MissingFieldInSchema_Skip() {
        final Schema schema = SchemaBuilder.struct().schema();
        final SinkRecord originalRecord = record(schema, new Struct(schema));
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(originalRecord);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalIntValue(final boolean withSchema) {
        final SinkRecord originalRecord;
        final var fieldValue = 123L;
        if (withSchema) {
            final Schema schema = SchemaBuilder.struct()
                .field(FIELD, Schema.INT64_SCHEMA)
                .schema();
            originalRecord = record(schema, new Struct(schema).put(FIELD, fieldValue));
        } else {
            final var value = Map.of(FIELD, fieldValue);
            originalRecord = record(null, value);
        }
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "123"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalBooleanValue(final boolean withSchema) {
        final SinkRecord originalRecord;
        final var fieldValue = false;
        if (withSchema) {
            final Schema schema = SchemaBuilder.struct()
                .field(FIELD, Schema.BOOLEAN_SCHEMA)
                .schema();
            originalRecord = record(schema, new Struct(schema).put(FIELD, fieldValue));
        } else {
            final var value = Map.of(FIELD, fieldValue);
            originalRecord = record(null, value);
        }
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "false"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void fieldName_NormalStringValue(final boolean withSchema) {
        final SinkRecord originalRecord;
        if (withSchema) {
            final Schema schema = SchemaBuilder.struct()
                .field(FIELD, STRING_SCHEMA)
                .schema();
            originalRecord = record(schema, new Struct(schema).put(FIELD, NEW_TOPIC));
        } else {
            final var value = Map.of(FIELD, NEW_TOPIC);
            originalRecord = record(null, value);
        }
        final SinkRecord result = transformation(FIELD, true).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    @Test
    void fieldName_Nested_Schemaless() {
        final Map<String, Object> valueMap = Map.of(
                "parent", Map.of(
                      "child", NEW_TOPIC
                )
        );

        final SinkRecord originalRecord = record(null, valueMap);
        final SinkRecord result = transformation("parent.child", false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    @Test
    void fieldName_Nested_Schema() {
        final Schema innerSchema = SchemaBuilder.struct()
                                                .field("child", STRING_SCHEMA)
                                                .build();
        final Schema schema = SchemaBuilder.struct()
                                           .field("parent", innerSchema)
                                           .schema();
        final SinkRecord originalRecord = record(
                schema, new Struct(schema).put("parent", new Struct(innerSchema).put("child", NEW_TOPIC)));

        final SinkRecord result = transformation("parent.child", false).apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, NEW_TOPIC));
    }

    @Test
    void append_Value() {
        final Map<String, String> props = new HashMap<>();
        props.put(FIELD_NAME_CONFIG, FIELD);
        props.put(APPEND_TO_ORIGINAL_TOPIC_NAME_CONFIG, Boolean.toString(true));
        props.put(APPEND_DELIMITER_CONFIG, "##");
        final ExtractTopic<SinkRecord> transform = createTransformationObject();
        transform.configure(props);

        final Map<String, Object> valueMap = new HashMap<>();
        valueMap.put(FIELD, "a");

        final SinkRecord originalRecord = setNewTopic(record(null, valueMap), "original");
        final SinkRecord result = transform.apply(originalRecord);
        assertThat(result).isEqualTo(setNewTopic(originalRecord, "original##a"));
    }

    private ExtractTopic<SinkRecord> transformation(final String fieldName, final boolean skipMissingOrNull) {
        final Map<String, String> props = new HashMap<>();
        if (fieldName != null) {
            props.put(FIELD_NAME_CONFIG, fieldName);
        }
        props.put(SKIP_MISSING_OR_NULL_CONFIG, Boolean.toString(skipMissingOrNull));
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
