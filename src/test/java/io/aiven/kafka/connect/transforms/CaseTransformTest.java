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
import java.util.stream.Stream;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;

public class CaseTransformTest {

    private static final String ORIGINAL_UPPERCASE_FIELD_1 = "original_uppercase_1";
    private static final String ORIGINAL_UPPERCASE_FIELD_2 = "original_uppercase_2";
    private static final String ORIGINAL_LOWERCASE_1 = "original_lowercase_1";
    private static final String ORIGINAL_LOWERCASE_2 = "original_lowercase_2";
    private static final String FIELD_NOT_TRANSFORMED = "do_not_touch";

    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct()
            .field(ORIGINAL_UPPERCASE_FIELD_1, Schema.OPTIONAL_STRING_SCHEMA)
            .field(ORIGINAL_UPPERCASE_FIELD_2, Schema.OPTIONAL_STRING_SCHEMA)
            .field(ORIGINAL_LOWERCASE_1, Schema.OPTIONAL_STRING_SCHEMA)
            .field(ORIGINAL_LOWERCASE_2, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_NOT_TRANSFORMED, Schema.OPTIONAL_STRING_SCHEMA)
            .schema();

    static Stream<SinkRecord> recordProvider() {
        return Stream.of(
                record(
                        STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA)
                                .put(ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE")
                                .put(ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1")
                                .put(ORIGINAL_LOWERCASE_1, "lowercase")
                                .put(ORIGINAL_LOWERCASE_2, "CamelCase_2")
                                .put(FIELD_NOT_TRANSFORMED, "DoNotTouch"),
                        STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA)
                                .put(ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE")
                                .put(ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1")
                                .put(ORIGINAL_LOWERCASE_1, "lowercase")
                                .put(ORIGINAL_LOWERCASE_2, "CamelCase_2")
                                .put(FIELD_NOT_TRANSFORMED, "DoNotTouch")
                        ),
                record(
                        null, Map.of(
                                ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE",
                                ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1",
                                ORIGINAL_LOWERCASE_1, "lowercase",
                                ORIGINAL_LOWERCASE_2, "CamelCase_2",
                                FIELD_NOT_TRANSFORMED, "DoNotTouch"
                        ),
                        null, Map.of(
                                ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE",
                                ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1",
                                ORIGINAL_LOWERCASE_1, "lowercase",
                                ORIGINAL_LOWERCASE_2, "CamelCase_2",
                                FIELD_NOT_TRANSFORMED, "DoNotTouch"
                        )
                ),
                record(
                        STRUCT_SCHEMA, Map.of(
                                ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE",
                                ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1",
                                ORIGINAL_LOWERCASE_1, "lowercase",
                                ORIGINAL_LOWERCASE_2, "CamelCase_2",
                                FIELD_NOT_TRANSFORMED, "DoNotTouch"
                        ),
                        STRUCT_SCHEMA, Map.of(
                                ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE",
                                ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1",
                                ORIGINAL_LOWERCASE_1, "lowercase",
                                ORIGINAL_LOWERCASE_2, "CamelCase_2",
                                FIELD_NOT_TRANSFORMED, "DoNotTouch"
                        )
                )
        );
    }

    private void assertRecordFieldMatches(
            final SchemaAndValue schemaAndValue, final String fieldName, final String expected) {
        final String actual;
        if (schemaAndValue.value() instanceof Struct) {
            final Struct struct = (Struct) schemaAndValue.value();
            actual = struct.getString(fieldName);
        } else {
            final Map<?, ?> map = (Map<?, ?>) schemaAndValue.value();
            actual = map.get(fieldName).toString();
        }
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @MethodSource("recordProvider")
    void testLowercaseKeyTransformation(final SinkRecord originalRecord) {
        final SinkRecord transformedRecord = keyTransform(
                String.format(
                        "%s, %s",
                        ORIGINAL_UPPERCASE_FIELD_1,
                        ORIGINAL_UPPERCASE_FIELD_2
                ), "lower")
                .apply(originalRecord);
        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.keySchema(), transformedRecord.key());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "uppercase");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "camelcase_1");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_1, "lowercase");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_2, "CamelCase_2");
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    @ParameterizedTest
    @MethodSource("recordProvider")
    void testUppercaseKeyTransformation(final SinkRecord originalRecord) {
        final SinkRecord transformedRecord = keyTransform(
                String.format(
                        "%s, %s",
                        ORIGINAL_LOWERCASE_1,
                        ORIGINAL_LOWERCASE_2
                ), "upper")
                .apply(originalRecord);
        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.keySchema(), transformedRecord.key());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_1, "LOWERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_2, "CAMELCASE_2");
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    @ParameterizedTest
    @MethodSource("recordProvider")
    void testLowercaseValueTransformation(final SinkRecord originalRecord) {
        final SinkRecord transformedRecord = valueTransform(
                String.format(
                        "%s, %s",
                        ORIGINAL_UPPERCASE_FIELD_1,
                        ORIGINAL_UPPERCASE_FIELD_2
                ), "lower")
                .apply(originalRecord);
        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.valueSchema(), transformedRecord.value());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "uppercase");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "camelcase_1");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_1, "lowercase");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_2, "CamelCase_2");
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    @ParameterizedTest
    @MethodSource("recordProvider")
    void testUppercaseValueTransformation(final SinkRecord originalRecord) {
        final SinkRecord transformedRecord = valueTransform(
                String.format(
                        "%s, %s",
                        ORIGINAL_LOWERCASE_1,
                        ORIGINAL_LOWERCASE_2
                ), "upper")
                .apply(originalRecord);
        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.valueSchema(), transformedRecord.value());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_1, "LOWERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_LOWERCASE_2, "CAMELCASE_2");
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    @Test
    void testStructNullAndMissingFieldTransformation() {
        final SinkRecord originalRecord = record(
                STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA)
                        .put(ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE")
                        .put(ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1")
                        .put(ORIGINAL_LOWERCASE_1, "lowercase") // This is null in the value.
                        .put(ORIGINAL_LOWERCASE_2, "CamelCase_2") // This is missing in the value.
                        .put(FIELD_NOT_TRANSFORMED, "DoNotTouch"),
                STRUCT_SCHEMA, new Struct(STRUCT_SCHEMA)
                        .put(ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE")
                        .put(ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1")
                        .put(ORIGINAL_LOWERCASE_1, null)
                        .put(FIELD_NOT_TRANSFORMED, "DoNotTouch")
        );
        final SinkRecord transformedRecord = valueTransform(
                String.format("%s, %s", ORIGINAL_LOWERCASE_1, ORIGINAL_LOWERCASE_2), "upper")
                .apply(originalRecord);

        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.valueSchema(), transformedRecord.value());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1");
        assertThat(((Struct) schemaAndValue.value()).get(ORIGINAL_LOWERCASE_1)).isNull();
        assertThat(((Struct) schemaAndValue.value()).get(ORIGINAL_LOWERCASE_2)).isNull();
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    @Test
    void testMapNullAndMissingFieldTransformation() {
        final HashMap<String, String> valueMap = new HashMap<>();
        valueMap.put(ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE");
        valueMap.put(ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1");
        valueMap.put(ORIGINAL_LOWERCASE_1, null);
        valueMap.put(FIELD_NOT_TRANSFORMED, "DoNotTouch");

        final SinkRecord originalRecord = record(
                STRUCT_SCHEMA, Map.of(
                        ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE",
                        ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1",
                        ORIGINAL_LOWERCASE_1, "lowercase", //  This is null in the value
                        ORIGINAL_LOWERCASE_2, "CamelCase_2", // This is missing in the value
                        FIELD_NOT_TRANSFORMED, "DoNotTouch"
                ),
                STRUCT_SCHEMA, valueMap
        );
        final SinkRecord transformedRecord = valueTransform(
                String.format("%s, %s", ORIGINAL_LOWERCASE_1, ORIGINAL_LOWERCASE_2), "upper")
                .apply(originalRecord);

        final SchemaAndValue schemaAndValue =
                new SchemaAndValue(transformedRecord.valueSchema(), transformedRecord.value());
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_1, "UPPERCASE");
        assertRecordFieldMatches(schemaAndValue, ORIGINAL_UPPERCASE_FIELD_2, "CamelCase_1");
        assertThat(((Map<String, String>) schemaAndValue.value()).get(ORIGINAL_LOWERCASE_1)).isNull();
        assertThat(((Map<String, String>) schemaAndValue.value()).get(ORIGINAL_LOWERCASE_2)).isNull();
        assertRecordFieldMatches(schemaAndValue, FIELD_NOT_TRANSFORMED, "DoNotTouch");
    }

    private CaseTransform<SinkRecord> keyTransform(final String fieldNames, final String transformCase) {
        final Map<String, String> props = new HashMap<>();
        props.put("field.names", fieldNames);
        props.put("case", transformCase);
        final CaseTransform.Key<SinkRecord> caseTransform = new CaseTransform.Key<>();
        caseTransform.configure(props);
        return caseTransform;
    }

    private CaseTransform<SinkRecord> valueTransform(final String fieldNames, final String transformCase) {
        final Map<String, String> props = new HashMap<>();
        props.put("field.names", fieldNames);
        props.put("case", transformCase);
        final CaseTransform.Value<SinkRecord> caseTransform = new CaseTransform.Value<>();
        caseTransform.configure(props);
        return caseTransform;
    }

    private static SinkRecord record(final Schema keySchema,
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
