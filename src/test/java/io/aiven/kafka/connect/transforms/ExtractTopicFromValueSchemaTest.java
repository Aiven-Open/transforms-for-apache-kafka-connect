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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtractTopicFromValueSchemaTest {
    @Test
    void nullConfigsValueSchemaNameToTopicTest() {

        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic").schema();
        final SinkRecord originalRecord = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation(null).apply(originalRecord);
        assertEquals("com.acme.schema.SchemaNameToTopic", transformedRecord.topic());

    }

    @Test
    void emptyConfigsValueSchemaNameToTopicTest() {

        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic").schema();
        final SinkRecord originalRecord = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation(new HashMap<>()).apply(originalRecord);
        assertEquals("com.acme.schema.SchemaNameToTopic", transformedRecord.topic());

    }

    @Test
    void configMapValueSchemaNameToTopicTest() {
        final Map<String, String> configs = new HashMap<>();
        configs.put(ExtractTopicFromValueSchemaConfig.SCHEMA_NAME_TO_TOPIC,
                "com.acme.schema.SchemaNameToTopic1:TheNameToReplace1,"
                        + "com.acme.schema.SchemaNameToTopic2:TheNameToReplace2,"
                        + "com.acme.schema.SchemaNameToTopic3:TheNameToReplace3"
        );
        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic1").schema();
        final SinkRecord originalRecord = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation(configs).apply(originalRecord);
        assertEquals("TheNameToReplace1", transformedRecord.topic());

        final Schema valueSchema2 = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic3").schema();
        final SinkRecord originalRecord2 = record(keySchema, "key", valueSchema2, "{}");
        final SinkRecord transformedRecord2 = transformation(configs).apply(originalRecord2);
        assertEquals("TheNameToReplace3", transformedRecord2.topic());

    }

    @Test
    void regexConfigValueAfterLastDotToTopicTest() {
        final Map<String, String> configs = new HashMap<>();
        // pass regegx that will parse the class name after last dot
        configs.put(ExtractTopicFromValueSchemaConfig.REGEX_SCHEMA_NAME_TO_TOPIC,
                "(?:[.]|^)([^.]*)$");
        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic").schema();
        final SinkRecord originalRecord = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation(configs).apply(originalRecord);
        assertEquals("SchemaNameToTopic", transformedRecord.topic());

    }

    @Test
    void regexNoMatchToTopicTest() {
        final Map<String, String> configs = new HashMap<>();
        // pass regegx that will parse the class name after last dot
        configs.put(ExtractTopicFromValueSchemaConfig.REGEX_SCHEMA_NAME_TO_TOPIC,
                "/[^;]*/");
        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic").schema();
        final SinkRecord originalRecord = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation(configs).apply(originalRecord);
        assertEquals("com.acme.schema.SchemaNameToTopic", transformedRecord.topic());

    }

    private ExtractTopicFromValueSchema<SinkRecord> transformation(final Map<String, ?> configs) {
        final ExtractTopicFromValueSchema<SinkRecord> transform = createTransformationObject();
        if (configs != null) {
            transform.configure(configs);
        }
        return transform;
    }

    protected ExtractTopicFromValueSchema<SinkRecord> createTransformationObject() {
        return new ExtractTopicFromValueSchema.Name<>();
    }

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
