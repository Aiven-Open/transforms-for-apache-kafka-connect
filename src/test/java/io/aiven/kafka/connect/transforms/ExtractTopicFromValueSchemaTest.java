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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExtractTopicFromValueSchemaTest {
    @Test
    void valueSchemaNameToTopicTest() {
        final Schema keySchema = SchemaBuilder.struct().keySchema();
        final Schema valueSchema = SchemaBuilder.struct().name("com.acme.schema.SchemaNameToTopic").schema();
        final SinkRecord originalRecord  = record(keySchema, "key", valueSchema, "{}");
        final SinkRecord transformedRecord = transformation().apply(originalRecord);
        assertEquals("com.acme.schema.SchemaNameToTopic", transformedRecord.topic());

    }

    private ExtractTopicFromValueSchema<SinkRecord> transformation() {
        final ExtractTopicFromValueSchema<SinkRecord> transform = createTransformationObject();
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
