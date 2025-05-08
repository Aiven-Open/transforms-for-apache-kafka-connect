/*
 * Copyright 2025 Aiven Oy
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

public class TestKeyToValueConnector extends AbstractTestSourceConnector {

    static final long MESSAGES_TO_PRODUCE = 10L;

    static final String TARGET_TOPIC = "key-to-value-target-topic";

    @Override
    public Class<? extends Task> taskClass() {
        return TestKeyToValueConnector.TestSourceConnectorTask.class;
    }

    public static class TestSourceConnectorTask extends SourceTask {
        private int counter = 0;

        private final Schema keySchema = SchemaBuilder.struct().field("a1", SchemaBuilder.STRING_SCHEMA)
                .field("a2", SchemaBuilder.STRING_SCHEMA)
                .field("a3", SchemaBuilder.STRING_SCHEMA).schema();
        private final Struct key = new Struct(keySchema).put("a1", "a1").put("a2", "a2").put("a3", "a3");
        private final Schema valueSchema = SchemaBuilder.struct().field("b1", SchemaBuilder.STRING_SCHEMA)
                .field("b2", SchemaBuilder.STRING_SCHEMA)
                .field("b3", SchemaBuilder.STRING_SCHEMA).schema();
        private final Struct value = new Struct(valueSchema).put("b1", "b1").put("b2", "b2").put("b3", "b3");

        @Override
        public void start(final Map<String, String> props) {
        }

        @Override
        public List<SourceRecord> poll() {
            if (counter >= MESSAGES_TO_PRODUCE) {
                return null; // indicate pause
            }

            final Map<String, String> sourcePartition = new HashMap<>();
            sourcePartition.put("partition", "0");
            final Map<String, String> sourceOffset = new HashMap<>();
            sourceOffset.put("offset", Integer.toString(counter));

            counter += 1;

            return Collections.singletonList(
                    new SourceRecord(sourcePartition, sourceOffset,
                            TARGET_TOPIC,
                            keySchema, key,
                            valueSchema, value)
            );
        }

        @Override
        public void stop() {
        }

        @Override
        public String version() {
            return null;
        }
    }
}
