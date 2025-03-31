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

public class TestCaseTransformConnector extends AbstractTestSourceConnector {
    static final long MESSAGES_TO_PRODUCE = 10L;

    static final String SOURCE_TOPIC = "case-transform-source-topic";
    static final String TARGET_TOPIC = "case-transform-target-topic";
    static final String TRANSFORM_FIELD = "transform";

    @Override
    public Class<? extends Task> taskClass() {
        return TestCaseTransformConnector.TestSourceConnectorTask.class;
    }

    public static class TestSourceConnectorTask extends SourceTask {
        private int counter = 0;

        private final Schema valueSchema = SchemaBuilder.struct()
                .field(TRANSFORM_FIELD, SchemaBuilder.STRING_SCHEMA)
                .schema();
        private final Struct value =
                new Struct(valueSchema).put(TRANSFORM_FIELD, "lower-case-data-transforms-to-uppercase");

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
                            SOURCE_TOPIC,
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
