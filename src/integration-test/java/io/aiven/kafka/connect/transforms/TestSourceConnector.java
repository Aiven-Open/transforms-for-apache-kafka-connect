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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/**
 * A connector needed for testing of ExtractTopic.
 *
 * <p>It just produces a fixed number of struct records.
 */
public class TestSourceConnector extends SourceConnector {
    static final int MESSAGES_TO_PRODUCE = 10;

    static final String ORIGINAL_TOPIC = "original-topic";
    static final String NEW_TOPIC = "new-topic";
    static final String ROUTING_FIELD = "field-0";

    @Override
    public void start(final Map<String, String> props) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestSourceConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.singletonList(Collections.EMPTY_MAP);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public String version() {
        return null;
    }

    public static class TestSourceConnectorTask extends SourceTask {
        private int counter = 0;

        private final Schema valueSchema = SchemaBuilder.struct()
            .field(ROUTING_FIELD, SchemaBuilder.STRING_SCHEMA)
            .schema();
        private final Struct value = new Struct(valueSchema).put(ROUTING_FIELD, NEW_TOPIC);

        @Override
        public void start(final Map<String, String> props) {
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
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
                    ORIGINAL_TOPIC,
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
