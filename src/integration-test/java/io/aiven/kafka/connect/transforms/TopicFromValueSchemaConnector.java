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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector needed for testing of ExtractTopicFromValueSchema.
 *
 * <p>It just produces a fixed number of struct records with value schema name set.
 */
public class TopicFromValueSchemaConnector extends SourceConnector {
    static final int MESSAGES_TO_PRODUCE = 10;

    private static final Logger log = LoggerFactory.getLogger(TopicFromValueSchemaConnector.class);
    static final String TOPIC = "topic-for-value-schema-connector-test";
    static final String FIELD = "field-0";

    static  final String NAME = "com.acme.schema.SchemaNameToTopic";

    @Override
    public void start(final Map<String, String> props) {
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TopicFromValueSchemaConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.singletonList(Collections.emptyMap());
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

    public static class TopicFromValueSchemaConnectorTask extends SourceTask {
        private int counter = 0;

        private final Schema valueSchema = SchemaBuilder.struct()
                .field(FIELD, SchemaBuilder.STRING_SCHEMA)
                .name(NAME)
                .schema();
        private final Struct value = new Struct(valueSchema).put(FIELD, "Data");

        @Override
        public void start(final Map<String, String> props) {
            log.info("Started TopicFromValueSchemaConnector!!!");
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
                            TOPIC,
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
