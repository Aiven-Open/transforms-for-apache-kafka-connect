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

import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public abstract class ExtractTopicFromValueSchema<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ExtractTopicFromValueSchema.class);

    @Override
    public ConfigDef config() {
        return new ConfigDef(); // no particular configs needed
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        log.info("ExtractTopicFromValueSchemaName Configure {}", configs);
    }

    @Override
    public R apply(final R record) {
        log.info("Try to change the topic to value schema name!");
        log.info("Record {} ", record);
        log.info("Record value schema : {}", record.valueSchema());
        log.info("Record value schema name : {}", record.valueSchema().name());
        log.info("Record value schema name : {}", record);
        log.info("Record class {}", record.value().getClass().getCanonicalName());

        final String newTopic = Optional.ofNullable(record.valueSchema().name()).orElse(record.topic());
        log.info("newTopic {} ", newTopic);

        final R connectRecord = record.newRecord(
                newTopic,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
        );
        return connectRecord;
    }

    @Override
    public void close() {
        log.info("ExtractTopicFromValueSchemaName Close");
    }

    public static class Name<R extends ConnectRecord<R>> extends ExtractTopicFromValueSchema<R> {

        @Override
        public void close() {
            log.info("ExtractTopicFromValueSchemaName Close");
        }
    }
}
