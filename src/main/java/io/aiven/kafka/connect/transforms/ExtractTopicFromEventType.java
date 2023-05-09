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



public abstract class ExtractTopicFromEventType<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ExtractTopicFromEventType.class);

    @Override
    public ConfigDef config() {
        log.info("ConfigDef ExtractTopicFromEventType");
        return new ConfigDef();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        log.info("Configure ExtractTopicFromEventType");
    }

    @Override
    public R apply(final R record) {
        log.info("Try to change the topic to value schema name!");
        log.info("Record {} ", record);
        log.info("Record value schema : {}", record.valueSchema());
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
        log.info("ExtractTopicFromEventType Close");
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractTopicFromEventType<R> {

        @Override
        public void close() {
            log.info("ExtractTopicFromEventTyp$Value Close");
        }
    }
}
