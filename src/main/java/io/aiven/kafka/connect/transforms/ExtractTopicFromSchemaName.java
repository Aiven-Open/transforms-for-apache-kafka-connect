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

import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExtractTopicFromSchemaName<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ExtractTopicFromSchemaName.class);

    private Map<String, String> schemaNameToTopicMap;
    private Pattern pattern;

    @Override
    public ConfigDef config() {
        return ExtractTopicFromSchemaNameConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final ExtractTopicFromSchemaNameConfig config = new ExtractTopicFromSchemaNameConfig(configs);
        schemaNameToTopicMap = config.schemaNameToTopicMap();
        final Optional<String> regex = config.regEx();
        regex.ifPresent(s -> pattern = Pattern.compile(s));
    }

    public abstract String schemaName(R record);

    @Override
    public R apply(final R record) {

        final String schemaName = schemaName(record);
        // First check schema value name -> desired topic name mapping and use that if it is set.
        if (schemaNameToTopicMap.containsKey(schemaName)) {
            return createConnectRecord(record, schemaNameToTopicMap.get(schemaName));
        }
        // Secondly check if regex parsing from schema value name is set and use that.
        if (pattern != null) {
            final Matcher matcher = pattern.matcher(schemaName);
            if (matcher.find() && matcher.groupCount() == 1) {
                return createConnectRecord(record, matcher.group(1));
            }
            log.trace("No match with pattern {} from schema name {}", pattern.pattern(), schemaName);
        }
        // If no other configurations are set use value schema name as new topic name.
        return createConnectRecord(record, schemaName);
    }

    private R createConnectRecord(final R record, final String newTopicName) {
        return record.newRecord(
                newTopicName,
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
        );
    }

    @Override
    public void close() {
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractTopicFromSchemaName<R> {
        @Override
        public String schemaName(final R record) {
            if (null == record.valueSchema() || null == record.valueSchema().name()) {
                throw new DataException(" value schema name can't be null: " + record);
            }
            return record.valueSchema().name();
        }
    }
}
