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

import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class ExtractTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {

    private ExtractTimestampConfig config;

    @Override
    public ConfigDef config() {
        return ExtractTimestampConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new ExtractTimestampConfig(configs);
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
        
        if (schemaAndValue.value() == null) {
            throw new DataException(keyOrValue() + " can't be null: " + record);
        }

        final Object fieldValue;
        if (schemaAndValue.value() instanceof Struct) {
            final Struct struct = (Struct) schemaAndValue.value();
            if (struct.schema().field(config.fieldName()) == null) {
                throw new DataException(config.fieldName() + " field must be present and its value can't be null: "
                    + record);
            }
            fieldValue = struct.get(config.fieldName());
        } else if (schemaAndValue.value() instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) schemaAndValue.value();
            fieldValue = map.get(config.fieldName());
        } else {
            throw new DataException(keyOrValue() + " type must be STRUCT or MAP: " + record);
        }

        if (fieldValue == null) {
            throw new DataException(config.fieldName() + " field must be present and its value can't be null: "
                + record);
        }

        final long newTimestamp;
        if (fieldValue instanceof Long) {
            newTimestamp = (long) fieldValue;
        } else if (fieldValue instanceof Date) {
            newTimestamp = ((Date) fieldValue).getTime();
        } else {
            throw new DataException(config.fieldName()
                + " field must be INT64 or org.apache.kafka.connect.data.Timestamp: "
                + record);
        }

        return record.newRecord(
            record.topic(),
            record.kafkaPartition(),
            record.keySchema(),
            record.key(),
            record.valueSchema(),
            record.value(),
            newTimestamp
        );
    }

    @Override
    public void close() {
    }

    protected abstract String keyOrValue();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    public static final class Key<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected String keyOrValue() {
            return "key";
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected String keyOrValue() {
            return "value";
        }
    }
    
}
