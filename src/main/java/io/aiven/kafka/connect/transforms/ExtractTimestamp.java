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
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExtractTimestamp<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ExtractTimestamp.class);

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
        if (record.value() == null) {
            throw new DataException("Value can't be null: " + record);
        }

        final Object fieldValue;
        if (record.value() instanceof Struct) {
            final Struct struct = (Struct) record.value();
            if (struct.schema().field(config.fieldName()) == null) {
                throw new DataException(config.fieldName() + " field must be present and its value can't be null: "
                    + record);
            }
            fieldValue = struct.get(config.fieldName());
        } else if (record.value() instanceof Map) {
            final Map map = (Map) record.value();
            fieldValue = map.get(config.fieldName());
        } else {
            throw new DataException("Value type must be STRUCT or MAP: " + record);
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

    public static final class Value<R extends ConnectRecord<R>> extends ExtractTimestamp<R> {
        // There's an implementation only for value, not for key.
        // We provide $Value class anyway for the consistency sake
        // and in case we need a $Key version in the future as well.
    }

    @Override
    public void close() {
    }
}
