/*
 * Copyright 2021 Aiven Oy
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

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConcatFields<R extends ConnectRecord<R>> implements Transformation<R> {
    private ConcatFieldsConfig config;
    private static final Logger log = LoggerFactory.getLogger(ConcatFields.class);

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    protected abstract R createNewRecord(final R record, final Schema newSchema, final Object newValue);

    @Override
    public ConfigDef config() {
        return ConcatFieldsConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new ConcatFieldsConfig(configs);
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
        final SchemaBuilder newSchema = SchemaBuilder.struct();

        if (schemaAndValue.value() == null) {
            throw new DataException(dataPlace() + " Value can't be null: " + record);
        }

        final R newRecord;

        if (schemaAndValue.value() instanceof Struct) {
            final Struct struct = (Struct) schemaAndValue.value();
            final StringJoiner outputValue = new StringJoiner(config.delimiter());

            if (schemaAndValue.schema() != null) {
                schemaAndValue.schema().fields().forEach(field -> newSchema.field(field.name(), field.schema()));
            } else {
                struct.schema().fields().forEach(field -> newSchema.field(field.name(), field.schema()));
            }
            newSchema.field(config.outputFieldName(), Schema.OPTIONAL_STRING_SCHEMA);
            final Struct newStruct = new Struct(newSchema.build());
            struct.schema().fields().forEach(field -> {
                newStruct.put(field.name(), struct.get(field));
            });
            config.fieldNames().forEach(field -> {
                try {
                    if (struct.get(field) == null) {
                        outputValue.add(config.fieldReplaceMissing());
                    } else {
                        outputValue.add(struct.get(field).toString());
                    }
                } catch (final DataException e) {
                    log.debug("{} is missing, concat will use {}", field, config.fieldReplaceMissing());
                    outputValue.add(config.fieldReplaceMissing());
                }
            });
            newStruct.put(config.outputFieldName(), outputValue.toString());
            newRecord = createNewRecord(record, newSchema.build(), newStruct);
        } else if (schemaAndValue.value() instanceof Map) {
            final Map newValue = new HashMap<>((Map<?, ?>) schemaAndValue.value());
            final StringJoiner outputValue = new StringJoiner(config.delimiter());
            config.fieldNames().forEach(field -> {
                if (newValue.get(field) == null) {
                    outputValue.add(config.fieldReplaceMissing());
                } else {
                    outputValue.add(newValue.get(field).toString());
                }
            });
            newValue.put(config.outputFieldName(), outputValue.toString());

            //if we have a schema, we can add the new field to it, otherwise just keep the schema null
            if (schemaAndValue.schema() != null) {
                schemaAndValue.schema().fields().forEach(field -> newSchema.field(field.name(), field.schema()));
                newSchema.field(config.outputFieldName(), Schema.OPTIONAL_STRING_SCHEMA);
                newRecord = createNewRecord(record, newSchema.build(), newValue);
            } else {
                newRecord = createNewRecord(record, null, newValue);
            }
        } else {
            throw new DataException("Value type must be STRUCT or MAP: " + record);
        }

        return newRecord;
    }

    public static class Key<R extends ConnectRecord<R>> extends ConcatFields<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected R createNewRecord(final R record, final Schema newSchema, final Object newValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                newSchema,
                newValue,
                record.valueSchema(),
                record.value(),
                record.timestamp(),
                record.headers()
            );
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ConcatFields<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected R createNewRecord(final R record, final Schema newSchema, final Object newValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp(),
                record.headers()
            );
        }

        @Override
        protected String dataPlace() {
            return "value";
        }
    }

    @Override
    public void close() {
    }
}
