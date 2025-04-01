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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Transform field value case based on the configuration.
 * Supports maps and structs.
 * @param <R> ConnectRecord
 */
public abstract class CaseTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CaseTransform.class);

    /**
     * The configuration for case transform.
     */
    private CaseTransformConfig config;

    /**
     * Configured transform operation.
     */
    private UnaryOperator<String> caseTransformFunc;

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    protected abstract R createNewRecord(final R record, final Schema newSchema, final Object newValue);

    /**
     * Apply the case transformation to given new struct from the original struct field.
     * @param newStruct New struct
     * @param fieldName The field name to case transform
     */
    private void applyStruct(final Struct newStruct, final String fieldName) {
        try {
            final Object value = newStruct.get(fieldName);
            if (value == null) {
                newStruct.put(fieldName, null);
                return;
            }
            newStruct.put(fieldName, caseTransformFunc.apply(value.toString()));
        } catch (final DataException e) {
            LOGGER.debug("{} is missing, cannot transform the case", fieldName);
        }
    }

    /**
     * Apply the case transformation to given map from the map field.
     * @param newValue The mutable map
     * @param fieldName The field name to case transform
     */
    private void applyMap(final Map<String, Object> newValue, final String fieldName) {
        final Object value = newValue.get(fieldName);
        if (value == null) {
            newValue.put(fieldName, null);
            return;
        }
        newValue.put(fieldName, caseTransformFunc.apply(newValue.get(fieldName).toString()));
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);

        if (schemaAndValue.value() == null) {
            throw new DataException(dataPlace() + " Value can't be null: " + record);
        }

        final R newRecord;

        if (schemaAndValue.value() instanceof Struct) {
            final Struct struct = (Struct) schemaAndValue.value();
            final Struct newStruct = new Struct(struct.schema());
            struct.schema().fields().forEach(field -> {
                newStruct.put(field.name(), struct.get(field));
            });
            config.fieldNames().forEach(field -> {
                applyStruct(newStruct, field);
            });
            newRecord = createNewRecord(record, struct.schema(), newStruct);
        } else if (schemaAndValue.value() instanceof Map) {
            final Map<String, Object> newValue = new HashMap<>((Map<String, Object>) schemaAndValue.value());
            config.fieldNames().forEach(field -> {
                applyMap(newValue, field);
            });
            //if we have a schema, use it, otherwise leave null.
            if (schemaAndValue.schema() != null) {
                newRecord = createNewRecord(record, schemaAndValue.schema(), newValue);
            } else {
                newRecord = createNewRecord(record, null, newValue);
            }
        } else {
            throw new DataException("Value type must be STRUCT or MAP: " + record);
        }
        return newRecord;
    }


    @Override
    public void close() {
        // no-op
    }

    @Override
    public ConfigDef config() {
        return CaseTransformConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> settings) {
        this.config = new CaseTransformConfig(settings);

        switch (config.transformCase()) {
            case LOWER:
                caseTransformFunc = value -> value.toLowerCase(Locale.ROOT);
                break;
            case UPPER:
                caseTransformFunc = value -> value.toUpperCase(Locale.ROOT);
                break;
            default:
                throw new ConnectException("Unknown case transform function " + config.transformCase());
        }
    }

    /**
     * Record key transform implementation.
     * @param <R> ConnectRecord
     */
    public static class Key<R extends ConnectRecord<R>> extends CaseTransform<R> {
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

    /**
     * Record value transform implementation.
     * @param <R> ConnectRecord
     */
    public static class Value<R extends ConnectRecord<R>> extends CaseTransform<R> {
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
}
