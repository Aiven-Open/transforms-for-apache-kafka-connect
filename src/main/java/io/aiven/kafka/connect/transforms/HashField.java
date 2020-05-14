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

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class HashField<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(HashField.class);

    private HashFieldConfig config;
    private static final List<Schema.Type> SUPPORTED_TYPES_TO_CONVERT_FROM = Arrays.asList(
            Schema.Type.STRING
    );

    @Override
    public ConfigDef config() {
        return HashFieldConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new HashFieldConfig(configs);
    }

    @Override
    public R apply(final R record) {
        final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
        if (schemaAndValue.schema() == null) {
            throw new DataException(dataPlace() + " schema can't be null: " + record);
        }

        final Optional<String> newValue;
        if (config.fieldName().isPresent()) {
            newValue = getNewValueForNamedField(
                    record.toString(), schemaAndValue.schema(), schemaAndValue.value(), config.fieldName().get());
        } else {
            if (config.skipMissingOrNull()) {
                newValue = getNewValueWithoutFieldName(
                        record.toString(), schemaAndValue.schema(), schemaAndValue.value());
            } else {
                throw new DataException(dataPlace() + " can't be null or empty: " + record);
            }
        }

        if (newValue.isPresent()) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    newValue.get(),
                    record.timestamp(),
                    record.headers()
            );
        } else {
            return record;
        }
    }

    @Override
    public void close() {
    }

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    private Optional<String> getNewValueForNamedField(final String recordStr,
                                                      final Schema schema,
                                                      final Object value,
                                                      final String fieldName) {
        if (Schema.Type.STRUCT != schema.type()) {
            throw new DataException(dataPlace() + " schema type must be STRUCT if field name is specified: "
                    + recordStr);
        }

        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

        final Field field = schema.field(fieldName);
        if (field == null) {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " schema can't be missing: " + recordStr);
            }
        }

        if (!SUPPORTED_TYPES_TO_CONVERT_FROM.contains(field.schema().type())) {
            throw new DataException(fieldName + " schema type in " + dataPlace()
                    + " must be " + SUPPORTED_TYPES_TO_CONVERT_FROM
                    + ": " + recordStr);
        }

        final Struct struct = (Struct) value;

        final Optional<String> result = Optional.ofNullable(struct.get(fieldName))
                .map(Object::toString).map(s -> hashString(config.hashFunction(), s));

        if (result.isPresent() && !result.get().equals("")) {
            return result;
        } else {
            if (config.skipMissingOrNull()) {
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null or empty: " + recordStr);
            }
        }
    }

    static final String hashString(final MessageDigest md, final String input) {
        final byte[] digest = md.digest(input.getBytes());
        return Base64.getEncoder().encodeToString(digest);
    }

    private Optional<String> getNewValueWithoutFieldName(final String recordStr,
                                                         final Schema schema,
                                                         final Object value) {
        if (!SUPPORTED_TYPES_TO_CONVERT_FROM.contains(schema.type())) {
            throw new DataException(dataPlace() + " schema type must be "
                    + SUPPORTED_TYPES_TO_CONVERT_FROM
                    + " if field name is not specified: "
                    + recordStr);
        }

        final Optional<String> result = Optional.ofNullable(value)
                .map(Object::toString).map(s -> hashString(config.hashFunction(), s));

        if (result.isPresent() && !result.get().equals("")) {
            return result;
        } else {
            return Optional.empty();
        }
    }

    public static class Key<R extends ConnectRecord<R>> extends HashField<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends HashField<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        protected String dataPlace() {
            return "value";
        }
    }

    protected HashFieldConfig getConfig() {
        return this.config;
    }
}
