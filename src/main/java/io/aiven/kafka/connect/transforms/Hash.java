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
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Optional;

import io.aiven.kafka.connect.transforms.utils.CursorField;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import io.aiven.kafka.connect.transforms.utils.Hex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class Hash<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(Hash.class);

    private HashConfig config;
    private MessageDigest messageDigest;

    @Override
    public ConfigDef config() {
        return HashConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new HashConfig(configs);

        try {
            switch (config.hashFunction()) {
                case MD5:
                    messageDigest = MessageDigest.getInstance("MD5");
                    break;
                case SHA1:
                    messageDigest = MessageDigest.getInstance("SHA1");
                    break;
                case SHA256:
                    messageDigest = MessageDigest.getInstance("SHA-256");
                    break;
                default:
                    throw new ConnectException("Unknown hash function " + config.hashFunction());
            }
        } catch (final NoSuchAlgorithmException e) {
            throw new ConnectException(e);
        }
    }

    public final Optional<Object> getNewValue(final R record, final SchemaAndValue schemaAndValue) {
        final Optional<Object> newValue;
        if (config.field().isPresent()) {
            newValue = getNewValueForNamedField(
                record.toString(), schemaAndValue.schema(), schemaAndValue.value(), config.field().get());
        } else {
            newValue = getNewValueWithoutFieldName(
                record.toString(), schemaAndValue.schema(), schemaAndValue.value());
        }
        return newValue;
    }

    @Override
    public void close() {
    }

    protected abstract String dataPlace();

    protected abstract SchemaAndValue getSchemaAndValue(final R record);

    private Optional<Object> getNewValueForNamedField(final String recordStr,
                                                      final Schema schema,
                                                      final Object value,
                                                      final CursorField field) {
        if (Schema.Type.STRUCT != schema.type()) {
            throw new DataException(dataPlace() + " schema type must be STRUCT if field name is specified: "
                    + recordStr);
        }

        if (value == null) {
            throw new DataException(dataPlace() + " can't be null if field name is specified: " + recordStr);
        }

        final Field fieldSchema = field.read(schema);
        if (fieldSchema == null) {
            if (config.skipMissingOrNull()) {
                log.debug(field.getCursor() + " in " + dataPlace() + " schema is missing, skipping transformation");
                return Optional.empty();
            } else {
                throw new DataException(field.getCursor() + " in " + dataPlace() + " schema can't be missing: " + recordStr);
            }
        }

        if (fieldSchema.schema().type() != Schema.Type.STRING) {
            throw new DataException(field.getCursor() + " schema type in " + dataPlace()
                    + " must be " + Schema.Type.STRING
                    + ": " + recordStr);
        }

        final Struct struct = (Struct) value;
        final Optional<String> stringValue = field.readAsString(struct);
        if (stringValue.isEmpty()) {
            if (config.skipMissingOrNull()) {
                log.debug(field.getCursor() + " in " + dataPlace() + " is null, skipping transformation");
                return Optional.empty();
            } else {
                throw new DataException(field.getCursor() + " in " + dataPlace() + " can't be null: " + recordStr);
            }
        } else {
            final String updatedValue = hashString(stringValue.get());
            final Struct updatedRecord = struct.put(field.getCursor(), updatedValue);
            return Optional.ofNullable(updatedRecord);
        }
    }

    private Optional<Object> getNewValueWithoutFieldName(final String recordStr,
                                                         final Schema schema,
                                                         final Object value) {
        if (schema.type() != Schema.Type.STRING) {
            throw new DataException(dataPlace() + " schema type must be "
                    + Schema.Type.STRING
                    + " if field name is not specified: "
                    + recordStr);
        }

        if (value == null) {
            if (config.skipMissingOrNull()) {
                log.debug(dataPlace() + " is null, skipping transformation");
                return Optional.empty();
            } else {
                throw new DataException(dataPlace() + " can't be null: " + recordStr);
            }
        }

        return Optional.of(hashString(value.toString()));
    }

    private String hashString(final String string) {
        // We don't call reset() here because digest() does resetting afterwards.
        final byte[] digest = messageDigest.digest(string.getBytes());
        return Hex.encode(digest);
    }

    public static class Key<R extends ConnectRecord<R>> extends Hash<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
        }

        @Override
        public R apply(final R record) {
            final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
            if (schemaAndValue.schema() == null) {
                throw new DataException(dataPlace() + " schema can't be null: " + record);
            }

            final Optional<Object> newValue = getNewValue(record, schemaAndValue);

            if (newValue.isPresent()) {
                return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    newValue.get(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp(),
                    record.headers()
                );
            } else {
                return record;
            }
        }

        @Override
        protected String dataPlace() {
            return "key";
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends Hash<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.valueSchema(), record.value());
        }

        @Override
        public R apply(final R record) {
            final SchemaAndValue schemaAndValue = getSchemaAndValue(record);
            if (schemaAndValue.schema() == null) {
                throw new DataException(dataPlace() + " schema can't be null: " + record);
            }

            final Optional<Object> newValue = getNewValue(record, schemaAndValue);

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
        protected String dataPlace() {
            return "value";
        }
    }

    protected HashConfig getConfig() {
        return this.config;
    }
}
