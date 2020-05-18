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
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

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
            newValue = getNewValueWithoutFieldName(
                    record.toString(), schemaAndValue.schema(), schemaAndValue.value());
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
                log.debug(fieldName + " in " + dataPlace() + " schema is missing, skipping transformation");
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " schema can't be missing: " + recordStr);
            }
        }

        if (field.schema().type() != Schema.Type.STRING) {
            throw new DataException(fieldName + " schema type in " + dataPlace()
                    + " must be " + Schema.Type.STRING
                    + ": " + recordStr);
        }

        final Struct struct = (Struct) value;
        final String stringValue = struct.getString(fieldName);
        if (stringValue == null) {
            if (config.skipMissingOrNull()) {
                log.debug(fieldName + " in " + dataPlace() + " is null, skipping transformation");
                return Optional.empty();
            } else {
                throw new DataException(fieldName + " in " + dataPlace() + " can't be null: " + recordStr);
            }
        } else {
            return Optional.of(hashString(stringValue));
        }
    }

    private Optional<String> getNewValueWithoutFieldName(final String recordStr,
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
        final byte[] digest = messageDigest.digest(string.getBytes());
        return Base64.getEncoder().encodeToString(digest);
    }

    public static class Key<R extends ConnectRecord<R>> extends Hash<R> {
        @Override
        protected SchemaAndValue getSchemaAndValue(final R record) {
            return new SchemaAndValue(record.keySchema(), record.key());
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
        protected String dataPlace() {
            return "value";
        }
    }

    protected HashConfig getConfig() {
        return this.config;
    }
}
