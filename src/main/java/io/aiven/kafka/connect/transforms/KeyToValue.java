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


import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @param <R> ConnectRecord
 */
public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyToValue.class);

    private KeyToValueConfig config;

    /**
     * A list of the new fields that will be added to the value schema, mapped to the key fields used to populate them.
     */
    private LinkedHashMap<String, String> valueFields;

    /**
     * Whether any keyFields are non-wildcard (that copies a field from the key).
     */
    private boolean keyFieldsHasNamedFields;

    /**
     * Whether any keyFields are a wildcard (that copies the entire key).
     */
    private boolean keyFieldsHasWildcard;


    private Cache<List<Object>, Schema> schemaCache;


    @Override
    public ConfigDef config() {
        return KeyToValueConfig.config();
    }

    @Override
    public void configure(final Map<String, ?> settings) {
        this.config = new KeyToValueConfig(settings);
        // Construct the mapping from the list in the config
        final List<String> keyFieldsList = config.keyFields();
        final List<String> valueFieldsList = config.valueFields();
        valueFields = new LinkedHashMap<>();

        for (int i = 0; i < keyFieldsList.size(); i++) {
            final String kf = keyFieldsList.get(i);
            final String vfIfPresent = i < valueFieldsList.size() ? valueFieldsList.get(i) : kf;
            final String vf = "*".equals(vfIfPresent) ? KeyToValueConfig.DEFAULT_WHOLE_KEY_FIELD : vfIfPresent;

            if (valueFields.containsKey(vf)) {
                throw new DataException(
                        String.format("More than one key value is copied to the value field name '%s'", vf));
            }
            valueFields.put(vf, kf);
        }

        keyFieldsHasNamedFields = valueFields.values().stream().anyMatch(kf -> !kf.equals("*"));
        keyFieldsHasWildcard = valueFields.containsValue("*");

        schemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    }


    /**
     * Validation check to ensure that if any named fields exist, they can be extracted from the structured data in the
     * key.
     *
     * @param record The incoming record to be transformed.
     * @throws DataException if the key does not support extracting named fields.
     */
    private void validateNoUnextractableKeyFields(final R record) {
        if (keyFieldsHasNamedFields && record.keySchema() != null && record.keySchema().type() != Schema.Type.STRUCT) {
            throw new DataException(
                    String.format("Named key fields %s cannot be copied from the key schema: %s", valueFields.values(),
                            record.keySchema().type()));
        }

        if (keyFieldsHasNamedFields && record.keySchema() == null
                && !(record.key() instanceof Map || record.key() instanceof Struct)) {
            throw new DataException(
                    String.format("Named key fields %s cannot be copied from the key class: %s", valueFields.values(),
                            record.key().getClass().getName()));
        }
    }

    /**
     * Validation check to fail quickly when the entire key is copied into a structured value with a schema, but has a
     * schemaless class.
     *
     * @param record The incoming record to be transformed.
     * @throws DataException if the value class requires the key to have a schema but the key is a schemaless class.
     */
    private void validateKeySchemaRequirementsMet(final R record) {
        if (keyFieldsHasWildcard && record.keySchema() == null && record.key() instanceof Map) {
            if (record.valueSchema() != null && record.valueSchema().type() == Schema.Type.STRUCT) {
                throw new DataException("The value requires a schema, but the key class is a schemaless Map");
            }
        }
    }

    /**
     * Validation check to ensure that the value can receive columns from the key, i.e. as either a Struct or Map.
     */
    private void validateStructuredValue(final R record) {
        if (record.valueSchema() == null && !(record.value() instanceof Map)
                || record.valueSchema() != null && record.valueSchema().type() != Schema.Type.STRUCT) {
            throw new DataException("The value needs to be a Struct or Map in order to append fields");
        }
    }

    @Override
    public R apply(final R record) {
        validateNoUnextractableKeyFields(record);
        validateKeySchemaRequirementsMet(record);
        validateStructuredValue(record);

        if (record.value() instanceof Struct) {
            if (record.keySchema() != null) {
                return applyToStruct(record, record.keySchema());
            } else {
                final Schema inferredSchema = Values.inferSchema(record.key());
                if (inferredSchema == null) {
                    throw new DataException(
                            "Cannot infer schema for unsupported key class: " + record.key().getClass().getName());
                }
                return applyToStruct(record, inferredSchema);
            }
        } else {
            return applyToMap(record);
        }
    }

    @Override
    public void close() {
        schemaCache = null;
    }

    /**
     * Merges the key and value schemas into a new schema, according to the configuration.
     *
     * @param record    The incoming record to be transformed.
     * @param keySchema The schema to be used for the incoming key. This may have been inferred from the key value.
     * @return The transformed record with the new schema.
     */
    private R applyToStruct(final R record, final Schema keySchema) {
        Schema newSchema;

        final List<Object> schemaKey = List.of(keySchema, record.valueSchema());
        newSchema = schemaCache.get(schemaKey);
        if (newSchema == null) {
            newSchema = mergeSchema(keySchema, record.valueSchema());
            LOGGER.debug("Merging into new schema {}", newSchema);
            schemaCache.put(schemaKey, newSchema);
        }

        final Struct value = (Struct) record.value();
        final Struct newValue = new Struct(newSchema);

        if (record.key() instanceof Struct) {
            final Struct key = (Struct) record.key();
            for (final Field f : newSchema.fields()) {
                final String kf = valueFields.get(f.name());
                if (kf == null) {
                    newValue.put(f.name(), value.get(f.name()));
                } else if (kf.equals("*")) {
                    newValue.put(f.name(), key);
                } else {
                    newValue.put(f.name(), key.get(kf));
                }
            }
        } else {
            for (final Field f : newSchema.fields()) {
                final String kf = valueFields.get(f.name());
                if (kf == null) {
                    newValue.put(f.name(), value.get(f.name()));
                } else if (kf.equals("*")) {
                    newValue.put(f.name(), record.key());
                }
            }
        }

        // Replace the value in the record
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                newValue.schema(), newValue, record.timestamp());
    }

    /**
     * Merges the key and value schemas into a new schema, according to the configuration of keyFields to copy into the
     * new value schema, and how they are renamed via valueFields.
     *
     * @param keySchema   The key schema.
     * @param valueSchema The original value schema.
     * @return The merged schema with any new types copied from the key schema.
     */
    private Schema mergeSchema(final Schema keySchema, final Schema valueSchema) {

        // Build a map of all the field names and schemas for the output, starting with the ones already present
        // in the value.
        final Map<String, Schema> updatedFieldSchemas = new LinkedHashMap<>();
        for (final Field vf : valueSchema.fields()) {
            updatedFieldSchemas.put(vf.name(), vf.schema());
        }

        // Add all the value fields that are going to be extracted by the key (overwriting any old ones).
        for (final Map.Entry<String, String> names : valueFields.entrySet()) {
            final String kf = names.getValue();
            final String vf = names.getKey();
            if (kf.equals("*")) {
                updatedFieldSchemas.put(vf, keySchema);
            } else if (keySchema.field(kf) == null) {
                throw new DataException(String.format("Key field '%s' not found in key schema", kf));
            } else {
                updatedFieldSchemas.put(vf, keySchema.field(kf).schema());
            }
        }

        // Create a copy of the output schema.
        final SchemaBuilder preVsb = SchemaBuilder.struct()
                .name(valueSchema.name())
                .version(valueSchema.version())
                .doc(valueSchema.doc())
                .parameters(valueSchema.parameters() != null ? valueSchema.parameters() : Collections.emptyMap());
        final SchemaBuilder vsb = valueSchema.isOptional() ? preVsb.optional() : preVsb;

        // Apply the fields retaining the order of the original value schema and valueFields configuration
        for (final Map.Entry<String, Schema> entry : updatedFieldSchemas.entrySet()) {
            vsb.field(entry.getKey(), entry.getValue());
        }

        return vsb.build();
    }

    @SuppressWarnings("unchecked")
    private R applyToMap(final R record) {

        final Map<String, Object> value = (Map<String, Object>) record.value();
        final Map<String, Object> newValue = new HashMap<>(value);

        if (record.key() instanceof Struct) {
            final Struct key = (Struct) record.key();
            for (final String vf : valueFields.keySet()) {
                final String kf = valueFields.get(vf);
                if (kf.equals("*")) {
                    newValue.put(vf, key);
                } else {
                    newValue.put(vf, key.get(kf));
                }
            }
        } else if (record.key() instanceof Map) {
            final Map<String, Object> key = (Map<String, Object>) record.key();
            for (final String vf : valueFields.keySet()) {
                final String kf = valueFields.get(vf);
                if (kf.equals("*")) {
                    newValue.put(vf, key);
                } else {
                    newValue.put(vf, key.get(kf));
                }
            }
        } else {
            for (final String vf : valueFields.keySet()) {
                final String kf = valueFields.get(vf);
                if (kf.equals("*")) {
                    newValue.put(vf, record.key());
                }
            }
        }

        // Replace the value in the record
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                null, newValue, record.timestamp());
    }
}

