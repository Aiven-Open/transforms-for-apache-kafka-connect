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
import java.util.function.Predicate;

import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.transforms.Transformation;

public abstract class FilterByFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {

    private String fieldName;
    private Optional<String> fieldExpectedValue;
    private Optional<String> fieldValuePattern;

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define("field.name",
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "The field name to filter by."
                    + "Schema-based records (Avro), schemaless (e.g. JSON), and raw values are supported."
                    + "If empty, the whole key/value record will be filtered.")
            .define("field.value",
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "Expected value to match. Either define this, or a regex pattern")
            .define("field.value.pattern",
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                "The pattern to match. Either define this, or an expected value")
            .define("field.value.matches",
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                "The filter mode, 'true' for matching or 'false' for non-matching");
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(config(), configs);
        this.fieldName = config.getString("field.name");
        this.fieldExpectedValue = Optional.ofNullable(config.getString("field.value"));
        this.fieldValuePattern = Optional.ofNullable(config.getString("field.value.pattern"));
        final boolean expectedValuePresent = fieldExpectedValue.isPresent();
        final boolean regexPatternPresent = fieldValuePattern.map(s -> !s.isEmpty()).orElse(false);
        if (expectedValuePresent == regexPatternPresent) {
            throw new ConfigException(
                "Either field.value or field.value.pattern have to be set to apply filter transform");
        }
        final Predicate<SchemaAndValue> matchCondition;

        if (expectedValuePresent) {
            final SchemaAndValue expectedSchemaAndValue = Values.parseString(fieldExpectedValue.get());
            matchCondition = schemaAndValue -> expectedSchemaAndValue.value().equals(schemaAndValue.value());
        } else {
            final String pattern = fieldValuePattern.get();
            final Predicate<String> regexPredicate = Pattern.compile(pattern).asPredicate();
            matchCondition = schemaAndValue ->
                schemaAndValue != null
                    && regexPredicate.test(Values.convertToString(schemaAndValue.schema(), schemaAndValue.value()));
        }

        this.filterCondition = config.getBoolean("field.value.matches")
            ? matchCondition
            : (result -> !matchCondition.test(result));
    }
    private Predicate<SchemaAndValue> filterCondition;

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    @Override
    public R apply(final R record) {
        if (operatingValue(record) == null) {
            return record;
        }

        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(final R record) {
        final Struct struct = (Struct) operatingValue(record);
        final SchemaAndValue schemaAndValue = getStructFieldValue(struct, fieldName).orElse(null);
        return filterCondition.test(schemaAndValue) ? record : null;
    }

    private Optional<SchemaAndValue> getStructFieldValue(final Struct struct, final String fieldName) {
        final Schema schema = struct.schema();
        final Field field = schema.field(fieldName);
        final Object fieldValue = struct.get(field);
        if (fieldValue == null) {
            return Optional.empty();
        } else {
            return Optional.of(new SchemaAndValue(field.schema(), struct.get(field)));
        }
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(final R record) {
        if (fieldName == null || fieldName.isEmpty()) {
            final SchemaAndValue schemaAndValue = getSchemalessFieldValue(operatingValue(record)).orElse(null);
            return filterCondition.test(schemaAndValue) ? record : null;
        } else {
            final Map<String, Object> map = (Map<String, Object>) operatingValue(record);
            final SchemaAndValue schemaAndValue = getSchemalessFieldValue(map.get(fieldName)).orElse(null);
            return filterCondition.test(schemaAndValue) ? record : null;
        }
    }

    private Optional<SchemaAndValue> getSchemalessFieldValue(final Object fieldValue) {
        if (fieldValue == null) {
            return Optional.empty();
        }
        return Optional.of(new SchemaAndValue(Values.inferSchema(fieldValue), fieldValue));
    }

    @Override
    public void close() {
    }

    public static final class Key<R extends ConnectRecord<R>> extends FilterByFieldValue<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.key();
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends FilterByFieldValue<R> {

        @Override
        protected Schema operatingSchema(final R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(final R record) {
            return record.value();
        }
    }
}
