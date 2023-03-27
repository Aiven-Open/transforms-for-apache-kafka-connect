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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import static org.apache.kafka.connect.data.Schema.Type.STRING;

public class FilterByFieldValue<R extends ConnectRecord<R>> implements Transformation<R> {

    private String fieldName;
    private Optional<String> fieldExpectedValue;
    private Optional<String> fieldValuePattern;
    private Predicate<Optional<String>> filterCondition;

    @Override
    public R apply(final R record) {
        if (record.value() instanceof Struct) {
            return handleStruct(record);
        } else if (record.value() instanceof Map) {
            return handleMap(record);
        }
        return record; // if record is other than map or struct, pass-by
    }

    private R handleStruct(final R record) {
        Struct struct = (Struct) record.value();
        final Optional<String> fieldValue = extractStructFieldValue(struct, fieldName);
        return filterCondition.test(fieldValue) ? record : null;
    }

    private Optional<String> extractStructFieldValue(Struct struct, String fieldName) {
        final Schema schema = struct.schema();
        final Field field = schema.field(fieldName);
        final Object fieldValue = struct.get(field);

        Optional<String> text = Optional.empty();
        if (STRING.equals(field.schema().type())) {
            text = Optional.of((String) fieldValue);
        } else if (schema.type().isPrimitive()) {
            text = Optional.of(fieldValue.toString());
        }
        return text;
    }

    @SuppressWarnings("unchecked")
    private R handleMap(final R record) {
        final Map<String, Object> map = (Map<String, Object>) record.value();
        final Optional<String> fieldValue = extractMapFieldValue(map, fieldName);
        return filterCondition.test(fieldValue) ? record : null;
    }

    private Optional<String> extractMapFieldValue(Map<String, Object> map, String fieldName) {
        if (!map.containsKey(fieldName)) return Optional.empty();

        final Object fieldValue = map.get(fieldName);

        Optional<String> text = Optional.empty();
        if (fieldValue instanceof String
                || fieldValue instanceof Long
                || fieldValue instanceof Integer
                || fieldValue instanceof Short
                || fieldValue instanceof Double
                || fieldValue instanceof Float
                || fieldValue instanceof Boolean) {
            text = Optional.of(fieldValue.toString());
        }
        return text;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("field.name",
                        ConfigDef.Type.STRING,
                        ConfigDef.Importance.HIGH, "The field name to filter by")
                .define("field.value",
                        ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, "Expected value to match. Either define this, or a regex pattern")
                .define("field.value.pattern",
                        ConfigDef.Type.STRING, null,
                        ConfigDef.Importance.HIGH, "The pattern to match. Either define this, or an expected value")
                .define("field.value.matches",
                        ConfigDef.Type.BOOLEAN, true,
                        ConfigDef.Importance.MEDIUM, "The filter mode, 'true' for matching or 'false' for non-matching");
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(config(), configs);
        this.fieldName = config.getString("field.name");
        this.fieldExpectedValue = Optional.ofNullable(config.getString("field.value"));
        this.fieldValuePattern = Optional.ofNullable(config.getString("field.value.pattern"));
        boolean expectedValuePresent = fieldExpectedValue.map(s -> !s.isEmpty()).orElse(false);
        boolean regexPatternPresent = fieldValuePattern.map(s -> !s.isEmpty()).orElse(false);
        if ((expectedValuePresent && regexPatternPresent)
                || (!expectedValuePresent && !regexPatternPresent)) {
            throw new ConfigException("Either field.value or field.value.pattern have to be set to apply filter transform");
        }
        Predicate<Optional<String>> matchCondition = fieldValue -> fieldValue
                .filter(value -> expectedValuePresent ?
                        fieldExpectedValue.get().equals(value) :
                        value.matches(fieldValuePattern.get()))
                .isPresent();
        this.filterCondition = config.getBoolean("field.value.matches")
                ? (matchCondition)
                : (result -> !matchCondition.test(result));
    }
}
