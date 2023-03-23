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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class FilterByValueRegex<R extends ConnectRecord<R>> implements Transformation<R> {

    private String fieldName;
    private String pattern;
    private Pattern fieldValuePattern;
    private boolean matches;
    private Predicate<Boolean> recordFilterCondition;

    public FilterByValueRegex() {
        this.fieldName = "";
        this.pattern = "";
        this.fieldValuePattern = Pattern.compile("");
        this.matches = true;
    }

    @Override
    public R apply(final R record) {
        if (record.value() instanceof Struct) {
            return handleStruct(record);
        } else if (record.value() instanceof Map) {
            return handleMap(record);
        } else if (record.value() instanceof List || record.value().getClass().isArray()) {
            return handleListOrArray(record);
        }
        return null;
    }

    private R handleStruct(final R record) {
        final Struct value = (Struct) record.value();
        final Object fieldValueObj = value.get(fieldName);

        if (fieldValueObj instanceof List || fieldValueObj instanceof Object[]) {
            if (handleArrayOrListField(fieldValueObj)) {
                return record;
            }
        } else {
            return checkAndReturnRecord(value.schema().field(fieldName).schema(), fieldValueObj, record);
        }

        return null;
    }

    private R handleMap(final R record) {
        final Map<String, Object> value = (Map<String, Object>) record.value();
        final Object fieldValueObj = value.get(fieldName);

        return checkAndReturnRecord(null, fieldValueObj, record);
    }

    private R handleListOrArray(final R record) {
        if (handleArrayOrListField(record.value())) {
            return record;
        }
        return null;
    }

    private R checkAndReturnRecord(final Schema schema, final Object fieldValueObj, final R record) {
        if (fieldValueObj != null) {
            final String fieldValueStr = schema != null
                    ? convertToString(schema, fieldValueObj) : fieldValueObj.toString();

            if (fieldValueStr != null) {
                final Matcher matcher = fieldValuePattern.matcher(fieldValueStr);
                final boolean matches = matcher.matches();

                if (recordFilterCondition.test(matches)) {
                    return record;
                }
            }
        }
        return null;
    }

    private String convertToString(final Schema schema, final Object fieldValueObj) {
        if (schema.type() == Schema.Type.STRING) {
            return (String) fieldValueObj;
        } else if (schema.type().isPrimitive()) {
            return fieldValueObj.toString();
        }
        return null;
    }

    private boolean handleArrayOrListField(final Object fieldValueObj) {
        final List<?> valueList = fieldValueObj
                instanceof List ? (List<?>) fieldValueObj : Arrays.asList((Object[]) fieldValueObj);
        boolean foundMatchingFieldValue = false;

        for (final Object value : valueList) {
            final String fieldValueStr = value.toString();
            final Matcher matcher = fieldValuePattern.matcher(fieldValueStr);
            final boolean matches = matcher.matches();

            if (recordFilterCondition.test(matches)) {
                foundMatchingFieldValue = true;
                break;
            }
        }

        return foundMatchingFieldValue;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
         .define("fieldName",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH, "The field name to filter by")
         .define("pattern",
              ConfigDef.Type.STRING,
              ConfigDef.Importance.HIGH, "The pattern to match")
         .define("matches",
              ConfigDef.Type.BOOLEAN, true,
              ConfigDef.Importance.MEDIUM, "The filter mode, 'true' for matching or 'false' for non-matching");
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final AbstractConfig config = new AbstractConfig(config(), configs);
        this.fieldName = config.getString("fieldName");
        this.pattern = config.getString("pattern");
        this.fieldValuePattern = Pattern.compile(this.pattern);
        this.matches = config.getBoolean("matches");
        recordFilterCondition = this.matches
                ? (result -> result)
                : (result -> !result);
    }
}
