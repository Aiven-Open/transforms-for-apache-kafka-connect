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

import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class HashFunctionValidator implements ConfigDef.Validator {
    public static final HashFunctionValidator INSTANCE =
            new HashFunctionValidator();

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            if (!HashFieldConfig.HashFunction.stringValues.contains(value.toString())) {
                throw new ConfigException(name, value, "Invalid Hash Function");
            }
        }
    }

    @Override
    public String toString() {
        final String validFunctions = HashFieldConfig.HashFunction.stringValues
                .stream()
                .collect(Collectors.joining("|"));
        return "Must be one of " + validFunctions;
    }
}
