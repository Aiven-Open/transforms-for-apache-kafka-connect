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

package io.aiven.kafka.connect.debezium.converters.utils;

import java.util.OptionalInt;

import io.debezium.spi.converter.RelationalColumn;


public class MoneyTestRelationalColumn implements RelationalColumn {

    public boolean isOptional = false;

    @Override
    public int jdbcType() {
        return 0;
    }

    @Override
    public int nativeType() {
        return 0;
    }

    @Override
    public String typeName() {
        return "money";
    }

    @Override
    public String typeExpression() {
        return null;
    }

    @Override
    public OptionalInt length() {
        return null;
    }

    @Override
    public OptionalInt scale() {
        return null;
    }

    @Override
    public boolean isOptional() {
        return isOptional;
    }

    @Override
    public Object defaultValue() {
        return null;
    }

    @Override
    public boolean hasDefaultValue() {
        return false;
    }

    @Override
    public String name() {
        return null;
    }

    @Override
    public String dataCollection() {
        return null;
    }
}
