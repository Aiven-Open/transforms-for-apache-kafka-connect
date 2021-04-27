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

package io.aiven.kafka.connect.debezium.converters;

import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

public class MoneyConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {
    /**
     * Convert money type to a correct format.
     * Source: https://github.com/moyphilip/MoneyConverter
     */
    private SchemaBuilder moneySchema;

    @Override
    public void configure(final Properties props) {
        moneySchema = SchemaBuilder.string().name(props.getProperty("schema.name"));
    }

    @Override
    public void converterFor(final RelationalColumn column,
                             final CustomConverter.ConverterRegistration<SchemaBuilder> registration) {

        if ("money".equals(column.typeName())) {
            registration.register(moneySchema, data -> {
                if (data == null) {
                    if (column.isOptional()) {
                        return null;
                    } else {
                        throw new IllegalArgumentException("Money column is not optional, but data is null");
                    }
                }

                return String.format("%.2f", ((Number) data).floatValue());
            });
        }
    }
}
