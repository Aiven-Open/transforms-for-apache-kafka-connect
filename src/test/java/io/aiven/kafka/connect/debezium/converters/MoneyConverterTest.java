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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.aiven.kafka.connect.debezium.converters.utils.DummyRelationalColumn;
import io.aiven.kafka.connect.debezium.converters.utils.MoneyTestRelationalColumn;

import io.debezium.spi.converter.CustomConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MoneyConverterTest {

    private MoneyConverter transform;
    private StubConverterRegistration registration;
    private Properties prop;


    @BeforeEach
    void init() {
        transform = new MoneyConverter();
        registration = new StubConverterRegistration();
        prop = new Properties();
        prop.setProperty("schema.name", "price");
    }

    @AfterEach
    void teardown() {
        transform = null;
        registration = null;
        prop = null;
    }

    @Test
    void shouldRegisterCorrectSchema() {
        transform.configure(prop);
        assertNull(registration.currFieldSchema);
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        assertEquals(registration.currFieldSchema.schema().name(), "price");
        assertEquals(registration.currFieldSchema.schema().type(), Schema.Type.STRING);
    }

    @Test
    void shouldDoNothingIfColumnIsNotMoney() {
        transform.configure(prop);

        transform.converterFor(new DummyRelationalColumn(), registration);

        assertNull(registration.currFieldSchema);
        assertNull(registration.currConverter);
    }

    @Test
    void shouldFormatDataToMoneyFormat() {
        assertNull(registration.currConverter);
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        final String result = (String) registration.currConverter.convert((float) 103.6999);
        assertEquals(result, "103.70");
    }

    @Test
    void shouldReturnIfDataIsMissing() {
        assertNull(registration.currConverter);
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        final String result = (String) registration.currConverter.convert(null);
        assertEquals(result, "nu");
    }

    @Test
    void shouldDoNothingIfColumnIsOptional() {
        transform.configure(prop);
        final MoneyTestRelationalColumn moneyColumn = new MoneyTestRelationalColumn();
        moneyColumn.isOptional = true;

        transform.converterFor(moneyColumn, registration);

        final String result = (String) registration.currConverter.convert(null);
        assertNull(result);
    }

    class StubConverterRegistration implements CustomConverter.ConverterRegistration<SchemaBuilder> {
        SchemaBuilder currFieldSchema;
        CustomConverter.Converter currConverter;

        @Override
        public void register(final SchemaBuilder fieldSchema,
                             final CustomConverter.Converter converter) {
            currFieldSchema = fieldSchema;
            currConverter = converter;
        }
    }
}
