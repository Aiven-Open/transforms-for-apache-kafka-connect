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

import java.math.BigDecimal;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.aiven.kafka.connect.debezium.converters.utils.DummyRelationalColumn;
import io.aiven.kafka.connect.debezium.converters.utils.MoneyTestRelationalColumn;

import io.debezium.spi.converter.CustomConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(registration.currFieldSchema).isNull();
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        assertThat(registration.currFieldSchema.schema().name()).isEqualTo("price");
        assertThat(registration.currFieldSchema.schema().type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void shouldDoNothingIfColumnIsNotMoney() {
        transform.configure(prop);

        transform.converterFor(new DummyRelationalColumn(), registration);

        assertThat(registration.currFieldSchema).isNull();
        assertThat(registration.currConverter).isNull();
    }

    @Test
    void shouldFormatDataToMoneyFormat() {
        assertThat(registration.currConverter).isNull();
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        final String result = (String) registration.currConverter.convert(BigDecimal.valueOf(103.6999));
        assertThat(result).isEqualTo("103.70");

        final String result2 = (String) registration.currConverter.convert((long) 103);
        assertThat(result2).isEqualTo("103.00");
    }

    @Test
    void shouldFailIfDataIsNotBigDecimal() {
        assertThat(registration.currConverter).isNull();
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        assertThatThrownBy(() -> registration.currConverter.convert("103.6999"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Money type should have BigDecimal type");
    }

    @Test
    void shouldFailIfDataIsMissing() {
        assertThat(registration.currConverter).isNull();
        transform.converterFor(new MoneyTestRelationalColumn(), registration);

        assertThatThrownBy(() -> registration.currConverter.convert(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Money column is not optional, but data is null");
    }

    @Test
    void shouldDoNothingIfColumnIsOptional() {
        transform.configure(prop);
        final MoneyTestRelationalColumn moneyColumn = new MoneyTestRelationalColumn();
        moneyColumn.isOptional = true;

        transform.converterFor(moneyColumn, registration);

        final String result = (String) registration.currConverter.convert(null);
        assertThat(result).isNull();
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
