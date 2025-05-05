/*
 * Copyright 2024 Aiven Oy
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


import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.util.HashMap;
import java.util.Map;


public class AvroToJsonConverter extends AbstractKafkaAvroDeserializer implements Converter {

    private static final Logger log = LoggerFactory.getLogger(AvroToJsonConverter.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private SchemaRegistryClient schemaRegistry;

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // Inicializar el cliente de Schema Registry usando la URL proporcionada
        final String schemaRegistryUrl = (String) configs.get("schema.registry.url");
        this.schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);

        // Configurar el deserializador AVRO con la configuración proporcionada
        super.configure(new KafkaAvroDeserializerConfig(new HashMap<>(configs)));
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        // Convertir los datos de Kafka Connect a un byte array en formato JSON
        try {
            final JsonNode jsonNode = objectMapper.valueToTree(value);
            return objectMapper.writeValueAsBytes(jsonNode);
        } catch (final Exception e) {
            throw new RuntimeException("Error converting Avro to JSON", e);
        }
    }

    /**
     * Deserializa un array de bytes en un objeto AVRO record
     * @param value Array de bytes a deserializar
     * @return Objeto AVRO record
     */
    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        // Deserializar el array de bytes en un AVRO record y convertirlo a JSON
        try {
            final GenericData.Record avroRecord = (GenericData.Record) deserialize(value);
            log.info("Contenido de avroRecord: {}", avroRecord.toString());

            return new SchemaAndValue(null, avroRecord.toString());
        } catch (final Exception e) {
            throw new RuntimeException("Error deserializing AVRO to JSON", e);
        }
    }
}
