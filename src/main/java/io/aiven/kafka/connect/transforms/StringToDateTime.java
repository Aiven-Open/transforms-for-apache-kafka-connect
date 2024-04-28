package io.aiven.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class StringToDateTime<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StringToDateTime.class);

    public static final String FIELD_CONFIG = "field";
    private String field;
    @Override
    public R apply(R record) {
        if (record.valueSchema() == null || !(record.value() instanceof Struct)) {
            throw new DataException("This transformation requires records with a Struct value type");
        }

        Struct struct = (Struct) record.value();
        Schema schema = record.valueSchema();

        // Ensure the field exists and is of type String
        if (schema.field(field) == null || !schema.field(field).schema().type().equals(Schema.Type.STRING)) {
            throw new DataException("Field '" + field + "' not found in record or not of type String");
        }

        // Retrieve the date string from the record
        String dateString = struct.getString(field);

        // Parse the date string into LocalDateTime
        LocalDateTime dateTime = LocalDateTime.parse(dateString, DateTimeFormatter.ISO_LOCAL_DATE_TIME);

        // Create a new schema including the timestamp field (you may want to adjust the schema more depending on your needs)

        Schema updatedSchema = SchemaBuilder.struct()
//                .from(schema)
                .field(field, Timestamp.SCHEMA) // Update field schema to Timestamp
                .build();
//        SchemaBuilder updatedSchema = Schema.struct()
//                .from(schema)
//                .field(field, Timestamp.SCHEMA) // Update field schema to Timestamp
//                .build();

        // Create a new Struct with the updated schema and copy the fields from the original struct
        Struct updatedStruct = new Struct(updatedSchema);
        for (Field field : schema.fields()) {
            updatedStruct.put(field.name(), struct.get(field));
        }

        // Replace the string field with the LocalDateTime converted to java.util.Date (expected by Timestamp schema)
        updatedStruct.put(field, java.sql.Timestamp.valueOf(dateTime));

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedStruct, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define(FIELD_CONFIG, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "The field containing the DateTime string to convert");
    }

    @Override
    public void close() {
        // Nothing to close in this example
    }

    @Override
    public void configure(Map<String, ?> configs) {
        field = (String) configs.get(FIELD_CONFIG);
        if (field == null || field.isEmpty()) {
            throw new ConfigException("StringToDateTime requires a valid field name to convert");
        }
    }
}
