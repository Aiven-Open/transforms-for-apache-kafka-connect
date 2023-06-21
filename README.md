# Aiven Transformations for Apache Kafka® Connect

A collection of [Single Message Transformations (SMTs)](https://kafka.apache.org/documentation/#connect_transforms) for Apache Kafka Connect.

## Transformations

See [the Kafka documentation](https://kafka.apache.org/documentation/#connect_transforms) for more details about configuring transformations. 

### `ExtractTimestamp`

This transformation replaces the original record's timestamp with a value taken from the the record.

The transformation:
- expects the record value to be either a `STRUCT` or a `MAP`;
- expects it to have a specified field;
- expects the value of the field to be either `INT64` or `org.apache.kafka.connect.data.Timestamp` and not be `null`.

Exists in two variants:
 - `io.aiven.kafka.connect.transforms.ExtractTimestamp$Key` - works on keys;
 - `io.aiven.kafka.connect.transforms.ExtractTimestamp$Value` - works on values.

The transformation defines the following configurations:
- `field.name` - The name of the field which should be used as the new timestamp. Cannot be `null` or empty.
- `timestamp.resolution` - The timestamp resolution for key or value   
   There are two possible values: 
    - `milliseconds` - key or value timestamp in milliseconds
    - `seconds` - key or value timestamp in seconds and will be converted in milliseconds,  
    the default is `milliseconds`.

Here's an example of this transformation configuration:

```properties
transforms=ExtractTimestampFromValueField
transforms.ExtractTimestampFromValueField.type=io.aiven.kafka.connect.transforms.ExtractTimestamp$Value
transforms.ExtractTimestampFromValueField.field.name=inner_field_name
```

### `ExtractTopic`

This transformation extracts a string value from the record and use it as the topic name.

The transformation can use either the whole key or value (in this case, it must have `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT32`, `BOOLEAN`, or `STRING` type; or related classes) or a field in them (in this case, it must have `STRUCT` type and the field's value must be `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT32`, `BOOLEAN`, or `STRING`; or related).

It supports fields with (e.g. Avro) or without schema (e.g. JSON).

Exists in two variants:
- `io.aiven.kafka.connect.transforms.ExtractTopic$Key` - works on keys;
- `io.aiven.kafka.connect.transforms.ExtractTopic$Value` - works on values.

The transformation defines the following configurations:

- `field.name` - The name of the field which should be used as the topic name. If `null` or empty, the entire key or value is used (and assumed to be a string). By default is `null`.
- `skip.missing.or.null` - In case the source of the new topic name is `null` or missing, should a record be silently passed without transformation. By default, is `false`.

Here is an example of this transformation configuration:

```properties
transforms=ExtractTopicFromValueField
transforms.ExtractTopicFromValueField.type=io.aiven.kafka.connect.transforms.ExtractTopic$Value
transforms.ExtractTopicFromValueField.field.name=inner_field_name
```

### `Hash`

This transformation replaces a string value with its hash.

The transformation can hash either the whole key or value (in this case, it must have `STRING` type) or a field in them (in this case, it must have `STRUCT` type and the field's value must be `STRING`).

Exists in two variants:

 - `io.aiven.kafka.connect.transforms.Hash$Key` - works on keys;
 - `io.aiven.kafka.connect.transforms.Hash$Value` - works on values.

The transformation defines the following configurations:

 - `field.name` - The name of the field which value should be hashed. If `null` or empty, the entire key or value is used (and assumed to be a string). By default, is `null`.
 - `function` - The name of the hash function to use. The supported values are: `md5`, `sha1`, and `sha256`.
 - `skip.missing.or.null` - In case the value to be hashed is `null` or missing, should a record be silently passed without transformation. By default, is `false`.

Here is an example of this transformation configuration:

```
transforms=HashEmail
transforms.HashEmail.type=io.aiven.kafka.connect.transforms.Hash$Value
transforms.HashEmail.field.name=email
transforms.HashEmail.function=sha1
```

### `TombstoneHandler`

This transformation manages tombstone records, 
i.e. records with the entire value field being `null`.

The transformation defines the following configurations:
- `behavior` - The action the transformation must perform when encounter a tombstone record. The supported values are:
  - `drop_silent` - silently drop tombstone records.
  - `drop_warn` - drop tombstone records and log at `WARN` level.
  - `fail` - fail with `DataException`.

Here is an example of this transformation configuration:
```properties
transforms=TombstoneHandler
transforms.TombstoneHandler.type=io.aiven.kafka.connect.transforms.TombstoneHandler
transforms.TombstoneHandler.behavior=drop_silent
```

### `ConcatFields`

This transformation adds a new field to the message with a key of type string and a value of string which is the
concatenation of the requested fields.

Exists in two variants:

 - `io.aiven.kafka.connect.transforms.ConcatFields$Key` - works on keys;
 - `io.aiven.kafka.connect.transforms.ConcatFields$Value` - works on values.

The transformation defines the following configurations:
 - `field.names` - A comma-separated list of fields to concatenate.
 - `output.field.name` - The name of field the concatenated value should be placed into.
 - `delimiter` - The string which should be used to join the extracted fields.
 - `field.replace.missing` - The string which should be used when a field is not found or its value is null.

Here is an example of this transformation configuration:

```
transforms=ConcatFields
transforms.ConcatFields.type=io.aiven.kafka.connect.transforms.ConcatFields$Value
transforms.ConcatFields.field.names=test,foo,bar,age
transforms.ConcatFields.output.field.name="combined"
transforms.ConcatFields.delimiter="-"
transforms.ConcatFields.field.replace.missing="*"
```

### `MakeTombstone`

This transformation converts a record into a tombstone by setting its value and value schema to `null`.

It can be used together with predicates, for example, to create a tombstone event from a delete event produced by a source connector.

Here is an example of this transformation configuration:
```properties
transforms=MakeTombstone
transforms.MakeTombstone.type=io.aiven.kafka.connect.transforms.MakeTombstone
```

### `FilterByFieldValue`

This transformation allows filtering records based either on a specific field or whole value and a matching expected value or regex pattern.

Here is an example of this transformation configuration:

```properties
transforms=Filter
transforms.Filter.type=io.aiven.kafka.connect.transforms.FilterByFieldValue
transforms.Filter.field.name=<field_name>
transforms.Filter.field.value=<field_value>
transforms.Filter.field.value.pattern=<regex_pattern>
transforms.Filter.field.value.matches=<true|false>
```

If `field.name` is empty, the whole value is considered for filtering.

Either `field.value` or `field.value.pattern` must be defined to apply filter.

Only, `string`, `numeric` and `boolean` types are considered for matching purposes, other types are ignored.

### `ExtractTopicFromSchemaName`

This transformation checks the  schema name and if it exists uses it as the topic name.

- `io.aiven.kafka.connect.transforms.ExtractTopicFromSchemaName$Value` - works on value schema name.

Currently this transformation only has implementation for record value schema name. Key schema name is not implemented.

By default (if schema.name.topic-map or the chema.name.regex is not set) transformation uses the content of the schema.name field.

The transformation defines the following optional configurations which can be used to tamper the schema.name:

- `schema.name.topic-map` - Map that contains the schema.name value and corresponding new topic name value that should be used instead. Format is "SchemaValue1:NewValue1,SchemaValue2:NewValue2" so key:value pairs as comma separated list.
- `schema.name.regex` - RegEx that should be used to parse the schema.name to desired value. For example for example `(?:[.]|^)([^.]*)$` which parses the name after last dot.

Here is an example of this transformation configuration (using :schema.name.topic-map)

```properties
transforms=ExtractTopicFromSchemaName
transforms.ExtractTopicFromSchemaName.type=io.aiven.kafka.connect.transforms.ExtractTopicFromSchemaName$Value
transforms.ExtractTopicFromSchemaName.schema.name.topic-map=com.acme.schema.SchemaNameToTopic1:TheNameToReplace1,com.acme.schema.SchemaNameToTopic2:TheNameToReplace2

```
And here is an example of this transformation configuration (using :schema.name.regex)
```properties
transforms=ExtractTopicFromValueSchema
transforms.ExtractTopicFromValueSchema.type=io.aiven.kafka.connect.transforms.ExtractTopicFromSchemaName$Value
transforms.ExtractTopicFromValueSchema.schema.name.regex=(?:[.]|^)([^.]*)$

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.

