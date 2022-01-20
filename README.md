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

The transformation can use either the whole key or value (in this case, it must have `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT32`, `BOOLEAN`, or `STRING` type) or a field in them (in this case, it must have `STRUCT` type and the field's value must be `INT8`, `INT16`, `INT32`, `INT64`, `FLOAT32`, `FLOAT32`, `BOOLEAN`, or `STRING`).

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
transforms.ConcatFields.field.names=["test","foo","bar","age"]
transforms.ConcatFields.output.field.name="combined"
transforms.ConcatFields.delimiter="-"
transforms.ConcatFields.field.replace.missing="*"
```

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).

## Trademarks

Apache Kafka and Apache Kafka Connect are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
