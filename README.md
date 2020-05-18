# Aiven Kafka Connect Transformations

[![Build Status](https://travis-ci.org/aiven/aiven-kafka-connect-transforms.svg?branch=master)](https://travis-ci.org/aiven/aiven-kafka-connect-transforms)

This is a set of [Kafka Connect transformations](https://kafka.apache.org/documentation/#connect_transforms).

## Transformations

See [the Kafka documentation](https://kafka.apache.org/documentation/#connect_transforms) for more details about configuring transformations. 

### `ExtractTimestamp`

This transformation replaces the original record's timestamp with a value taken from the the record.

The transformation:
- expects the record value to be either a `STRUCT` or a `MAP`;
- expects it to have a specified field;
- expects the value of the field to be either `INT64` or `org.apache.kafka.connect.data.Timestamp` and not be `null`.

Exists in one variant:
- `io.aiven.kafka.connect.transforms.ExtractTimestamp$Value` - works on values.

The transformation defines the following configurations:

- `field.name` - The name of the field which should be used as the new timestamp. Cannot be `null` or empty.

Here's an example of this transformation configuration:

```properties
transforms=ExtractTimestampFromValueField
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

## License

This project is licensed under the [Apache License, Version 2.0](LICENSE).
