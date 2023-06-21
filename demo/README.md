# Demo

How to load plugins to Kafka Connect installations.

## Requirements

To run the demo, you need:
- Docker compose
- `curl`
- `jq`

## Demo

To install transforms into a Kafka Connect installation, it needs:

- Build `transforms-for-apache-kafka-connect` libraries
- Add libraries to Kafka Connect nodes
- Configure Kafka Connect `plugin.path`

### Build `transforms-for-apache-kafka-connect` libraries

To build libraries, use `gradlew installDist` command.
e.g. Dockerfile build:

```dockerfile
FROM eclipse-temurin:11-jdk AS base

ADD ./ transforms
WORKDIR transforms

RUN ./gradlew installDist
```

This generates the set of libraries to be installed in Kafka Connect workers.

### Add libraries to Kafka Connect nodes

Copy the directory with libraries into your Kafka Connect nodes.
e.g. add directory to Docker images:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.3.3

COPY --from=base /transforms/build/install/transforms-for-apache-kafka-connect /transforms
```

### Configure Kafka Connect `plugin.path`

On Kafka Connect configuration file, set `plugin.path` to indicate where to load plugins from,
e.g. with Docker compose:

```yaml
connect:
  # ...
  environment:
    # ...
    CONNECT_PLUGIN_PATH: /usr/share/java,/transforms # /transforms added on Dockerfile build
```

## Running

1. Build docker images: `make build` or `docker compose build`
2. Start environment: `make up` or `docker compose up -d`
3. Test connect plugins are loaded: `make test`

Sample response:
```json lines
{
  "class": "io.aiven.kafka.connect.transforms.ConcatFields$Key",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.ConcatFields$Value",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.ExtractTimestamp$Key",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.ExtractTimestamp$Value",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.ExtractTopic$Key",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.ExtractTopic$Value",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.FilterByFieldValue$Key",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.FilterByFieldValue$Value",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.Hash$Key",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.Hash$Value",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.MakeTombstone",
  "type": "transformation"
}
{
  "class": "io.aiven.kafka.connect.transforms.TombstoneHandler",
  "type": "transformation"
}
```
