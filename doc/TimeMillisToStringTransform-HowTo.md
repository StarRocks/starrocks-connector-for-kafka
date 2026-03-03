# TimeMillisToStringTransform How-To Guide

## Background

When using Kafka Connect to ingest data from databases such as SQL Server, `TIME` columns are serialized as the Avro `time-millis` logical type — an `int` representing milliseconds since midnight. For example:

| Source (SQL Server) | Actual Kafka Value | Meaning |
|---|---|---|
| `18:02:37.0000000` | `64957000` | 18×3600000 + 2×60000 + 37×1000 |

The corresponding schema fragment looks like this:

```json
{
  "name": "TMCTDR",
  "type": ["null", {
    "connect.name": "org.apache.kafka.connect.data.Time",
    "logicalType": "time-millis",
    "type": "int"
  }]
}
```

`TimeMillisToStringTransform` is a Kafka Connect **SMT (Single Message Transform)** that converts these `time-millis` integer values into human-readable time strings such as `18:02:37` or `18:02:37.000`.

---

## Quick Start

### 1. Deploy the JAR

Build the project and copy the JAR containing the SMT into the Kafka Connect plugin directory:

```bash
mvn clean package -DskipTests
cp target/starrocks-connector-for-kafka-*-with-dependencies.jar /path/to/kafka-connect/plugins/
```

### 2. Configure the Connector

Add the transform configuration to your connector's JSON or properties file.

#### Minimal Configuration (Auto-Detect All time-millis Fields)

```json
{
  "name": "my-sink-connector",
  "config": {
    "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
    "topics": "my-topic",

    "transforms": "timeconv",
    "transforms.timeconv.type": "com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform",

    "...": "other connector configs"
  }
}
```

In this mode the SMT automatically scans the record schema and converts every field with the `org.apache.kafka.connect.data.Time` logical type from int to string.

#### Specifying Field Names

To convert only specific fields, use the `fields` parameter (comma-separated):

```json
{
  "transforms": "timeconv",
  "transforms.timeconv.type": "com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform",
  "transforms.timeconv.fields": "TMCTDR,TMLDDR"
}
```

#### Output with Millisecond Precision

The default output format is `HH:mm:ss` (e.g. `18:02:37`). To preserve millisecond precision, set the `format` parameter:

```json
{
  "transforms": "timeconv",
  "transforms.timeconv.type": "com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform",
  "transforms.timeconv.fields": "TMCTDR,TMLDDR",
  "transforms.timeconv.format": "HH:mm:ss.SSS"
}
```

Output example: `18:02:37.000`

---

## Configuration Reference

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fields` | String | `""` (empty) | Comma-separated list of field names to convert. When empty, all fields with the `time-millis` logical type are converted automatically. |
| `format` | String | `HH:mm:ss` | Output time format. Supported values: `HH:mm:ss` and `HH:mm:ss.SSS`. |

---

## Conversion Examples

Using actual CFPDR table data as an example:

### Before Transformation

```json
{
  "TMCTDR": { "int": 64957000 },
  "TMLDDR": { "int": 64957000 },
  "DTCTDR": { "int": 20067 },
  "name": { "string": "test" }
}
```

### After Transformation (format=HH:mm:ss)

```json
{
  "TMCTDR": "18:02:37",
  "TMLDDR": "18:02:37",
  "DTCTDR": { "int": 20067 },
  "name": { "string": "test" }
}
```

Note that `DTCTDR` uses the `date` logical type and is **not** affected. Only `time-millis` fields are converted.

### Conversion Reference Table

| Raw Value (ms) | HH:mm:ss | HH:mm:ss.SSS |
|---|---|---|
| `0` | `00:00:00` | `00:00:00.000` |
| `1000` | `00:00:01` | `00:00:01.000` |
| `43200000` | `12:00:00` | `12:00:00.000` |
| `64957000` | `18:02:37` | `18:02:37.000` |
| `64957123` | `18:02:37` | `18:02:37.123` |
| `86399999` | `23:59:59` | `23:59:59.999` |

---

## Full Connector Configuration Example

Below is a complete StarRocks Sink Connector configuration that includes the `TimeMillisToStringTransform`:

```json
{
  "name": "starrocks-sink-cfpdr",
  "config": {
    "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
    "topics": "SBDPDWHDBQ02.ACSCSTG.CFPDR",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "starrocks.http.url": "starrocks-fe:8030",
    "starrocks.database.name": "my_database",
    "starrocks.username": "root",
    "starrocks.password": "",

    "sink.properties.format": "json",
    "sink.properties.strip_outer_array": "true",

    "transforms": "timeconv",
    "transforms.timeconv.type": "com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform",
    "transforms.timeconv.fields": "TMCTDR,TMLDDR",
    "transforms.timeconv.format": "HH:mm:ss"
  }
}
```

---

## Chaining with Other Transforms

The SMT can be chained with other transforms. Transforms are executed in the order they are declared:

```json
{
  "transforms": "timeconv,addfield,unwrap",
  "transforms.timeconv.type": "com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform",
  "transforms.timeconv.fields": "TMCTDR,TMLDDR",
  "transforms.addfield.type": "com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

---

## Important Notes

1. **Schema requirement**: This SMT only processes Struct-type records that carry a schema. Schema-less records (e.g. plain JSON strings) are passed through unchanged.
2. **Field type change**: After transformation the target field's schema type changes from `int` (time-millis) to `string`. Make sure the corresponding column in the downstream system (e.g. StarRocks) uses a string-compatible type such as `VARCHAR`, `STRING`, or `CHAR`.
3. **Null handling**: Null field values remain null after transformation.
4. **Performance**: The SMT maintains an internal LRU cache for transformed schemas, so the same schema structure is never rebuilt twice.
