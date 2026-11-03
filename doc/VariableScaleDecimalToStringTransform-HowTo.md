# VariableScaleDecimalToStringTransform How-To Guide

## Background

When Debezium captures a numeric column with no declared precision/scale — for example Postgres `numeric` with no size, or similarly unconstrained numeric types on other databases — it cannot use the standard Kafka Connect `org.apache.kafka.connect.data.Decimal` logical type, because that type fixes one scale for every value sharing its schema. Instead Debezium uses its own `io.debezium.data.VariableScaleDecimal` logical type: a `Struct` with two fields, `scale` (int32) and `value` (bytes, an unscaled `BigInteger`), so each record can carry its own scale.

The schema fragment looks like this:

```json
{
  "field": "savings_amount",
  "type": "struct",
  "name": "io.debezium.data.VariableScaleDecimal",
  "version": 1,
  "fields": [
    { "field": "scale", "type": "int32" },
    { "field": "value", "type": "bytes" }
  ]
}
```

Left unconverted, this connector's JSON serialization has no special handling for that struct shape, so it serializes it as a nested JSON object — `{"scale":2,"value":"AJw="}` — instead of a plain number. StarRocks cannot load that shape into a `DECIMAL` column; the value is silently dropped or coerced to `NULL` depending on `strict_mode`.

`VariableScaleDecimalToStringTransform` is a Kafka Connect **SMT (Single Message Transform)** that converts these fields into plain decimal strings (e.g. `"1234.5678"`) before the record reaches the sink, retaining full precision regardless of scale.

### Why a string, and why an SMT instead of a JsonConverter change

- **Why a string, not a fixed-scale Decimal**: different records sharing the same field can carry different scales — that is the whole point of "variable scale". A standard Connect `Decimal` schema fixes one scale for every value using it, so it isn't a good target. A string has no such limit, and StarRocks Stream Load already accepts string-formatted numbers for numeric columns.
- **Why an SMT, not a JsonConverter change**: this connector's `JsonConverter` implements the general Kafka Connect `Converter` contract (the `schemas.enable` envelope, round-tripping via `toConnectData`). `VariableScaleDecimal`'s schema is genuinely `STRUCT`-typed, so collapsing it to a scalar inside `JsonConverter` would make the emitted schema and payload disagree, and would break deserialization for anyone using `JsonConverter` as a normal `value.converter`. Running this transform first keeps that contract intact and only affects records flowing through this sink.

---

## Quick Start

### 1. Deploy the JAR

```bash
mvn clean package -DskipTests
cp target/starrocks-connector-for-kafka-*-with-dependencies.jar /path/to/kafka-connect/plugins/
```

### 2. Configure the Connector

#### Minimal Configuration (Auto-Detect All VariableScaleDecimal Fields)

```json
{
  "transforms": "vsdconv",
  "transforms.vsdconv.type": "com.starrocks.connector.kafka.transforms.VariableScaleDecimalToStringTransform"
}
```

In this mode the SMT scans the record schema and converts every field carrying the `io.debezium.data.VariableScaleDecimal` logical type.

#### Specifying Field Names

To restrict conversion to specific fields, use the `fields` parameter (comma-separated). A field must still carry the `VariableScaleDecimal` logical type to be converted — `fields` narrows *which* matching fields are converted, it does not force conversion of a same-named field of a different type (this matters on a connector consuming multiple topics, where a field name is not unique across schemas):

```json
{
  "transforms": "vsdconv",
  "transforms.vsdconv.type": "com.starrocks.connector.kafka.transforms.VariableScaleDecimalToStringTransform",
  "transforms.vsdconv.fields": "savings_amount,account_balance"
}
```

### 3. Placement relative to Debezium's unwrap — order does not matter

Unlike some SMTs, **this transform works correctly whether it runs before or after Debezium's `ExtractNewRecordState` ("unwrap")**:

- If the record is still a raw CDC envelope (`op`/`before`/`after`/`source`), the transform detects this and converts fields inside `before` and `after` directly — independently, since e.g. a delete event has a populated `before` and a `null` `after`.
- If the record has already been unwrapped to a flat row, the transform inspects its top-level fields directly.

It does not recurse into arbitrarily nested structs, arrays, or maps beyond that one documented envelope shape (`before`/`after`).

---

## Configuration Reference

| Parameter | Type | Default | Description |
|---|---|---|---|
| `fields` | String | `""` (empty) | Comma-separated list of field names to restrict conversion to. A field must still carry the `io.debezium.data.VariableScaleDecimal` logical type — a same-named field of a different type is left unchanged. When empty, all fields with that logical type are converted automatically. |

---

## Conversion Examples

| Raw value (scale, unscaled value) | Decimal | Converted string |
|---|---|---|
| scale=4, unscaled=12345678 | `1234.5678` | `"1234.5678"` |
| scale=6, unscaled=-987654321 | `-987.654321` | `"-987.654321"` |
| scale=2, unscaled=123500 | `1235.00` | `"1235.00"` (trailing zero scale preserved) |
| scale=9, unscaled=99999999999999999999123456789 | `99999999999999999999.123456789` | `"99999999999999999999.123456789"` (unconstrained precision) |
| — | `0` | `"0"` |
| — | `null` | `null` (nulls pass through unchanged) |

### Before Transformation (flat record, post-unwrap)

```json
{ "id": 1, "name": "test", "savings_amount": { "scale": 4, "value": "AwvOfg==" } }
```

### After Transformation

```json
{ "id": 1, "name": "test", "savings_amount": "1234.5678" }
```

### Envelope example (transform placed *before* unwrap)

For an update event, `before` and `after` are converted independently:

```json
{
  "op": "u",
  "before": { "id": 1, "name": "test", "savings_amount": "100.00" },
  "after":  { "id": 1, "name": "test", "savings_amount": "1235.00" },
  "source": { "...": "..." }
}
```

For a create event, `before` is `null` and only `after` is converted; for a delete event, `after` is `null` and only `before` is converted.

---

## Full Connector Configuration Example

```json
{
  "name": "starrocks-sink-accounts",
  "config": {
    "connector.class": "com.starrocks.connector.kafka.StarRocksSinkConnector",
    "topics": "dbserver1.public.accounts",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

    "starrocks.http.url": "starrocks-fe:8030",
    "starrocks.database.name": "my_database",
    "starrocks.username": "root",
    "starrocks.password": "",

    "sink.properties.format": "json",

    "transforms": "vsdconv,unwrap",
    "transforms.vsdconv.type": "com.starrocks.connector.kafka.transforms.VariableScaleDecimalToStringTransform",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite"
  }
}
```

Make sure the corresponding StarRocks column (`savings_amount` above) is a `DECIMAL` type wide enough for the actual values Debezium sends — a `VariableScaleDecimal` field can carry a different scale per row, so pick a precision/scale that comfortably covers the source column's range.

---

## Chaining with Other Transforms

The SMT can be chained with the other transforms in this repository. Transforms run in the order declared:

```json
{
  "transforms": "vsdconv,addfield,unwrap",
  "transforms.vsdconv.type": "com.starrocks.connector.kafka.transforms.VariableScaleDecimalToStringTransform",
  "transforms.addfield.type": "com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "true",
  "transforms.unwrap.delete.handling.mode": "rewrite"
}
```

Since order relative to `unwrap` does not matter for this SMT, `vsdconv` could equally run after `unwrap` instead.

---

## Important Notes

1. **Schema requirement**: like the other SMTs in this repository, this transform only processes `Struct`-type records that carry a schema. Schema-less records are passed through unchanged.
2. **Field type change**: after transformation the target field's schema type changes from `struct` (`VariableScaleDecimal`) to `string`. Make sure the corresponding column in StarRocks uses a `DECIMAL` type — StarRocks Stream Load accepts a string-formatted number for a numeric column.
3. **Type safety**: a field is only converted if it actually carries the `VariableScaleDecimal` logical type. Configuring `fields` restricts conversion to that set of names but never forces conversion of a same-named field of a different type — this matters on a connector consuming multiple topics, where field names are not unique across schemas.
4. **Null handling**: null field values, and a null `before`/`after` in an envelope (create/delete events), remain null after transformation.
5. **Special values**: for databases that can produce `NaN`/`+Infinity`/`-Infinity` for unconstrained numeric columns, the transform falls back to that value's text form (e.g. `"NaN"`) since these have no `BigDecimal` representation.
6. **Performance**: the SMT maintains internal LRU caches for transformed schemas (row-level and, when applicable, envelope-level), so the same schema structure is never rebuilt twice.
