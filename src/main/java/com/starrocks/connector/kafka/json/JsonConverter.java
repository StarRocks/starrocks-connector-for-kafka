/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.starrocks.connector.kafka.json.Converters.*;

/**
 * Converts Kafka Connect data to JSON bytes. Operates with fixed settings:
 * schemas disabled, null replacement disabled, and NUMERIC decimal format.
 */
public class JsonConverter {
    private final HashMap<String, LogicalTypeConverter> converters = new HashMap<>();
    private final JsonNodeFactory jsonNodeFactory = JsonNodeFactory.instance;
    private final ObjectMapper jsonMapper = new JsonMapper()
            .setNodeFactory(jsonNodeFactory);

    @FunctionalInterface
    public interface LogicalTypeConverter {
        JsonNode toJson(Schema schema, Object value, JsonConverter converter);
    }

    public JsonConverter() {
        // Kafka logical types.
        converters.put(org.apache.kafka.connect.data.Decimal.LOGICAL_NAME, Converters.convertDecimal());
        converters.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, Converters.forDate());
        converters.put(org.apache.kafka.connect.data.Time.LOGICAL_NAME, Converters.forTime(MILLI));
        converters.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, Converters.forTimestamp(MILLI));

        // Debezium logical types.
        converters.put(io.debezium.time.Date.SCHEMA_NAME, Converters.forDate());

        converters.put(io.debezium.time.Time.SCHEMA_NAME, Converters.forTime(MILLI));
        converters.put(io.debezium.time.MicroTime.SCHEMA_NAME, Converters.forTime(MICRO));
        converters.put(io.debezium.time.NanoTime.SCHEMA_NAME, Converters.forTime(NANO));

        converters.put(io.debezium.time.Timestamp.SCHEMA_NAME, Converters.forTimestamp(MILLI));
        converters.put(io.debezium.time.MicroTimestamp.SCHEMA_NAME, Converters.forTimestamp(MICRO));
        converters.put(io.debezium.time.NanoTimestamp.SCHEMA_NAME, Converters.forTimestamp(NANO));

        // Zoned(Time|TimeStamp), Iso(Date|Time|Timestamp) keep as is string
    }

    public String fromConnectData(Schema schema, Object object) {
        if (schema == null && object == null) {
            return null;
        }
        try {
            var node = convertToJson(schema, object);
            return jsonMapper.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new DataException("Converting Kafka Connect data to String failed due to serialization error: ", e);
        }
    }

    public JsonNode convertToJson(Schema schema, Object object) {
        if (object == null) {
            if (schema == null)
                return null;
            if (schema.isOptional())
                return jsonNodeFactory.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            var logicalConverter = converters.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toJson(schema, object, this);
        }

        try {
            final var schemaType = schema == null ? ConnectSchema.schemaType(object.getClass()) : schema.type();
            if (schemaType == null)
                throw new DataException("Java class " + object.getClass() + " does not have corresponding schema type.");

            return switch (schemaType) {
                case INT8 -> jsonNodeFactory.numberNode((Byte) object);
                case INT16 -> jsonNodeFactory.numberNode((Short) object);
                case INT32 -> jsonNodeFactory.numberNode((Integer) object);
                case INT64 -> jsonNodeFactory.numberNode((Long) object);
                case FLOAT32 -> jsonNodeFactory.numberNode((Float) object);
                case FLOAT64 -> jsonNodeFactory.numberNode((Double) object);
                case BOOLEAN -> jsonNodeFactory.booleanNode((Boolean) object);
                case STRING -> jsonNodeFactory.textNode(((CharSequence) object).toString());
                case BYTES -> {
                    if (object instanceof byte[] bytes)
                        yield jsonNodeFactory.binaryNode(bytes);
                    else if (object instanceof ByteBuffer buf)
                        yield jsonNodeFactory.binaryNode(buf.array());
                    else
                        throw new DataException("Invalid type for bytes type: " + object.getClass());
                }
                case ARRAY -> {
                    var collection = (Collection<?>) object;
                    var list = jsonNodeFactory.arrayNode();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    collection.forEach(elem -> list.add(convertToJson(valueSchema, elem)));
                    yield list;
                }
                case MAP -> {
                    var map = (Map<?, ?>) object;
                    var objectMode = schema != null
                            ? schema.keySchema().type() == Schema.Type.STRING
                            : map.keySet().stream().allMatch(k -> k instanceof String);
                    var keySchema = schema == null ? null : schema.keySchema();
                    var valueSchema = schema == null ? null : schema.valueSchema();
                    if (objectMode) {
                        var obj = jsonNodeFactory.objectNode();
                        map.forEach((k, v) -> {
                            var key = convertToJson(keySchema, k).asText();
                            var value = convertToJson(valueSchema, v);
                            obj.set(key, value);
                        });
                        yield obj;
                    } else {
                        var list = jsonNodeFactory.arrayNode();
                        map.forEach((k, v) -> {
                            var key = convertToJson(keySchema, k);
                            var value = convertToJson(valueSchema, v);
                            var elem = jsonNodeFactory.arrayNode()
                                    .add(key)
                                    .add(value);
                            list.add(elem);
                        });
                        yield list;
                    }
                }
                case STRUCT -> {
                    if (!(object instanceof Struct struct) || !struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    var obj = jsonNodeFactory.objectNode();
                    schema.fields().forEach(field -> {
                        var value = convertToJson(field.schema(), struct.get(field));
                        obj.set(field.name(), value);
                    });
                    yield obj;
                }
            };
        } catch (ClassCastException e) {
            var schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + object.getClass());
        }
    }

    public JsonNodeFactory nodeFactory() {
        return jsonNodeFactory;
    }
}
