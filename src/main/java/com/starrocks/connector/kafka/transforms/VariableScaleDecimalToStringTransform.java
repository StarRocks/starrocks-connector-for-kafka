/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka.transforms;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// Debezium represents numeric columns with no declared precision/scale (e.g. Postgres `numeric`
// with no size) using io.debezium.data.VariableScaleDecimal - a Struct{scale: int32, value: bytes} -
// rather than the fixed-scale org.apache.kafka.connect.data.Decimal logical type. Left as-is, the
// sink's JSON conversion serializes that struct as a nested JSON object instead of a scalar, which
// StarRocks cannot load into a DECIMAL column.
//
// This is deliberately a separate opt-in SMT rather than a change to JsonConverter itself: JsonConverter
// implements the general Kafka Connect Converter contract (schemas.enable envelope, round-tripping via
// toConnectData), and VariableScaleDecimal's schema is genuinely STRUCT-typed - collapsing it to a scalar
// there would make the emitted schema and payload disagree, and break deserialization for anyone using
// JsonConverter as a normal value.converter. Running this transform first keeps that contract intact and
// only applies the conversion to records flowing through this sink.
//
// A fixed-scale Decimal isn't a good target either: different records sharing the same field can carry
// different scales (that's what "variable scale" means), but a single Connect Decimal schema fixes one
// scale for every value using it. A string retains the exact value regardless of scale, and StarRocks
// Stream Load already accepts string-formatted numbers for numeric columns.
//
// Configuration example:
// transforms=vsdconv
// transforms.vsdconv.type=com.starrocks.connector.kafka.transforms.VariableScaleDecimalToStringTransform
public class VariableScaleDecimalToStringTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(VariableScaleDecimalToStringTransform.class);

    private static final String FIELDS_CONFIG = "fields";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Comma-separated list of field names to convert. "
                            + "If empty, all fields with the io.debezium.data.VariableScaleDecimal logical type are converted.");

    private Set<String> targetFields;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public R apply(R record) {
        if (record.value() == null || record.valueSchema() == null) {
            return record;
        }
        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Schema schema = record.valueSchema();
        Struct value = (Struct) record.value();

        if (!hasTargetFields(schema)) {
            return record;
        }

        Schema updatedSchema = getOrBuildSchema(schema);
        Struct updatedValue = buildUpdatedValue(schema, updatedSchema, value);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
        );
    }

    private boolean shouldConvertField(Field field) {
        if (!targetFields.isEmpty()) {
            return targetFields.contains(field.name());
        }
        return isVariableScaleDecimal(field.schema());
    }

    private boolean isVariableScaleDecimal(Schema schema) {
        return schema != null && VariableScaleDecimal.LOGICAL_NAME.equals(schema.name());
    }

    private boolean hasTargetFields(Schema schema) {
        for (Field field : schema.fields()) {
            if (shouldConvertField(field)) {
                return true;
            }
        }
        return false;
    }

    private Schema getOrBuildSchema(Schema originalSchema) {
        Schema cached = schemaUpdateCache.get(originalSchema);
        if (cached != null) {
            return cached;
        }

        SchemaBuilder builder = SchemaUtil.copySchemaBasics(originalSchema, SchemaBuilder.struct());
        for (Field field : originalSchema.fields()) {
            if (shouldConvertField(field)) {
                if (field.schema().isOptional()) {
                    builder.field(field.name(), Schema.OPTIONAL_STRING_SCHEMA);
                } else {
                    builder.field(field.name(), Schema.STRING_SCHEMA);
                }
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        Schema updatedSchema = builder.build();
        schemaUpdateCache.put(originalSchema, updatedSchema);
        return updatedSchema;
    }

    private Struct buildUpdatedValue(Schema originalSchema, Schema updatedSchema, Struct originalValue) {
        Struct updatedValue = new Struct(updatedSchema);

        for (Field field : originalSchema.fields()) {
            Object rawValue = originalValue.get(field);

            if (shouldConvertField(field)) {
                updatedValue.put(field.name(), rawValue == null ? null : toDecimalString(rawValue, field.name()));
            } else {
                updatedValue.put(field.name(), rawValue);
            }
        }

        return updatedValue;
    }

    String toDecimalString(Object rawValue, String fieldName) {
        if (!(rawValue instanceof Struct)) {
            throw new DataException("Field '" + fieldName + "' has VariableScaleDecimal logical type but value is not a Struct: "
                    + rawValue.getClass().getName());
        }
        SpecialValueDecimal specialValueDecimal = VariableScaleDecimal.toLogical((Struct) rawValue);
        Optional<BigDecimal> decimalValue = specialValueDecimal.getDecimalValue();
        // NaN / +Infinity / -Infinity (possible for e.g. Postgres numeric) have no BigDecimal form.
        return decimalValue.isPresent() ? decimalValue.get().toPlainString() : specialValueDecimal.toString();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Object fieldsObj = configs.get(FIELDS_CONFIG);
        String fieldsStr = fieldsObj != null ? fieldsObj.toString() : "";
        if (!fieldsStr.trim().isEmpty()) {
            targetFields = new HashSet<>(Arrays.asList(fieldsStr.split("\\s*,\\s*")));
        } else {
            targetFields = Collections.emptySet();
        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

        LOG.info("VariableScaleDecimalToStringTransform configured: fields={}",
                targetFields.isEmpty() ? "(auto-detect VariableScaleDecimal fields)" : targetFields);
    }
}
