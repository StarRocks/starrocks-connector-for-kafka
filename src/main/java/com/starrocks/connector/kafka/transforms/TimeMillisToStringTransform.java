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

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A Kafka Connect SMT that converts time-millis (int) fields to human-readable
 * time strings. The time-millis logical type stores time as milliseconds since
 * midnight (e.g. 64957000 -> "18:02:37.000").
 *
 * <p>Configuration example:
 * <pre>
 * transforms=timeconv
 * transforms.timeconv.type=com.starrocks.connector.kafka.transforms.TimeMillisToStringTransform
 * transforms.timeconv.fields=TMCTDR,TMLDDR
 * transforms.timeconv.format=HH:mm:ss
 * </pre>
 *
 * <p>If {@code fields} is empty, all fields with the {@code org.apache.kafka.connect.data.Time}
 * logical type are converted automatically.
 */
public class TimeMillisToStringTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(TimeMillisToStringTransform.class);

    private static final String FIELDS_CONFIG = "fields";
    private static final String FORMAT_CONFIG = "format";
    private static final String TIME_LOGICAL_NAME = "org.apache.kafka.connect.data.Time";

    private static final String FORMAT_HH_MM_SS = "HH:mm:ss";
    private static final String FORMAT_HH_MM_SS_SSS = "HH:mm:ss.SSS";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "Comma-separated list of field names to convert. "
                            + "If empty, all fields with time-millis logical type are converted.")
            .define(FORMAT_CONFIG,
                    ConfigDef.Type.STRING,
                    FORMAT_HH_MM_SS,
                    ConfigDef.Importance.LOW,
                    "Output time format: 'HH:mm:ss' or 'HH:mm:ss.SSS'. Default: HH:mm:ss");

    private Set<String> targetFields;
    private String format;
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

        if (!hasTimeMillisFields(schema)) {
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
        return isTimeMillisSchema(field.schema());
    }

    private boolean isTimeMillisSchema(Schema schema) {
        if (schema == null) {
            return false;
        }
        if (schema.type() == Schema.Type.STRUCT || schema.type() == Schema.Type.ARRAY
                || schema.type() == Schema.Type.MAP) {
            return false;
        }
        return TIME_LOGICAL_NAME.equals(schema.name());
    }

    private boolean hasTimeMillisFields(Schema schema) {
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
                    builder.field(field.name(), SchemaBuilder.string().optional().build());
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
                if (rawValue == null) {
                    updatedValue.put(field.name(), null);
                } else {
                    int millis;
                    if (rawValue instanceof Integer) {
                        millis = (Integer) rawValue;
                    } else if (rawValue instanceof Long) {
                        millis = ((Long) rawValue).intValue();
                    } else if (rawValue instanceof java.util.Date) {
                        millis = (int) (((java.util.Date) rawValue).getTime() % 86400000L);
                    } else {
                        LOG.warn("Field '{}' has unexpected type {}, passing through as string",
                                field.name(), rawValue.getClass().getName());
                        updatedValue.put(field.name(), rawValue.toString());
                        continue;
                    }
                    updatedValue.put(field.name(), formatMillisSinceMidnight(millis));
                }
            } else {
                updatedValue.put(field.name(), rawValue);
            }
        }

        return updatedValue;
    }

    String formatMillisSinceMidnight(int millis) {
        boolean negative = millis < 0;
        if (negative) {
            millis = -millis;
        }

        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis) % 60;
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis) % 60;
        long ms = millis % 1000;

        String prefix = negative ? "-" : "";

        if (FORMAT_HH_MM_SS_SSS.equals(format)) {
            return String.format("%s%02d:%02d:%02d.%03d", prefix, hours, minutes, seconds, ms);
        }
        return String.format("%s%02d:%02d:%02d", prefix, hours, minutes, seconds);
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

        Object formatObj = configs.get(FORMAT_CONFIG);
        format = formatObj != null ? formatObj.toString() : FORMAT_HH_MM_SS;
        if (format.trim().isEmpty()) {
            format = FORMAT_HH_MM_SS;
        }

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

        LOG.info("TimeMillisToStringTransform configured: fields={}, format={}",
                targetFields.isEmpty() ? "(auto-detect time-millis)" : targetFields, format);
    }
}
