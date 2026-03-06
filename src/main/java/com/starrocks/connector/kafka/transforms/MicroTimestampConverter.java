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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Kafka Connect SMT that converts microsecond-epoch Long values to datetime strings
 * for specified fields in schemaless (Map) records.
 *
 * <p>MySQL DATETIME(6) columns are emitted by Debezium as microseconds since Unix epoch (Long),
 * which StarRocks cannot parse as DATETIME. This SMT converts those Longs to formatted datetime
 * strings that StarRocks Stream Load can ingest directly.
 *
 * <p>Intended to run <em>after</em> {@code AddOpFieldForDebeziumRecord} has flattened the
 * Debezium envelope into a plain Map record.
 *
 * <p>Example configuration:
 * <pre>
 *   transforms=addfield,convertDatetime
 *   transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
 *   transforms.convertDatetime.type=com.starrocks.connector.kafka.transforms.MicroTimestampConverter
 *   transforms.convertDatetime.fields=createdAt,updatedAt,preparedAt,completedAt
 * </pre>
 */
public class MicroTimestampConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOG = LoggerFactory.getLogger(MicroTimestampConverter.class);

    static final String FIELDS_CONFIG = "fields";
    static final String FORMAT_CONFIG = "format";
    private static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG,
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    "Comma-separated list of field names holding microsecond-epoch Long values to convert to datetime strings.")
            .define(FORMAT_CONFIG,
                    ConfigDef.Type.STRING,
                    DEFAULT_FORMAT,
                    ConfigDef.Importance.LOW,
                    "Java DateTimeFormatter pattern for the output string (default: yyyy-MM-dd HH:mm:ss).");

    private Set<String> fields;
    private DateTimeFormatter formatter;

    @Override
    public void configure(Map<String, ?> configs) {
        String fieldsStr = (String) configs.get(FIELDS_CONFIG);
        this.fields = Arrays.stream(fieldsStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
        Object fmtObj = configs.get(FORMAT_CONFIG);
        String format = (fmtObj != null) ? fmtObj.toString() : DEFAULT_FORMAT;
        this.formatter = DateTimeFormatter.ofPattern(format).withZone(ZoneOffset.UTC);
    }

    @Override
    @SuppressWarnings("unchecked")
    public R apply(R record) {
        if (!(record.value() instanceof Map)) {
            return record;
        }

        Map<String, Object> original = (Map<String, Object>) record.value();
        Map<String, Object> updated = null;

        for (String field : fields) {
            Object raw = original.get(field);
            if (!(raw instanceof Number) || raw instanceof Double || raw instanceof Float) {
                // null, strings, or floating-point values are left as-is
                continue;
            }
            long micros = ((Number) raw).longValue();
            try {
                long seconds = micros / 1_000_000L;
                int nanoAdj = (int) ((micros % 1_000_000L) * 1_000L);
                String formatted = formatter.format(Instant.ofEpochSecond(seconds, nanoAdj));
                if (updated == null) {
                    updated = new HashMap<>(original);
                }
                updated.put(field, formatted);
            } catch (Exception e) {
                LOG.warn("MicroTimestampConverter: failed to convert field '{}' value={} — leaving as-is", field, micros, e);
            }
        }

        if (updated == null) {
            return record;
        }
        return record.newRecord(
                record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                null, updated,
                record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
