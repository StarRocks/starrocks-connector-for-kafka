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

package com.starrocks.connector.kafka;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StarRocksSinkConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConfig.class);

    public enum SinkFormat {
        CSV,
        JSON,
    }

    public enum Envelope {
        NONE,
        DEBEZIUM,
    }

    // Connection
    public static final String LOAD_URL_CONFIG = "starrocks.http.url";
    private static final String LOAD_URL_DOC =
            "FE HTTP server connection address. The format is "
                    + "``<fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>``. "
                    + "For example, ``192.168.xxx.xxx:8030,192.168.xxx.xxx:8030``.";
    private static final String LOAD_URL_DISPLAY = "StarRocks HTTP URL";

    public static final String USERNAME_CONFIG = "starrocks.username";
    private static final String USERNAME_DOC = "StarRocks username.";
    private static final String USERNAME_DISPLAY = "Username";

    public static final String PASSWORD_CONFIG = "starrocks.password";
    private static final String PASSWORD_DOC = "StarRocks password.";
    private static final String PASSWORD_DISPLAY = "Password";

    public static final String DATABASE_CONFIG = "starrocks.database.name";
    private static final String DATABASE_DOC = "The target database for loading data.";
    private static final String DATABASE_DISPLAY = "Database";

    // Sink
    public static final String TOPIC2TABLE_MAP_CONFIG = "starrocks.topic2table.map";
    private static final String TOPIC2TABLE_MAP_DOC =
            "Mapping from topic name to table name when they differ. "
                    + "Format: ``<topic-1>:<table-1>,<topic-2>:<table-2>,...``.";
    private static final String TOPIC2TABLE_MAP_DISPLAY = "Topic to Table Map";

    public static final String MAX_RETRIES_CONFIG = "sink.maxretries";
    public static final long MAX_RETRIES_DEFAULT = 3;
    private static final String MAX_RETRIES_DOC =
            "Number of failed retries after a stream load failure. "
                    + "Default is ``3``. ``-1`` indicates unlimited retries.";
    private static final String MAX_RETRIES_DISPLAY = "Max Retries";

    public static final String ENVELOPE_CONFIG = "starrocks.envelope";
    public static final Envelope ENVELOPE_DEFAULT = Envelope.NONE;
    private static final String ENVELOPE_DOC =
            "Envelope format of incoming Kafka records. Supported values: "
                    + "``none`` (default), ``debezium``. When set to ``debezium``, records are "
                    + "expected to be in Debezium CDC envelope format and are unwrapped before "
                    + "being written to StarRocks.";
    private static final String ENVELOPE_DISPLAY = "Envelope Format";

    public static final String DELETE_ENABLED_CONFIG = "starrocks.delete.enabled";
    public static final boolean DELETE_ENABLED_DEFAULT = false;
    private static final String DELETE_ENABLED_DOC =
            "Whether the connector processes DELETE or tombstone events and removes the "
                    + "corresponding row from the database. Requires the target table to be a "
                    + "PRIMARY KEY table.";
    private static final String DELETE_ENABLED_DISPLAY = "Enable Deletes";

    // Stream Load
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";

    public static final String FORMAT_CONFIG = "sink.properties.format";
    public static final SinkFormat FORMAT_DEFAULT = SinkFormat.JSON;
    private static final String FORMAT_DOC =
            "Data format for writing to StarRocks. Supported values: ``json`` (default), ``csv``.";
    private static final String FORMAT_DISPLAY = "Data Format";

    public static final String ROW_DELIMITER_CONFIG = "sink.properties.row_delimiter";
    private static final String ROW_DELIMITER_DOC =
            "Line delimiter for writing CSV data to StarRocks.";
    private static final String ROW_DELIMITER_DISPLAY = "Row Delimiter";

    public static final String BUFFER_MAX_BYTES_CONFIG = "bufferflush.maxbytes";
    public static final long BUFFER_MAX_BYTES_DEFAULT = 64 * 1024 * 1024L;
    private static final String BUFFER_MAX_BYTES_DOC =
            "Maximum size of data (in bytes) that can be loaded into StarRocks at a time. "
                    + "Valid values: 64 MB to 10 GB. The SDK buffer may buffer data from multiple "
                    + "streams, and this threshold refers to the total size.";
    private static final String BUFFER_MAX_BYTES_DISPLAY = "Buffer Max Bytes";

    public static final String BUFFER_FLUSH_INTERVAL_MS_CONFIG = "bufferflush.intervalms";
    public static final long BUFFER_FLUSH_INTERVAL_MS_DEFAULT = 1000;
    private static final String BUFFER_FLUSH_INTERVAL_MS_DOC =
            "Interval (in milliseconds) at which data is flushed. Valid values: 1000 to 3600000.";
    private static final String BUFFER_FLUSH_INTERVAL_MS_DISPLAY = "Buffer Flush Interval (ms)";

    public static final String CONNECT_TIMEOUT_MS_CONFIG = "connect.timeoutms";
    public static final long CONNECT_TIMEOUT_MS_DEFAULT = 100;
    private static final String CONNECT_TIMEOUT_MS_DOC =
            "Timeout (in milliseconds) for connecting to the load URL. Valid values: 100 to 60000.";
    private static final String CONNECT_TIMEOUT_MS_DISPLAY = "Connection Timeout (ms)";

    public static final ConfigDef CONFIG_DEF;

    static {
        var idx = 0;

        // Connection configuration
        var connectionGroup = "StarRocks Connection";
        var config = new ConfigDef();
        config.define(
                LOAD_URL_CONFIG, Type.LIST, ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                LOAD_URL_DOC,
                connectionGroup, idx++,
                Width.LONG, LOAD_URL_DISPLAY
        ).define(
                USERNAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                USERNAME_DOC,
                connectionGroup, idx++,
                Width.MEDIUM, USERNAME_DISPLAY
        ).define(
                PASSWORD_CONFIG, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE,
                Importance.HIGH,
                PASSWORD_DOC,
                connectionGroup, idx++,
                Width.MEDIUM, PASSWORD_DISPLAY
        ).define(
                DATABASE_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                Importance.HIGH,
                DATABASE_DOC,
                connectionGroup, idx++,
                Width.MEDIUM, DATABASE_DISPLAY
        );

        // Sink configuration
        idx = 0;
        var sinkGroup = "StarRocks sink";
        config.define(
                TOPIC2TABLE_MAP_CONFIG, Type.STRING, null,
                Importance.LOW,
                TOPIC2TABLE_MAP_DOC,
                sinkGroup, idx++,
                Width.LONG, TOPIC2TABLE_MAP_DISPLAY
        ).define(
                MAX_RETRIES_CONFIG, Type.LONG, MAX_RETRIES_DEFAULT,
                ConfigDef.Range.between(-1, Long.MAX_VALUE),
                Importance.LOW,
                MAX_RETRIES_DOC,
                sinkGroup, idx++,
                Width.SHORT, MAX_RETRIES_DISPLAY
        ).define(
                ENVELOPE_CONFIG, Type.STRING, ENVELOPE_DEFAULT.name().toLowerCase(),
                ConfigDef.ValidString.in("none", "debezium"),
                Importance.MEDIUM,
                ENVELOPE_DOC,
                sinkGroup, idx++,
                Width.SHORT, ENVELOPE_DISPLAY
        ).define(
                DELETE_ENABLED_CONFIG, Type.BOOLEAN, DELETE_ENABLED_DEFAULT,
                Importance.MEDIUM,
                DELETE_ENABLED_DOC,
                sinkGroup, idx++,
                Width.SHORT, DELETE_ENABLED_DISPLAY
        );

        // StreamLoad configuration
        idx = 0;
        var streamLoadGroup = "StarRocks StreamLoad";
        config.define(
                FORMAT_CONFIG, Type.STRING, FORMAT_DEFAULT.name().toLowerCase(),
                ConfigDef.ValidString.in("json", "csv"),
                Importance.MEDIUM,
                FORMAT_DOC,
                streamLoadGroup, idx++,
                Width.SHORT, FORMAT_DISPLAY
        ).define(
                ROW_DELIMITER_CONFIG, Type.STRING, null,
                Importance.LOW,
                ROW_DELIMITER_DOC,
                streamLoadGroup, idx++,
                Width.SHORT, ROW_DELIMITER_DISPLAY
        ).define(
                BUFFER_MAX_BYTES_CONFIG, Type.LONG, BUFFER_MAX_BYTES_DEFAULT,
                ConfigDef.Range.between(64 * 1024 * 1024L, 10 * 1024 * 1024 * 1024L),
                Importance.LOW,
                BUFFER_MAX_BYTES_DOC,
                streamLoadGroup, idx++,
                Width.MEDIUM, BUFFER_MAX_BYTES_DISPLAY
        ).define(
                BUFFER_FLUSH_INTERVAL_MS_CONFIG, Type.LONG, BUFFER_FLUSH_INTERVAL_MS_DEFAULT,
                ConfigDef.Range.between(1000, 3600000),
                Importance.LOW,
                BUFFER_FLUSH_INTERVAL_MS_DOC,
                streamLoadGroup, idx++,
                Width.SHORT, BUFFER_FLUSH_INTERVAL_MS_DISPLAY
        ).define(
                CONNECT_TIMEOUT_MS_CONFIG, Type.INT, CONNECT_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.between(100, 60000),
                Importance.LOW,
                CONNECT_TIMEOUT_MS_DOC,
                streamLoadGroup, idx++,
                Width.SHORT, CONNECT_TIMEOUT_MS_DISPLAY
        );

        CONFIG_DEF = config;
    }

    public final String[] loadUrl;
    public final String username;
    public final String password;
    public final String database;
    public final Map<String, String> topic2Table;
    public final long maxRetries;
    public final Envelope envelope;
    public final boolean deleteEnabled;
    public final SinkFormat format;
    public final String rowDelimiter;
    public final Map<String, String> streamLoadProps;
    public final long bufferMaxBytes;
    public final long bufferFlushIntervalMs;
    public final int connectTimeoutMs;

    public StarRocksSinkConfig(Map<?, ?> props) {
        super(CONFIG_DEF, props);
        loadUrl = getList(LOAD_URL_CONFIG).toArray(String[]::new);
        username = getString(USERNAME_CONFIG);
        password = getPasswordValue(PASSWORD_CONFIG);
        database = getString(DATABASE_CONFIG);
        topic2Table = parseTopicToTableMap(getString(TOPIC2TABLE_MAP_CONFIG));
        maxRetries = getLong(MAX_RETRIES_CONFIG);
        envelope = Envelope.valueOf(getString(ENVELOPE_CONFIG).toUpperCase());
        deleteEnabled = getBoolean(DELETE_ENABLED_CONFIG);
        format = SinkFormat.valueOf(getString(FORMAT_CONFIG).toUpperCase());
        rowDelimiter = getString(ROW_DELIMITER_CONFIG);
        streamLoadProps = originalsWithPrefix(SINK_PROPERTIES_PREFIX, true).entrySet().stream()
                .collect(Collectors.toUnmodifiableMap(
                        e -> e.getKey().toLowerCase(),
                        e -> String.valueOf(e.getValue())
                ));
        bufferMaxBytes = getLong(BUFFER_MAX_BYTES_CONFIG);
        bufferFlushIntervalMs = getLong(BUFFER_FLUSH_INTERVAL_MS_CONFIG);
        connectTimeoutMs = getInt(CONNECT_TIMEOUT_MS_CONFIG);
    }

    private String getPasswordValue(String key) {
        Password pwd = getPassword(key);
        return pwd != null ? pwd.value() : null;
    }

    private static final Predicate<String> nameMatcher = Pattern
            .compile("^[_a-zA-Z][_$a-zA-Z0-9]+(\\.[_a-zA-Z][_$a-zA-Z0-9]+){0,2}$")
            .asMatchPredicate();

    private static Map<String, String> parseTopicToTableMap(String input) {
        if (input == null || input.isEmpty()) {
            return Collections.emptyMap();
        }

        var topic2Table = new HashMap<String, String>();
        for (var str : input.split(",")) {
            var tt = Arrays.stream(str.split(":"))
                    .map(String::trim)
                    .toArray(String[]::new);

            if (tt.length != 2 || tt[0].isEmpty() || tt[1].isEmpty()) {
                throw new ConfigException(TOPIC2TABLE_MAP_CONFIG, input, "each entry must be in <topic>:<table> format");
            }

            var topic = tt[0];
            var table = tt[1];

            if (!nameMatcher.test(table)) {
                throw new ConfigException(TOPIC2TABLE_MAP_CONFIG, input,
                        "table name '" + table + "' must start with a letter or underscore and contain only letters, digits, underscores, or dollar signs");
            }

            if (topic2Table.containsKey(topic)) {
                throw new ConfigException(TOPIC2TABLE_MAP_CONFIG, input, "topic '" + topic + "' is duplicated");
            }

            topic2Table.put(topic, table);
        }
        return Collections.unmodifiableMap(topic2Table);
    }
}
