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

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnectorConfig.class);

    public static final String CONFIG_GROUP_1 = "config_group1";
    public static final String CONFIG_GROUP_MERGE_COMMIT = "merge_commit";

    // FE HTTP Server connection address.
    // The format is <fe_host1>:<fe_http_port1>,<fe_host2>:<fe_http_port2>,
    // You can provide multiple addresses, separated by commas  (,).
    // For example: 192.168.xxx.xxx:8030,192.168.xxx.xxx:8030
    public static final String STARROCKS_LOAD_URL = "starrocks.http.url";
    // The target database for loading data.
    public static final String STARROCKS_DATABASE_NAME = "starrocks.database.name";
    // The target database for loading data. The default is JSON.
    public static final String SINK_FORMAT = "sink.properties.format";
    // This configuration is used to specify line delimiters when writing CSV data to starrocks.
    public static final String SINK_PROPERTIES_ROW_DELIMITER = "sink.properties.row_delimiter";
    // The maximum size of data that can be loaded into StarRocks at a time.
    // Valid values: 64 MB to 10 GB.
    // The SDK buffer may buffer data from multiple streams,
    // and this threshold refers to the total size
    public static final String BUFFERFLUSH_MAXBYTES = "bufferflush.maxbytes";
    // The period of time after which the stream load times out. Valid values: 100 to 60000.
    public static final String CONNECT_TIMEOUTMS = "connect.timeoutms";
    public static final String STARROCKS_USERNAME = "starrocks.username";
    public static final String STARROCKS_PASSWORD = "starrocks.password";
    // The interval at which data is flushed. Valid values: 1000 to 3600000, or 100 to 3600000 with merge commit.
    public static final String BUFFERFLUSH_INTERVALMS = "bufferflush.intervalms";
    // Stream Load parameters, which controls the load behavior.
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";
    // When the topic name does not match the SR table name,
    // this configuration item provides a mapping in the following format:
    // <topic-1>:<table-1>,<topic-2>:<table-2>,...
    public static final String STARROCKS_TOPIC2TABLE_MAP = "starrocks.topic2table.map";
    // Data writing to StarRocks may fail due to a network fault or a short time StarRcoks restart.
    // For precommit, the connector detects if an error has occurred and writes the failed data to the SR again.
    // This configuration controls the number of failed retries. The default value is 3. -1 indicates unlimited retry.
    public static final String SINK_MAXRETRIES = "sink.maxretries";
    // The number of threads that send stream load requests to StarRocks in a task.
    public static final String SINK_IO_THREAD_COUNT = "sink.io.thread-count";
    // How long to wait for a committed transaction to become visible before treating the load as successful.
    // -1 uses the StarRocks default, and merge commit uses 10000 if not set.
    public static final String SINK_PUBLISH_TIMEOUT_MS = "sink.publish-timeout.ms";

    public static final String[] mustRequiredConfigs = {
            STARROCKS_LOAD_URL,
            STARROCKS_DATABASE_NAME,
            STARROCKS_USERNAME,
            STARROCKS_PASSWORD
    };

    public static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(
                        STARROCKS_LOAD_URL,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks http url",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_LOAD_URL
                ).define(
                        STARROCKS_DATABASE_NAME,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks database name",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_DATABASE_NAME
                ).define(
                        SINK_FORMAT,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.LOW,
                        "write to starrocks data format",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_FORMAT
                ).define(
                        BUFFERFLUSH_MAXBYTES,
                        ConfigDef.Type.LONG,
                        67108864,
                        ConfigDef.Range.between(67108864, 10737418240L),
                        ConfigDef.Importance.LOW,
                        "the size of a batch of data",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        BUFFERFLUSH_MAXBYTES
                ).define(
                        CONNECT_TIMEOUTMS,
                        ConfigDef.Type.LONG,
                        100,
                        ConfigDef.Range.between(100, 60000),
                        ConfigDef.Importance.LOW,
                        "timeout period for connecting to load-url",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        CONNECT_TIMEOUTMS
                ).define(
                        STARROCKS_USERNAME,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks username",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_USERNAME
                ).define(
                        STARROCKS_PASSWORD,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.HIGH,
                        "starrocks password",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_PASSWORD
                ).define(
                        BUFFERFLUSH_INTERVALMS,
                        ConfigDef.Type.LONG,
                        1000,
                        ConfigDef.Range.between(MergeCommitOptions.MIN_BUFFERFLUSH_INTERVAL_MS, 3600000),
                        ConfigDef.Importance.LOW,
                        "the interval at which data is sent in bulk to starrocks, at least 1000 unless merge " +
                                "commit is enabled",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        BUFFERFLUSH_INTERVALMS
                ).define(
                        STARROCKS_TOPIC2TABLE_MAP,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.LOW,
                        "a mapping between the topic name and the table name",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_TOPIC2TABLE_MAP
                ).define(
                        SINK_MAXRETRIES,
                        ConfigDef.Type.LONG,
                        3,
                        ConfigDef.Range.between(-1, Long.MAX_VALUE),
                        ConfigDef.Importance.LOW,
                        "number of Stream Load retries after a stream load failure",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_MAXRETRIES
                ).define(
                        SINK_IO_THREAD_COUNT,
                        ConfigDef.Type.INT,
                        2,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "number of threads that send stream load requests to starrocks in a task",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_IO_THREAD_COUNT
                ).define(
                        SINK_PUBLISH_TIMEOUT_MS,
                        ConfigDef.Type.INT,
                        null,
                        null,
                        ConfigDef.Importance.LOW,
                        "how long to wait for a committed transaction to become visible before treating the " +
                                "load as successful. -1 uses the starrocks default, and merge commit uses 10000 " +
                                "if not set",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_PUBLISH_TIMEOUT_MS
                ).define(
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.ENABLE_MERGE_COMMIT,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "whether starrocks merges the stream load requests of all tasks to a table within " +
                                "the merge window into one transaction. Requires starrocks 3.4 or later",
                        CONFIG_GROUP_MERGE_COMMIT,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.ENABLE_MERGE_COMMIT
                ).define(
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_INTERVAL_MS,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.MEDIUM,
                        "the merge window in milliseconds. Required when merge commit is enabled",
                        CONFIG_GROUP_MERGE_COMMIT,
                        1,
                        ConfigDef.Width.NONE,
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_INTERVAL_MS
                ).define(
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_PARALLEL,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.LOW,
                        "the parallelism of the load plan of each merged transaction, 3 if not set",
                        CONFIG_GROUP_MERGE_COMMIT,
                        2,
                        ConfigDef.Width.NONE,
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_PARALLEL
                ).define(
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_ASYNC,
                        ConfigDef.Type.STRING,
                        null,
                        null,
                        ConfigDef.Importance.LOW,
                        "whether starrocks returns a request once it receives the data (true) or once the merged " +
                                "transaction is visible (false), true if not set. The connector waits until the " +
                                "data is visible before it commits the offsets in both cases",
                        CONFIG_GROUP_MERGE_COMMIT,
                        3,
                        ConfigDef.Width.NONE,
                        SINK_PROPERTIES_PREFIX + MergeCommitOptions.MERGE_COMMIT_ASYNC
                ).define(
                        MergeCommitOptions.CHUNK_SIZE,
                        ConfigDef.Type.LONG,
                        null,
                        new OptionalAtLeast(1),
                        ConfigDef.Importance.LOW,
                        "the maximum bytes of a table sent in one request. 20971520 if not set, or 524288000 " +
                                "when the requests of a table are sent one at a time",
                        CONFIG_GROUP_MERGE_COMMIT,
                        4,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.CHUNK_SIZE
                ).define(
                        MergeCommitOptions.MAX_CONCURRENT_REQUESTS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_MAX_CONCURRENT_REQUESTS,
                        null,
                        ConfigDef.Importance.LOW,
                        "the maximum number of inflight requests of a table in a task. 0 or 1 sends one request " +
                                "at a time, which keeps the order of the records for primary key tables. A " +
                                "negative value means no limit. A limit needs the stream load sdk of " +
                                "starrocks-connector-for-apache-flink 3ad9f84 or later",
                        CONFIG_GROUP_MERGE_COMMIT,
                        5,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.MAX_CONCURRENT_REQUESTS
                ).define(
                        MergeCommitOptions.HTTP_THREAD_NUM,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_HTTP_THREAD_NUM,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "number of threads that check the state of the merged transactions",
                        CONFIG_GROUP_MERGE_COMMIT,
                        6,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.HTTP_THREAD_NUM
                ).define(
                        MergeCommitOptions.HTTP_MAX_CONNECTIONS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_HTTP_MAX_CONNECTIONS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "the maximum number of http connections to a starrocks node",
                        CONFIG_GROUP_MERGE_COMMIT,
                        7,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.HTTP_MAX_CONNECTIONS
                ).define(
                        MergeCommitOptions.HTTP_TOTAL_MAX_CONNECTIONS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_HTTP_TOTAL_MAX_CONNECTIONS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "the maximum number of http connections to starrocks",
                        CONFIG_GROUP_MERGE_COMMIT,
                        8,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.HTTP_TOTAL_MAX_CONNECTIONS
                ).define(
                        MergeCommitOptions.HTTP_IDLE_CONNECTION_TIMEOUT_MS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_HTTP_IDLE_CONNECTION_TIMEOUT_MS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "how long an idle http connection is kept",
                        CONFIG_GROUP_MERGE_COMMIT,
                        9,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.HTTP_IDLE_CONNECTION_TIMEOUT_MS
                ).define(
                        MergeCommitOptions.NODE_META_UPDATE_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_NODE_META_UPDATE_INTERVAL_MS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "how often to refresh the nodes that starrocks assigns to a table, used with " +
                                MergeCommitOptions.BACKEND_DIRECT_CONNECTION,
                        CONFIG_GROUP_MERGE_COMMIT,
                        10,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.NODE_META_UPDATE_INTERVAL_MS
                ).define(
                        MergeCommitOptions.CHECK_LABEL_STATE_INIT_DELAY_MS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_CHECK_LABEL_STATE_INIT_DELAY_MS,
                        ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.LOW,
                        "how long after the merge window closes to first check the state of a merged transaction",
                        CONFIG_GROUP_MERGE_COMMIT,
                        11,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.CHECK_LABEL_STATE_INIT_DELAY_MS
                ).define(
                        MergeCommitOptions.CHECK_LABEL_STATE_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_CHECK_LABEL_STATE_INTERVAL_MS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "how often to check the state of a merged transaction until it is visible",
                        CONFIG_GROUP_MERGE_COMMIT,
                        12,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.CHECK_LABEL_STATE_INTERVAL_MS
                ).define(
                        MergeCommitOptions.CHECK_LABEL_STATE_TIMEOUT_MS,
                        ConfigDef.Type.INT,
                        null,
                        new OptionalAtLeast(1),
                        ConfigDef.Importance.LOW,
                        "how long to wait for a merged transaction to become visible. The stream load timeout " +
                                "if not set",
                        CONFIG_GROUP_MERGE_COMMIT,
                        13,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.CHECK_LABEL_STATE_TIMEOUT_MS
                ).define(
                        MergeCommitOptions.BACKEND_DIRECT_CONNECTION,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        null,
                        ConfigDef.Importance.LOW,
                        "send the requests to the be or cn nodes that starrocks assigns to the table instead " +
                                "of an fe node, which redirects every request through the leader fe. The nodes " +
                                "must be reachable from kafka connect",
                        CONFIG_GROUP_MERGE_COMMIT,
                        14,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.BACKEND_DIRECT_CONNECTION
                ).define(
                        MergeCommitOptions.MAX_RETRIES,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_MAX_RETRIES,
                        ConfigDef.Range.atLeast(0),
                        ConfigDef.Importance.LOW,
                        "how many times a failed stream load request is sent again, to an fe picked at random, " +
                                "before the flush fails and the tasks replay the records from their committed " +
                                "offsets. 0 fails the flush at once",
                        CONFIG_GROUP_MERGE_COMMIT,
                        15,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.MAX_RETRIES
                ).define(
                        MergeCommitOptions.RETRY_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        MergeCommitOptions.DEFAULT_RETRY_INTERVAL_MS,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "how long to wait before sending a failed stream load request again. The stream load " +
                                "sdk adds up to 2 seconds at random",
                        CONFIG_GROUP_MERGE_COMMIT,
                        16,
                        ConfigDef.Width.NONE,
                        MergeCommitOptions.RETRY_INTERVAL_MS
                );
    }

    // Returns the stream load properties, the options with the prefix sink.properties., keyed by their
    // lowercased names without the prefix.
    public static Map<String, String> getStreamLoadProperties(Map<String, String> props) {
        Map<String, String> streamLoadProps = new HashMap<>();
        props.forEach((key, value) -> {
            if (key.startsWith(SINK_PROPERTIES_PREFIX)) {
                streamLoadProps.put(key.substring(SINK_PROPERTIES_PREFIX.length()).toLowerCase(), value);
            }
        });
        return streamLoadProps;
    }

    static int getInt(Map<String, String> props, String key, int defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(key, value, "must be an integer");
        }
    }

    static long getLong(Map<String, String> props, String key, long defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException(key, value, "must be a long integer");
        }
    }

    static boolean getBoolean(Map<String, String> props, String key, boolean defaultValue) {
        String value = props.get(key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        throw new ConfigException(key, value, "must be true or false");
    }

    // Like ConfigDef.Range.atLeast, but also accepts no value for an option that has no default.
    static class OptionalAtLeast implements ConfigDef.Validator {

        private final long min;

        OptionalAtLeast(long min) {
            this.min = min;
        }

        @Override
        public void ensureValid(String name, Object value) {
            if (value != null && ((Number) value).longValue() < min) {
                throw new ConfigException(name, value, "Value must be at least " + min);
            }
        }

        @Override
        public String toString() {
            return "[" + min + ",...] or not set";
        }
    }
}
