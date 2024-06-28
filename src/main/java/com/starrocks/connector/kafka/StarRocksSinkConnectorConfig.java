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

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnectorConfig.class);

    public static final String CONFIG_GROUP_1 = "config_group1";

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
    // The interval at which data is flushed. Valid values: 1000 to 3600000.
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
                        "starrocks datbase name",
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
                        ConfigDef.Range.between(1000, 3600000),
                        ConfigDef.Importance.LOW,
                        "the interval at which data is sent in bulk to starrocks",
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
                );
    }
}
