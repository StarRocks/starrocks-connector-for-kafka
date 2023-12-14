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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Util {
    private static final Logger LOG = LoggerFactory.getLogger(Util.class);
    public static final String VERSION = "1.0.2";

    static boolean isValidStarrocksTableName(String tableName) {
        return tableName.matches("^([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+$");
    }
    public static Map<String, String> parseTopicToTableMap(String input) {
        Map<String, String> topic2Table = new HashMap<>();
        boolean isInvalid = false;
        for (String str : input.split(",")) {
            String[] tt = str.split(":");

            if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
                LOG.error(
                        "Invalid {} config format: {}", StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP, input);
                return null;
            }

            String topic = tt[0].trim();
            String table = tt[1].trim();

            if (!isValidStarrocksTableName(table)) {
                LOG.error(
                        "table name {} should have at least 2 "
                                + "characters, start with _a-zA-Z, and only contains "
                                + "_$a-zA-z0-9",
                        table);
                isInvalid = true;
            }

            if (topic2Table.containsKey(topic)) {
                LOG.error("topic name {} is duplicated", topic);
                isInvalid = true;
            }

            topic2Table.put(tt[0].trim(), tt[1].trim());
        }
        if (isInvalid) {
            String errMsg = String.format("Invalid {} config format: {}", StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP, input);
            throw new RuntimeException(errMsg);
        }
        return topic2Table;
    }
}
