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
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Assert;
import org.junit.Test;

public class StarRocksSinkConnectorTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL, "127.0.0.1:8030");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME, "db");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_USERNAME, "root");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD, "");
        return props;
    }

    private static List<String> errors(Config config, String name) {
        for (ConfigValue value : config.configValues()) {
            if (value.name().equals(name)) {
                return value.errorMessages();
            }
        }
        throw new AssertionError("No config value for " + name);
    }

    private static void assertNoErrors(Config config) {
        for (ConfigValue value : config.configValues()) {
            Assert.assertTrue(value.name() + ": " + value.errorMessages(), value.errorMessages().isEmpty());
        }
    }

    @Test
    public void testValidateDefaults() {
        assertNoErrors(new StarRocksSinkConnector().validate(baseProps()));
    }

    @Test
    public void testFlushIntervalBelowOneSecondNeedsMergeCommit() {
        Map<String, String> props = baseProps();
        props.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "200");
        Config config = new StarRocksSinkConnector().validate(props);
        Assert.assertEquals(1, errors(config, StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS).size());

        props.put("sink.properties.enable_merge_commit", "true");
        props.put("sink.properties.merge_commit_interval_ms", "1000");
        assertNoErrors(new StarRocksSinkConnector().validate(props));

        // Below the minimum even with merge commit.
        props.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "50");
        config = new StarRocksSinkConnector().validate(props);
        Assert.assertFalse(errors(config, StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS).isEmpty());
    }

    @Test
    public void testMergeCommitIntervalIsRequired() {
        Map<String, String> props = baseProps();
        props.put("sink.properties.enable_merge_commit", "true");
        Config config = new StarRocksSinkConnector().validate(props);
        Assert.assertEquals(1, errors(config, "sink.properties.merge_commit_interval_ms").size());
    }

    @Test
    public void testMergeCommitOptions() {
        Map<String, String> props = baseProps();
        props.put("sink.properties.enable_merge_commit", "true");
        props.put("sink.properties.merge_commit_interval_ms", "1000");
        props.put(MergeCommitOptions.CHUNK_SIZE, "0");
        props.put(MergeCommitOptions.HTTP_THREAD_NUM, "0");
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "-1");
        props.put(MergeCommitOptions.MAX_RETRIES, "-1");
        props.put(MergeCommitOptions.RETRY_INTERVAL_MS, "0");
        Config config = new StarRocksSinkConnector().validate(props);
        Assert.assertFalse(errors(config, MergeCommitOptions.CHUNK_SIZE).isEmpty());
        Assert.assertFalse(errors(config, MergeCommitOptions.HTTP_THREAD_NUM).isEmpty());
        Assert.assertTrue(errors(config, MergeCommitOptions.MAX_CONCURRENT_REQUESTS).isEmpty());
        Assert.assertTrue(errors(config, MergeCommitOptions.CHECK_LABEL_STATE_TIMEOUT_MS).isEmpty());
        Assert.assertFalse(errors(config, MergeCommitOptions.MAX_RETRIES).isEmpty());
        Assert.assertFalse(errors(config, MergeCommitOptions.RETRY_INTERVAL_MS).isEmpty());

        // A limit is accepted with an SDK that keeps the data of a table that reaches it.
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "0");
        config = new StarRocksSinkConnector().validate(props);
        Assert.assertEquals(MergeCommitOptions.sdkKeepsDataAtRequestLimit() ? 0 : 1,
                errors(config, MergeCommitOptions.MAX_CONCURRENT_REQUESTS).size());
    }
}
