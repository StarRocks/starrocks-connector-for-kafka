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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import static com.starrocks.connector.kafka.MergeCommitOptionsTest.assertConfigException;

public class StarRocksSinkTaskMergeCommitTest {

    private static Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL, "127.0.0.1:8030");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME, "db");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_USERNAME, "root");
        props.put(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD, "");
        return props;
    }

    private static Map<String, String> mergeCommitProps() {
        Map<String, String> props = baseProps();
        props.put("sink.properties.enable_merge_commit", "true");
        props.put("sink.properties.merge_commit_interval_ms", "1000");
        return props;
    }

    // A write that fails before the SDK registers the table, here on a timeout that is not a number, leaves no
    // exception in the SDK. preCommit must still fail, so that Kafka Connect does not commit the offset of a record
    // that was never sent and replays it instead.
    @Test
    public void testFailedWriteFailsPreCommit() {
        MergeCommitSharedServices.reset();
        Map<String, String> props = mergeCommitProps();
        props.put(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL, "127.0.0.1:1");
        props.put("sink.properties.timeout", "not-an-integer");
        StarRocksSinkTask task = new StarRocksSinkTask();
        task.start(props);
        try {
            task.put(Collections.singletonList(new SinkRecord("tbl", 0, null, null, null,
                    Collections.singletonMap("id", 1), 0)));
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    Collections.singletonMap(new TopicPartition("tbl", 0), new OffsetAndMetadata(1));
            try {
                task.preCommit(offsets);
                Assert.fail("preCommit committed the offset of a record that was not written");
            } catch (RuntimeException e) {
                Throwable cause = e;
                while (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                Assert.assertTrue(cause.toString(), cause instanceof NumberFormatException);
            }
        } finally {
            task.stop();
            MergeCommitSharedServices.reset();
        }
    }

    @Test
    public void testWithoutMergeCommit() {
        StarRocksSinkTask task = new StarRocksSinkTask();
        StreamLoadProperties properties = task.buildLoadProperties(baseProps());
        Assert.assertFalse(task.isMergeCommitEnabled());
        Assert.assertTrue(properties.isEnableTransaction());
        Assert.assertFalse(properties.getHeaders().containsKey("enable_merge_commit"));
        Assert.assertFalse(properties.getHeaders().containsKey("ignore_json_size"));
        Assert.assertEquals(3L * 1024 * 1024 * 1024, properties.getDefaultTableProperties().getChunkLimit().longValue());
        Assert.assertEquals(2, properties.getIoThreadCount());
        Assert.assertEquals(-1, properties.getPublishTimeoutMs());
        Assert.assertEquals(1000L, properties.getExpectDelayTime());
        // The merge commit retries do not change the transaction stream load.
        Assert.assertEquals(0, properties.getMaxRetries());
    }

    @Test
    public void testFlushIntervalBelowOneSecondNeedsMergeCommit() {
        Map<String, String> props = baseProps();
        props.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "200");
        assertConfigException(() -> new StarRocksSinkTask().buildLoadProperties(props));
    }

    @Test
    public void testMergeCommitIntervalIsRequired() {
        Map<String, String> props = mergeCommitProps();
        props.remove("sink.properties.merge_commit_interval_ms");
        assertConfigException(() -> new StarRocksSinkTask().buildLoadProperties(props));
    }

    @Test
    public void testMergeCommitWithJson() {
        Map<String, String> props = mergeCommitProps();
        props.put(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "200");
        StarRocksSinkTask task = new StarRocksSinkTask();
        StreamLoadProperties properties = task.buildLoadProperties(props);
        Assert.assertTrue(task.isMergeCommitEnabled());
        // A transaction stream load can not be merged.
        Assert.assertFalse(properties.isEnableTransaction());
        Assert.assertEquals(200L, properties.getExpectDelayTime());

        // StreamLoadManagerV2 picks merge commit from the headers, and the merge commit loader sends the
        // stream load properties of the table.
        Map<String, String> headers = properties.getHeaders();
        Map<String, String> tableProps = properties.getDefaultTableProperties().getCommonProperties();
        for (Map<String, String> loadProps : Arrays.asList(headers, tableProps)) {
            Assert.assertEquals("true", loadProps.get("enable_merge_commit"));
            Assert.assertEquals("1000", loadProps.get("merge_commit_interval_ms"));
            Assert.assertEquals("3", loadProps.get("merge_commit_parallel"));
            Assert.assertEquals("true", loadProps.get("merge_commit_async"));
            Assert.assertEquals("json", loadProps.get("format"));
            Assert.assertEquals("true", loadProps.get("strip_outer_array"));
            Assert.assertEquals("true", loadProps.get("ignore_json_size"));
        }

        StreamLoadTableProperties tableProperties = properties.getTableProperties("db.tbl", "db", "tbl");
        Assert.assertEquals(MergeCommitOptions.DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER,
                tableProperties.getChunkLimit().longValue());
        Assert.assertEquals(Integer.MAX_VALUE, properties.getMaxConcurrentRequests());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_MAX_RETRIES, properties.getMaxRetries());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_RETRY_INTERVAL_MS, properties.getRetryIntervalInMs());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_PUBLISH_TIMEOUT_MS, properties.getPublishTimeoutMs());
    }

    @Test
    public void testMergeCommitWithCsv() {
        Map<String, String> props = mergeCommitProps();
        props.put(StarRocksSinkConnectorConfig.SINK_FORMAT, "csv");
        StarRocksSinkTask task = new StarRocksSinkTask();
        StreamLoadProperties properties = task.buildLoadProperties(props);
        Assert.assertEquals("csv", properties.getHeaders().get("format"));
        Assert.assertFalse(properties.getHeaders().containsKey("strip_outer_array"));
        Assert.assertFalse(properties.getHeaders().containsKey("ignore_json_size"));
    }

    @Test
    public void testMergeCommitOptions() {
        Map<String, String> props = mergeCommitProps();
        props.put(MergeCommitOptions.BACKEND_DIRECT_CONNECTION, "true");
        props.put(MergeCommitOptions.CHUNK_SIZE, "1048576");
        props.put(StarRocksSinkConnectorConfig.SINK_IO_THREAD_COUNT, "4");
        props.put(StarRocksSinkConnectorConfig.SINK_PUBLISH_TIMEOUT_MS, "20000");
        props.put(MergeCommitOptions.MAX_RETRIES, "0");
        props.put(MergeCommitOptions.RETRY_INTERVAL_MS, "3000");
        StreamLoadProperties properties = new StarRocksSinkTask().buildLoadProperties(props);
        Assert.assertEquals(1048576L, properties.getDefaultTableProperties().getChunkLimit().longValue());
        Assert.assertTrue(properties.isBackendDirectConnection());
        Assert.assertEquals(4, properties.getIoThreadCount());
        Assert.assertEquals(20000, properties.getPublishTimeoutMs());
        Assert.assertEquals(0, properties.getMaxRetries());
        Assert.assertEquals(3000, properties.getRetryIntervalInMs());
    }

    // With an SDK that keeps the data of a table that reaches its request limit, a limit of 1 sends the requests of
    // a table one at a time, in larger chunks.
    @Test
    public void testRequestLimit() {
        Assume.assumeTrue("the stream load SDK does not have starrocks-connector-for-apache-flink#528",
                MergeCommitOptions.sdkKeepsDataAtRequestLimit());
        Map<String, String> props = mergeCommitProps();
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "1");
        StreamLoadProperties properties = new StarRocksSinkTask().buildLoadProperties(props);
        Assert.assertEquals(1, properties.getMaxConcurrentRequests());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_CHUNK_SIZE_FOR_IN_ORDER,
                properties.getTableProperties("db.tbl", "db", "tbl").getChunkLimit().longValue());
    }
}
