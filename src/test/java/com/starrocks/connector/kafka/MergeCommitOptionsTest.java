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

import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import com.starrocks.data.load.stream.mergecommit.Table;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.SINK_PUBLISH_TIMEOUT_MS;

public class MergeCommitOptionsTest {

    // JUnit 4.12 has no assertThrows.
    static void assertConfigException(Runnable runnable) {
        try {
            runnable.run();
        } catch (ConfigException e) {
            return;
        }
        Assert.fail("Expected a ConfigException");
    }

    private static Map<String, String> mergeCommitProps() {
        Map<String, String> props = new HashMap<>();
        props.put("sink.properties.enable_merge_commit", "true");
        props.put("sink.properties.merge_commit_interval_ms", "1000");
        return props;
    }

    @Test
    public void testIsEnabled() {
        Map<String, String> streamLoadProps = new HashMap<>();
        Assert.assertFalse(MergeCommitOptions.isEnabled(streamLoadProps));
        streamLoadProps.put("enable_merge_commit", "false");
        Assert.assertFalse(MergeCommitOptions.isEnabled(streamLoadProps));
        streamLoadProps.put("enable_merge_commit", "TRUE");
        Assert.assertTrue(MergeCommitOptions.isEnabled(streamLoadProps));
    }

    @Test
    public void testFlushIntervalBelowOneSecondNeedsMergeCommit() {
        Map<String, String> props = new HashMap<>();
        props.put(BUFFERFLUSH_INTERVALMS, "1000");
        Assert.assertTrue(MergeCommitOptions.validate(props).isEmpty());

        props.put(BUFFERFLUSH_INTERVALMS, "200");
        Map<String, String> errors = MergeCommitOptions.validate(props);
        Assert.assertEquals(1, errors.size());
        Assert.assertTrue(errors.get(BUFFERFLUSH_INTERVALMS).contains("unless merge commit is enabled"));

        props.put("sink.properties.enable_merge_commit", "false");
        Assert.assertTrue(MergeCommitOptions.validate(props).containsKey(BUFFERFLUSH_INTERVALMS));

        props.putAll(mergeCommitProps());
        Assert.assertTrue(MergeCommitOptions.validate(props).isEmpty());
    }

    @Test
    public void testMergeCommitIntervalIsRequired() {
        Map<String, String> props = mergeCommitProps();
        props.remove("sink.properties.merge_commit_interval_ms");
        Assert.assertTrue(MergeCommitOptions.validate(props).get("sink.properties.merge_commit_interval_ms")
                .contains("must be set"));

        for (String invalid : new String[] {"0", "-1", "abc"}) {
            props.put("sink.properties.merge_commit_interval_ms", invalid);
            Assert.assertTrue(invalid, MergeCommitOptions.validate(props)
                    .containsKey("sink.properties.merge_commit_interval_ms"));
        }
    }

    @Test
    public void testInvalidParallelAndAsync() {
        Map<String, String> props = mergeCommitProps();
        props.put("sink.properties.merge_commit_parallel", "0");
        props.put("sink.properties.merge_commit_async", "yes");
        Map<String, String> errors = MergeCommitOptions.validate(props);
        Assert.assertEquals(2, errors.size());
        Assert.assertTrue(errors.containsKey("sink.properties.merge_commit_parallel"));
        Assert.assertTrue(errors.containsKey("sink.properties.merge_commit_async"));
    }

    // A limit is only accepted with an SDK that keeps the data of a table that reaches it.
    @Test
    public void testRequestLimitNeedsTheSdkFix() {
        Map<String, String> props = mergeCommitProps();
        for (String limit : new String[] {"0", "1", "5"}) {
            props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, limit);
            Assert.assertTrue(limit, MergeCommitOptions.validate(props, false)
                    .containsKey(MergeCommitOptions.MAX_CONCURRENT_REQUESTS));
            Assert.assertTrue(limit, MergeCommitOptions.validate(props, true).isEmpty());
        }
        for (String noLimit : new String[] {"-1", String.valueOf(Integer.MAX_VALUE)}) {
            props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, noLimit);
            Assert.assertTrue(noLimit, MergeCommitOptions.validate(props, false).isEmpty());
        }
        // Without merge commit, the option does not matter.
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "0");
        props.put("sink.properties.enable_merge_commit", "false");
        Assert.assertTrue(MergeCommitOptions.validate(props, false).isEmpty());
    }

    // Stands in for the MergeCommitManager of an SDK with the fix.
    public static class FixedMergeCommitManager {
        public void onTableFailure(Table table, Throwable throwable) {
        }
    }

    @Test
    public void testDetectsTheSdkFix() {
        Assert.assertTrue(MergeCommitOptions.hasRequestLimitFix(FixedMergeCommitManager.class));
        Assert.assertFalse(MergeCommitOptions.hasRequestLimitFix(Object.class));
    }

    @Test
    public void testStreamLoadPropertyNamesAreCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put("sink.properties.ENABLE_MERGE_COMMIT", "true");
        props.put("sink.properties.Merge_Commit_Interval_Ms", "500");
        Assert.assertTrue(MergeCommitOptions.validate(props).isEmpty());
    }

    @Test
    public void testCheckThrowsTheFirstError() {
        Map<String, String> props = mergeCommitProps();
        props.remove("sink.properties.merge_commit_interval_ms");
        try {
            MergeCommitOptions.check(props);
            Assert.fail("Expected a ConfigException");
        } catch (ConfigException e) {
            Assert.assertTrue(e.getMessage().contains("sink.properties.merge_commit_interval_ms"));
        }
        MergeCommitOptions.check(mergeCommitProps());
    }

    @Test
    public void testAddDefaultStreamLoadPropertiesForJson() {
        Map<String, String> streamLoadProps = new HashMap<>();
        streamLoadProps.put("enable_merge_commit", "TRUE");
        streamLoadProps.put("merge_commit_interval_ms", "1000");
        MergeCommitOptions.addDefaultStreamLoadProperties(streamLoadProps, true);
        Assert.assertEquals("true", streamLoadProps.get("enable_merge_commit"));
        Assert.assertEquals("1000", streamLoadProps.get("merge_commit_interval_ms"));
        Assert.assertEquals("3", streamLoadProps.get("merge_commit_parallel"));
        Assert.assertEquals("true", streamLoadProps.get("merge_commit_async"));
        Assert.assertEquals("true", streamLoadProps.get("strip_outer_array"));
        Assert.assertEquals("true", streamLoadProps.get("ignore_json_size"));
    }

    @Test
    public void testAddDefaultStreamLoadPropertiesKeepsUserValues() {
        Map<String, String> streamLoadProps = new HashMap<>();
        streamLoadProps.put("enable_merge_commit", "true");
        streamLoadProps.put("merge_commit_interval_ms", "1000");
        streamLoadProps.put("merge_commit_parallel", "5");
        streamLoadProps.put("merge_commit_async", "FALSE");
        streamLoadProps.put("ignore_json_size", "false");
        MergeCommitOptions.addDefaultStreamLoadProperties(streamLoadProps, true);
        Assert.assertEquals("5", streamLoadProps.get("merge_commit_parallel"));
        Assert.assertEquals("false", streamLoadProps.get("merge_commit_async"));
        Assert.assertEquals("false", streamLoadProps.get("ignore_json_size"));
    }

    @Test
    public void testAddDefaultStreamLoadPropertiesForCsv() {
        Map<String, String> streamLoadProps = new HashMap<>();
        streamLoadProps.put("enable_merge_commit", "true");
        streamLoadProps.put("merge_commit_interval_ms", "1000");
        MergeCommitOptions.addDefaultStreamLoadProperties(streamLoadProps, false);
        Assert.assertFalse(streamLoadProps.containsKey("strip_outer_array"));
        Assert.assertFalse(streamLoadProps.containsKey("ignore_json_size"));
    }

    @Test
    public void testChunkSize() {
        Map<String, String> props = mergeCommitProps();
        Assert.assertEquals(MergeCommitOptions.DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER, MergeCommitOptions.getChunkSize(props));
        for (String inOrder : new String[] {"0", "1"}) {
            props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, inOrder);
            Assert.assertEquals(MergeCommitOptions.DEFAULT_CHUNK_SIZE_FOR_IN_ORDER, MergeCommitOptions.getChunkSize(props));
        }
        for (String outOfOrder : new String[] {"2", "-1"}) {
            props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, outOfOrder);
            Assert.assertEquals(MergeCommitOptions.DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER,
                    MergeCommitOptions.getChunkSize(props));
        }
        props.put(MergeCommitOptions.CHUNK_SIZE, "1048576");
        Assert.assertEquals(1048576L, MergeCommitOptions.getChunkSize(props));
        props.put(MergeCommitOptions.CHUNK_SIZE, "0");
        assertConfigException(() -> MergeCommitOptions.getChunkSize(props));
    }

    private static StreamLoadProperties.Builder newBuilder() {
        return StreamLoadProperties.builder()
                .loadUrls("127.0.0.1:8030")
                .username("root")
                .password("")
                .defaultTableProperties(StreamLoadTableProperties.builder().database("db").table("tbl").build());
    }

    @Test
    public void testConfigureDefaults() {
        StreamLoadProperties.Builder builder = newBuilder();
        MergeCommitOptions.configure(mergeCommitProps(), builder);
        StreamLoadProperties properties = builder.build();
        Assert.assertEquals(Integer.MAX_VALUE, properties.getMaxConcurrentRequests());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_HTTP_THREAD_NUM, properties.getHttpThreadNum());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_HTTP_MAX_CONNECTIONS, properties.getHttpMaxConnectionsPerRoute());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_HTTP_TOTAL_MAX_CONNECTIONS, properties.getHttpTotalMaxConnections());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_HTTP_IDLE_CONNECTION_TIMEOUT_MS,
                properties.getHttpIdleConnectionTimeoutMs());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_NODE_META_UPDATE_INTERVAL_MS,
                properties.getNodeMetaUpdateIntervalMs());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_CHECK_LABEL_STATE_INIT_DELAY_MS,
                properties.getCheckLabelStateInitDelayMs());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_CHECK_LABEL_STATE_INTERVAL_MS,
                properties.getCheckLabelStateIntervalMs());
        // The SDK then waits up to the stream load timeout.
        Assert.assertEquals(-1, properties.getCheckLabelStateTimeoutMs());
        Assert.assertFalse(properties.isBackendDirectConnection());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_MAX_RETRIES, properties.getMaxRetries());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_RETRY_INTERVAL_MS, properties.getRetryIntervalInMs());
        Assert.assertEquals(MergeCommitOptions.DEFAULT_PUBLISH_TIMEOUT_MS, properties.getPublishTimeoutMs());
    }

    @Test
    public void testConfigureUserValues() {
        Map<String, String> props = mergeCommitProps();
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "0");
        props.put(MergeCommitOptions.HTTP_THREAD_NUM, "4");
        props.put(MergeCommitOptions.HTTP_MAX_CONNECTIONS, "5");
        props.put(MergeCommitOptions.HTTP_TOTAL_MAX_CONNECTIONS, "6");
        props.put(MergeCommitOptions.HTTP_IDLE_CONNECTION_TIMEOUT_MS, "7000");
        props.put(MergeCommitOptions.NODE_META_UPDATE_INTERVAL_MS, "8000");
        props.put(MergeCommitOptions.CHECK_LABEL_STATE_INIT_DELAY_MS, "100");
        props.put(MergeCommitOptions.CHECK_LABEL_STATE_INTERVAL_MS, "200");
        props.put(MergeCommitOptions.CHECK_LABEL_STATE_TIMEOUT_MS, "30000");
        props.put(MergeCommitOptions.BACKEND_DIRECT_CONNECTION, "true");
        props.put(MergeCommitOptions.MAX_RETRIES, "2");
        props.put(MergeCommitOptions.RETRY_INTERVAL_MS, "500");
        props.put(SINK_PUBLISH_TIMEOUT_MS, "-1");
        // The task sets the publish timeout from sink.publish-timeout.ms before configure().
        StreamLoadProperties.Builder builder = newBuilder().setPublishTimeoutMs(-1);
        MergeCommitOptions.configure(props, builder);
        StreamLoadProperties properties = builder.build();
        Assert.assertEquals(0, properties.getMaxConcurrentRequests());
        Assert.assertEquals(4, properties.getHttpThreadNum());
        Assert.assertEquals(5, properties.getHttpMaxConnectionsPerRoute());
        Assert.assertEquals(6, properties.getHttpTotalMaxConnections());
        Assert.assertEquals(7000, properties.getHttpIdleConnectionTimeoutMs());
        Assert.assertEquals(8000, properties.getNodeMetaUpdateIntervalMs());
        Assert.assertEquals(100, properties.getCheckLabelStateInitDelayMs());
        Assert.assertEquals(200, properties.getCheckLabelStateIntervalMs());
        Assert.assertEquals(30000, properties.getCheckLabelStateTimeoutMs());
        Assert.assertTrue(properties.isBackendDirectConnection());
        Assert.assertEquals(2, properties.getMaxRetries());
        Assert.assertEquals(500, properties.getRetryIntervalInMs());
        Assert.assertEquals(-1, properties.getPublishTimeoutMs());
    }

    @Test
    public void testInvalidNumber() {
        Map<String, String> props = mergeCommitProps();
        props.put(MergeCommitOptions.HTTP_THREAD_NUM, "many");
        assertConfigException(() -> MergeCommitOptions.configure(props, newBuilder()));
    }
}
