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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.starrocks.data.load.stream.mergecommit.LoadParameters;
import com.starrocks.data.load.stream.mergecommit.MergeCommitManager;
import com.starrocks.data.load.stream.mergecommit.Table;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;

import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.SINK_PUBLISH_TIMEOUT_MS;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.getBoolean;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.getInt;
import static com.starrocks.connector.kafka.StarRocksSinkConnectorConfig.getLong;

// Merge commit (StarRocks 3.4 and later) merges the stream load requests that all tasks send to a table
// with the same stream load properties within a time window into one transaction, so the number of
// transactions no longer grows with the number of tasks and the flush frequency. It is enabled by setting
// sink.properties.enable_merge_commit to true. The options and defaults follow the Flink connector.
public class MergeCommitOptions {

    // Stream load properties, set with the prefix sink.properties., that control merge commit in StarRocks.
    public static final String ENABLE_MERGE_COMMIT = LoadParameters.ENABLE_MERGE_COMMIT;
    // The merge window in milliseconds. Required when merge commit is enabled.
    public static final String MERGE_COMMIT_INTERVAL_MS = LoadParameters.MERGE_COMMIT_INTERVAL_MS;
    // The parallelism of the load plan of each merged transaction.
    public static final String MERGE_COMMIT_PARALLEL = LoadParameters.MERGE_COMMIT_PARALLEL;
    // Whether StarRocks returns once it receives the data (true) or once the merged transaction is visible (false).
    public static final String MERGE_COMMIT_ASYNC = LoadParameters.MERGE_COMMIT_ASYNC;

    public static final String PREFIX = "sink.merge-commit.";
    // The maximum bytes of a chunk, which is the data of a table sent in one stream load request.
    public static final String CHUNK_SIZE = PREFIX + "chunk.size";
    // The maximum number of inflight stream load requests of a table in a task. 0 or 1 sends one request
    // at a time, which keeps the order of the records. A negative value means no limit.
    public static final String MAX_CONCURRENT_REQUESTS = PREFIX + "max-concurrent-requests";
    public static final String HTTP_THREAD_NUM = PREFIX + "http.thread.num";
    public static final String HTTP_MAX_CONNECTIONS = PREFIX + "http.max-connections";
    public static final String HTTP_TOTAL_MAX_CONNECTIONS = PREFIX + "http.total-max-connections";
    public static final String HTTP_IDLE_CONNECTION_TIMEOUT_MS = PREFIX + "http.idle-connection-timeout-ms";
    public static final String NODE_META_UPDATE_INTERVAL_MS = PREFIX + "node-meta.update-interval-ms";
    public static final String CHECK_LABEL_STATE_INIT_DELAY_MS = PREFIX + "check-label-state.init-delay-ms";
    public static final String CHECK_LABEL_STATE_INTERVAL_MS = PREFIX + "check-label-state.interval-ms";
    // How long to wait for a merged transaction to become visible. The stream load timeout if not set.
    public static final String CHECK_LABEL_STATE_TIMEOUT_MS = PREFIX + "check-label-state.timeout-ms";
    // Send the requests to the BE or CN nodes that StarRocks assigns to the table instead of an FE node,
    // which redirects every request through the leader FE.
    public static final String BACKEND_DIRECT_CONNECTION = PREFIX + "backend-direct-connection";
    // How many times a failed request is sent again before the flush fails, and how long to wait before each
    // time. The stream load SDK adds up to 2 seconds to the wait at random.
    public static final String MAX_RETRIES = PREFIX + "max-retries";
    public static final String RETRY_INTERVAL_MS = PREFIX + "retry-interval-ms";

    public static final String DEFAULT_MERGE_COMMIT_PARALLEL = "3";
    public static final String DEFAULT_MERGE_COMMIT_ASYNC = "true";
    public static final int DEFAULT_MAX_CONCURRENT_REQUESTS = Integer.MAX_VALUE;
    public static final long DEFAULT_CHUNK_SIZE_FOR_IN_ORDER = 500 * 1024 * 1024L;
    public static final long DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER = 20 * 1024 * 1024L;
    public static final int DEFAULT_HTTP_THREAD_NUM = 3;
    public static final int DEFAULT_HTTP_MAX_CONNECTIONS = 20;
    public static final int DEFAULT_HTTP_TOTAL_MAX_CONNECTIONS = 1000;
    public static final int DEFAULT_HTTP_IDLE_CONNECTION_TIMEOUT_MS = 60000;
    public static final int DEFAULT_NODE_META_UPDATE_INTERVAL_MS = 5000;
    public static final int DEFAULT_CHECK_LABEL_STATE_INIT_DELAY_MS = 500;
    public static final int DEFAULT_CHECK_LABEL_STATE_INTERVAL_MS = 500;
    public static final int DEFAULT_PUBLISH_TIMEOUT_MS = 10000;
    public static final int DEFAULT_MAX_RETRIES = 10;
    public static final int DEFAULT_RETRY_INTERVAL_MS = 1000;

    // Without merge commit, each flush of each task is a transaction, so bufferflush.intervalms stays
    // at least 1 second. With merge commit, a smaller interval only adds requests to the merge window.
    public static final long MIN_BUFFERFLUSH_INTERVAL_MS = 100;
    public static final long MIN_BUFFERFLUSH_INTERVAL_MS_WITHOUT_MERGE_COMMIT = 1000;

    // The stream load SDK before starrocks-connector-for-apache-flink 3ad9f84 silently drops a chunk when a table's
    // flush timer fires while the table is at its request limit, which is the common case with a limit. The fix
    // (starrocks-connector-for-apache-flink#528) added MergeCommitManager.onTableFailure, so a limit is only
    // accepted with an SDK that has it.
    private static final boolean SDK_KEEPS_DATA_AT_REQUEST_LIMIT = hasRequestLimitFix(MergeCommitManager.class);

    public static boolean isEnabled(Map<String, String> streamLoadProps) {
        return "true".equalsIgnoreCase(streamLoadProps.get(ENABLE_MERGE_COMMIT));
    }

    // Returns the errors of the settings that merge commit depends on, keyed by the connector option.
    public static Map<String, String> validate(Map<String, String> props) {
        return validate(props, SDK_KEEPS_DATA_AT_REQUEST_LIMIT);
    }

    static Map<String, String> validate(Map<String, String> props, boolean sdkKeepsDataAtRequestLimit) {
        Map<String, String> errors = new LinkedHashMap<>();
        Map<String, String> streamLoadProps = StarRocksSinkConnectorConfig.getStreamLoadProperties(props);
        boolean enabled = isEnabled(streamLoadProps);
        Long flushIntervalMs = parseLongOrNull(props.get(BUFFERFLUSH_INTERVALMS));
        if (!enabled && flushIntervalMs != null && flushIntervalMs < MIN_BUFFERFLUSH_INTERVAL_MS_WITHOUT_MERGE_COMMIT) {
            errors.put(BUFFERFLUSH_INTERVALMS, String.format("must be at least %d unless merge commit is enabled " +
                    "with %s%s=true", MIN_BUFFERFLUSH_INTERVAL_MS_WITHOUT_MERGE_COMMIT, SINK_PROPERTIES_PREFIX,
                    ENABLE_MERGE_COMMIT));
        }
        if (!enabled) {
            return errors;
        }
        String intervalMs = streamLoadProps.get(MERGE_COMMIT_INTERVAL_MS);
        if (intervalMs == null) {
            errors.put(SINK_PROPERTIES_PREFIX + MERGE_COMMIT_INTERVAL_MS, "must be set when merge commit is enabled");
        } else if (!isPositiveInt(intervalMs)) {
            errors.put(SINK_PROPERTIES_PREFIX + MERGE_COMMIT_INTERVAL_MS,
                    "must be a positive integer, but is " + intervalMs);
        }
        String parallel = streamLoadProps.get(MERGE_COMMIT_PARALLEL);
        if (parallel != null && !isPositiveInt(parallel)) {
            errors.put(SINK_PROPERTIES_PREFIX + MERGE_COMMIT_PARALLEL, "must be a positive integer, but is " + parallel);
        }
        String async = streamLoadProps.get(MERGE_COMMIT_ASYNC);
        if (async != null && !"true".equalsIgnoreCase(async) && !"false".equalsIgnoreCase(async)) {
            errors.put(SINK_PROPERTIES_PREFIX + MERGE_COMMIT_ASYNC, "must be true or false, but is " + async);
        }
        Long maxConcurrentRequests = parseLongOrNull(props.get(MAX_CONCURRENT_REQUESTS));
        if (maxConcurrentRequests != null && maxConcurrentRequests >= 0
                && maxConcurrentRequests != DEFAULT_MAX_CONCURRENT_REQUESTS && !sdkKeepsDataAtRequestLimit) {
            errors.put(MAX_CONCURRENT_REQUESTS, "a limit needs the stream load SDK of " +
                    "starrocks-connector-for-apache-flink 3ad9f84 or later, because an older SDK can drop data " +
                    "when a table reaches it. Build the connector with a newer SDK, or leave this unset and set " +
                    SINK_PROPERTIES_PREFIX + "merge_condition to keep the order of the updates to a primary key table");
        }
        return errors;
    }

    // Throws the first error that validate() finds.
    public static void check(Map<String, String> props) {
        Map<String, String> errors = validate(props);
        if (!errors.isEmpty()) {
            Map.Entry<String, String> error = errors.entrySet().iterator().next();
            String value = props.get(error.getKey());
            throw new ConfigException(error.getKey(), value, error.getValue());
        }
    }

    // Adds the stream load properties that merge commit needs when the user does not set them.
    public static void addDefaultStreamLoadProperties(Map<String, String> streamLoadProps, boolean jsonFormat) {
        // The values are lowercased because the BE only takes "true" for merge_commit_async.
        streamLoadProps.put(ENABLE_MERGE_COMMIT, "true");
        String async = streamLoadProps.get(MERGE_COMMIT_ASYNC);
        streamLoadProps.put(MERGE_COMMIT_ASYNC, async == null ? DEFAULT_MERGE_COMMIT_ASYNC : async.trim().toLowerCase());
        streamLoadProps.putIfAbsent(MERGE_COMMIT_PARALLEL, DEFAULT_MERGE_COMMIT_PARALLEL);
        if (jsonFormat) {
            // A chunk is sent as one JSON array. A merge commit request always carries its length, so without
            // ignore_json_size StarRocks rejects JSON bodies above streaming_load_max_batch_size_mb (100MB).
            streamLoadProps.putIfAbsent("strip_outer_array", "true");
            streamLoadProps.putIfAbsent("ignore_json_size", "true");
        }
    }

    public static int getMaxConcurrentRequests(Map<String, String> props) {
        return getInt(props, MAX_CONCURRENT_REQUESTS, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    // The SDK sends one request at a time for both 0 and 1, which keeps the order of the records.
    public static boolean isInOrder(int maxConcurrentRequests) {
        return maxConcurrentRequests == 0 || maxConcurrentRequests == 1;
    }

    public static long getChunkSize(Map<String, String> props) {
        if (props.containsKey(CHUNK_SIZE)) {
            long chunkSize = getLong(props, CHUNK_SIZE, 0L);
            if (chunkSize <= 0) {
                throw new ConfigException(CHUNK_SIZE, props.get(CHUNK_SIZE), "must be positive");
            }
            return chunkSize;
        }
        // An in-order table has one request at a time, so a larger chunk keeps the throughput.
        return isInOrder(getMaxConcurrentRequests(props))
                ? DEFAULT_CHUNK_SIZE_FOR_IN_ORDER : DEFAULT_CHUNK_SIZE_FOR_OUT_OF_ORDER;
    }

    public static void configure(Map<String, String> props, StreamLoadProperties.Builder builder) {
        builder.setMaxConcurrentRequests(getMaxConcurrentRequests(props))
                .setHttpThreadNum(getInt(props, HTTP_THREAD_NUM, DEFAULT_HTTP_THREAD_NUM))
                .setHttpMaxConnectionsPerRoute(getInt(props, HTTP_MAX_CONNECTIONS, DEFAULT_HTTP_MAX_CONNECTIONS))
                .setHttpTotalMaxConnections(getInt(props, HTTP_TOTAL_MAX_CONNECTIONS,
                        DEFAULT_HTTP_TOTAL_MAX_CONNECTIONS))
                .setHttpIdleConnectionTimeoutMs(getInt(props, HTTP_IDLE_CONNECTION_TIMEOUT_MS,
                        DEFAULT_HTTP_IDLE_CONNECTION_TIMEOUT_MS))
                .setNodeMetaUpdateIntervalMs(getInt(props, NODE_META_UPDATE_INTERVAL_MS,
                        DEFAULT_NODE_META_UPDATE_INTERVAL_MS))
                .setCheckLabelStateInitDelayMs(getInt(props, CHECK_LABEL_STATE_INIT_DELAY_MS,
                        DEFAULT_CHECK_LABEL_STATE_INIT_DELAY_MS))
                .setCheckLabelStateIntervalMs(getInt(props, CHECK_LABEL_STATE_INTERVAL_MS,
                        DEFAULT_CHECK_LABEL_STATE_INTERVAL_MS))
                .setBackendDirectConnection(getBoolean(props, BACKEND_DIRECT_CONNECTION, false))
                // The SDK sends each request to an FE it picks at random, without skipping one that is down,
                // and a failed flush makes every task replay from its committed offsets. Sending a failed request
                // again, to another FE, rides out an FE failover and the time StarRocks takes to notice a CN
                // that died: the default retries span 10 to 30 seconds.
                .maxRetries(getInt(props, MAX_RETRIES, DEFAULT_MAX_RETRIES))
                .retryIntervalInMs(getInt(props, RETRY_INTERVAL_MS, DEFAULT_RETRY_INTERVAL_MS));
        if (props.containsKey(CHECK_LABEL_STATE_TIMEOUT_MS)) {
            builder.setCheckLabelStateTimeoutMs(getInt(props, CHECK_LABEL_STATE_TIMEOUT_MS, -1));
        }
        if (!props.containsKey(SINK_PUBLISH_TIMEOUT_MS)) {
            builder.setPublishTimeoutMs(DEFAULT_PUBLISH_TIMEOUT_MS);
        }
    }

    static boolean hasRequestLimitFix(Class<?> mergeCommitManager) {
        try {
            mergeCommitManager.getMethod("onTableFailure", Table.class, Throwable.class);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    // Whether the stream load SDK that the connector runs with has the fix, so that a request limit is accepted.
    public static boolean sdkKeepsDataAtRequestLimit() {
        return SDK_KEEPS_DATA_AT_REQUEST_LIMIT;
    }

    private static boolean isPositiveInt(String value) {
        try {
            return Integer.parseInt(value.trim()) > 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static Long parseLongOrNull(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            // ConfigDef reports a value that is not a number.
            return null;
        }
    }
}
