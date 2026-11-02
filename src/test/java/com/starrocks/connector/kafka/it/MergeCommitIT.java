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

package com.starrocks.connector.kafka.it;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import com.starrocks.connector.kafka.MergeCommitOptions;
import com.starrocks.connector.kafka.StarRocksSinkTask;

// Behaviors that only apply to merge commit.
public class MergeCommitIT extends StarRocksITBase {

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("score", Schema.INT32_SCHEMA)
            .build();

    private static final String DDL = "CREATE TABLE %s (id BIGINT, name VARCHAR(64), score INT) " +
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4";

    private static SinkRecord record(String topic, long id) {
        Struct value = new Struct(SCHEMA).put("id", id).put("name", "n" + id).put("score", (int) (id % 10));
        return new SinkRecord(topic, 0, null, null, SCHEMA, value, id);
    }

    private static List<SinkRecord> records(String topic, long first, long count) {
        List<SinkRecord> records = new ArrayList<>();
        for (long id = first; id < first + count; id++) {
            records.add(record(topic, id));
        }
        return records;
    }

    private static List<String> mergeCommitLabels(String table) throws Exception {
        List<String> labels = new ArrayList<>();
        for (List<String> row : query(String.format("SELECT LABEL, TYPE FROM information_schema.loads " +
                "WHERE DB_NAME = '%s' AND TABLE_NAME = '%s'", database, table))) {
            Assert.assertEquals(row.get(0), "MERGE_COMMIT", row.get(1));
            labels.add(row.get(0));
        }
        return labels;
    }

    // Several tasks keep sending small requests to the same table. StarRocks merges them into about one
    // transaction per merge window, instead of one per request.
    @Test
    public void testTasksShareMergedTransactions() throws Exception {
        String table = createTable("merged", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        int numTasks = 4;
        long durationMs = 5000;
        List<Callable<Long>> loaders = new ArrayList<>();
        for (int t = 0; t < numTasks; t++) {
            StarRocksSinkTask task = startTask(props);
            long first = t * 1_000_000L;
            loaders.add(() -> {
                long next = first;
                long start = System.currentTimeMillis();
                while (System.currentTimeMillis() - start < durationMs) {
                    task.put(records(table, next, 100));
                    next += 100;
                    Thread.sleep(50);
                }
                flush(task, props);
                return next - first;
            });
        }
        long total = 0;
        for (long loaded : runInParallel(loaders)) {
            total += loaded;
        }
        Assert.assertEquals(total, count(table));
        // Each task flushes every 200 ms, so without merge commit this would be about 100 transactions.
        List<String> labels = mergeCommitLabels(table);
        Assert.assertTrue("merged transactions: " + labels.size(), labels.size() <= durationMs / 1000 + 4);
    }

    @Test
    public void testSyncMode() throws Exception {
        String table = createTable("sync", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        props.put("sink.properties.merge_commit_async", "false");
        List<Callable<Void>> loaders = new ArrayList<>();
        for (int t = 0; t < 2; t++) {
            StarRocksSinkTask task = startTask(props);
            long first = t * 10000L;
            loaders.add(() -> {
                putInBatches(task, records(table, first, 2000), 500);
                flush(task, props);
                return null;
            });
        }
        runInParallel(loaders);
        Assert.assertEquals(4000, count(table));
        Assert.assertFalse(mergeCommitLabels(table).isEmpty());
    }

    @Test
    public void testBackendDirectConnection() throws Exception {
        String table = createTable("direct", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        props.put(MergeCommitOptions.BACKEND_DIRECT_CONNECTION, "true");
        StarRocksSinkTask task = startTask(props);
        putInBatches(task, records(table, 0, 5000), 500);
        flush(task, props);
        Assert.assertEquals(5000, count(table));
        Assert.assertFalse(mergeCommitLabels(table).isEmpty());
    }

    // With a 100 ms flush interval and a 500 ms merge window, a row becomes visible well within a few seconds
    // without any preCommit.
    @Test
    public void testSubSecondFlushInterval() throws Exception {
        String table = createTable("fast", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        props.put("bufferflush.intervalms", "100");
        props.put("sink.properties.merge_commit_interval_ms", "500");
        StarRocksSinkTask task = startTask(props);
        for (long id = 0; id < 5; id++) {
            long start = System.currentTimeMillis();
            task.put(Collections.singletonList(record(table, id)));
            while (count(table) < id + 1) {
                Assert.assertTrue("row " + id + " is not visible", System.currentTimeMillis() - start < 5000);
                Thread.sleep(20);
            }
        }
        flush(task, props);
        Assert.assertEquals(5, count(table));
    }

    @Test
    public void testAnotherEndpointInTheSameWorkerIsRejected() throws Exception {
        String table = createTable("guard", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        startTask(props);
        Map<String, String> other = props(Mode.MERGE_COMMIT, table, table);
        other.put("starrocks.http.url", "127.0.0.2:" + HTTP_PORT);
        try {
            startTask(other);
            Assert.fail("A task with another endpoint should not start");
        } catch (ConfigException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("127.0.0.2"));
        }
    }

    // A request limit needs an SDK with starrocks-connector-for-apache-flink#528. Without it, the task does not
    // start. With it, the task sends the requests of a table one at a time and every row arrives.
    @Test
    public void testRequestLimit() throws Exception {
        String table = createTable("limit", DDL);
        Map<String, String> props = props(Mode.MERGE_COMMIT, table, table);
        props.put(MergeCommitOptions.MAX_CONCURRENT_REQUESTS, "1");
        if (!MergeCommitOptions.sdkKeepsDataAtRequestLimit()) {
            try {
                startTask(props);
                Assert.fail("A request limit should be rejected without the SDK fix");
            } catch (ConfigException e) {
                Assert.assertTrue(e.getMessage(), e.getMessage().contains(MergeCommitOptions.MAX_CONCURRENT_REQUESTS));
            }
            return;
        }
        StarRocksSinkTask task = startTask(props);
        // Keep putting while a request is in flight, so that the flush timer fires while the table is at its
        // limit, which is when an SDK without the fix drops the chunk.
        for (long first = 0; first < 20000; first += 500) {
            task.put(records(table, first, 500));
            Thread.sleep(100);
        }
        flush(task, props);
        Assert.assertEquals(20000, count(table));
        Assert.assertFalse(mergeCommitLabels(table).isEmpty());
    }
}
