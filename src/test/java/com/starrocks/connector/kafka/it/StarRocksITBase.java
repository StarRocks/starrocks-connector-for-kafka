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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;

import com.starrocks.connector.kafka.StarRocksSinkTask;

// Base of the integration tests, which drive StarRocksSinkTask against a real StarRocks cluster the way
// Kafka Connect does: put() batches of records, then preCommit() to flush them before offsets are committed.
// They run only when STARROCKS_IT_FE is set, for example:
//   STARROCKS_IT_FE=<fe host> mvn test -Dtest='*IT'
// Optional: STARROCKS_IT_HTTP_PORT (8030), STARROCKS_IT_QUERY_PORT (9030), STARROCKS_IT_USER (root),
// STARROCKS_IT_PASSWORD (empty), STARROCKS_IT_KEEP_DATABASE (false).
public abstract class StarRocksITBase {

    protected static final String FE_HOST = System.getenv("STARROCKS_IT_FE");
    protected static final int HTTP_PORT = Integer.parseInt(env("STARROCKS_IT_HTTP_PORT", "8030"));
    protected static final int QUERY_PORT = Integer.parseInt(env("STARROCKS_IT_QUERY_PORT", "9030"));
    protected static final String USER = env("STARROCKS_IT_USER", "root");
    protected static final String PASSWORD = env("STARROCKS_IT_PASSWORD", "");

    // The flush intervals the tests use. preCommit only flushes once this much time has passed since
    // the previous flush.
    protected static final long FLUSH_INTERVAL_MS = 1000;
    protected static final long MERGE_COMMIT_FLUSH_INTERVAL_MS = 200;
    protected static final String MERGE_COMMIT_WINDOW_MS = "1000";

    protected static String database;
    private static Connection connection;

    private final List<StarRocksSinkTask> tasks = new ArrayList<>();

    protected enum Mode {
        DEFAULT,
        MERGE_COMMIT
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    @BeforeClass
    public static void setUpDatabase() throws Exception {
        Assume.assumeTrue("STARROCKS_IT_FE is not set", FE_HOST != null && !FE_HOST.isEmpty());
        PropertyConfigurator.configure("src/test/conf/log4j-it.properties");
        connection = DriverManager.getConnection(
                String.format("jdbc:mysql://%s:%d/?useSSL=false&allowPublicKeyRetrieval=true", FE_HOST, QUERY_PORT),
                USER, PASSWORD);
        database = "kafka_it_" + System.currentTimeMillis();
        execute("CREATE DATABASE " + database);
    }

    @AfterClass
    public static void tearDownDatabase() throws Exception {
        if (connection == null) {
            return;
        }
        if (!Boolean.parseBoolean(env("STARROCKS_IT_KEEP_DATABASE", "false"))) {
            execute("DROP DATABASE IF EXISTS " + database + " FORCE");
        }
        connection.close();
        connection = null;
    }

    @After
    public void stopTasks() {
        for (StarRocksSinkTask task : tasks) {
            try {
                task.stop();
            } catch (Exception e) {
                // The test already failed or stopped the task.
            }
        }
        tasks.clear();
    }

    protected static void execute(String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    protected static List<List<String>> query(String sql) throws Exception {
        List<List<String>> rows = new ArrayList<>();
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            while (resultSet.next()) {
                List<String> row = new ArrayList<>();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    row.add(resultSet.getString(i));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    protected static String queryOne(String sql) throws Exception {
        List<List<String>> rows = query(sql);
        Assert.assertEquals(sql, 1, rows.size());
        return rows.get(0).get(0);
    }

    protected static long count(String table) throws Exception {
        return Long.parseLong(queryOne("SELECT COUNT(*) FROM " + database + "." + table));
    }

    // Returns a unique table name and creates the table from the DDL, where %s is the qualified name.
    protected static String createTable(String prefix, String ddl) throws Exception {
        String table = prefix + "_" + Long.toHexString(System.nanoTime());
        execute(String.format(ddl, database + "." + table));
        return table;
    }

    protected static Map<String, String> props(Mode mode, String... topicsAndTables) {
        Map<String, String> props = new HashMap<>();
        props.put("starrocks.http.url", FE_HOST + ":" + HTTP_PORT);
        props.put("starrocks.database.name", database);
        props.put("starrocks.username", USER);
        props.put("starrocks.password", PASSWORD);
        props.put("sink.properties.strip_outer_array", "true");
        StringBuilder topic2Table = new StringBuilder();
        for (int i = 0; i < topicsAndTables.length; i += 2) {
            if (topic2Table.length() > 0) {
                topic2Table.append(',');
            }
            topic2Table.append(topicsAndTables[i]).append(':').append(topicsAndTables[i + 1]);
        }
        props.put("starrocks.topic2table.map", topic2Table.toString());
        if (mode == Mode.MERGE_COMMIT) {
            props.put("sink.properties.enable_merge_commit", "true");
            props.put("sink.properties.merge_commit_interval_ms", MERGE_COMMIT_WINDOW_MS);
            props.put("bufferflush.intervalms", String.valueOf(MERGE_COMMIT_FLUSH_INTERVAL_MS));
        } else {
            props.put("bufferflush.intervalms", String.valueOf(FLUSH_INTERVAL_MS));
        }
        return props;
    }

    protected static long flushIntervalMs(Map<String, String> props) {
        return Long.parseLong(props.get("bufferflush.intervalms"));
    }

    protected StarRocksSinkTask startTask(Map<String, String> props) {
        StarRocksSinkTask task = new StarRocksSinkTask();
        // The task adds defaults to the map, so give each task its own copy.
        task.start(new HashMap<>(props));
        tasks.add(task);
        return task;
    }

    // Flushes the records put so far and waits until they are loaded, like preCommit before an offset commit.
    protected static void flush(StarRocksSinkTask task, Map<String, String> props) throws Exception {
        Thread.sleep(flushIntervalMs(props) + 100);
        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(new TopicPartition("it", 0), new OffsetAndMetadata(1));
        Assert.assertEquals("preCommit did not flush", offsets, task.preCommit(offsets));
    }

    protected static void putInBatches(StarRocksSinkTask task, List<SinkRecord> records, int batchSize) {
        for (int i = 0; i < records.size(); i += batchSize) {
            task.put(new ArrayList<>(records.subList(i, Math.min(records.size(), i + batchSize))));
        }
    }

    // Runs the callables in parallel and rethrows the first failure.
    protected static <T> List<T> runInParallel(List<Callable<T>> callables) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(callables.size());
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (Callable<T> callable : callables) {
                futures.add(executor.submit(callable));
            }
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } finally {
            executor.shutdownNow();
        }
    }

    protected static Throwable rootCause(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root;
    }
}
