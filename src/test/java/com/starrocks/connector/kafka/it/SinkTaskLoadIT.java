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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.starrocks.connector.kafka.StarRocksSinkTask;

// Loads that must work the same with and without merge commit.
@RunWith(Parameterized.class)
public class SinkTaskLoadIT extends StarRocksITBase {

    private static final Schema ROW_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("score", Schema.INT32_SCHEMA)
            .field("price", Decimal.schema(2))
            .field("dbl", Schema.FLOAT64_SCHEMA)
            .field("flag", Schema.BOOLEAN_SCHEMA)
            .build();

    private static final Schema SIMPLE_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("score", Schema.INT32_SCHEMA)
            .build();

    // Rows of a primary key table with the operation: 0 upserts and 1 deletes.
    private static final Schema OP_SCHEMA = SchemaBuilder.struct()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("score", Schema.INT32_SCHEMA)
            .field("__op", Schema.INT32_SCHEMA)
            .build();

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> modes() {
        return Arrays.asList(new Object[][] {{Mode.DEFAULT}, {Mode.MERGE_COMMIT}});
    }

    @Parameterized.Parameter
    public Mode mode;

    private static SinkRecord record(String topic, Schema schema, Object value, long offset) {
        return new SinkRecord(topic, 0, null, null, schema, value, offset);
    }

    private static Struct row(long id) {
        return new Struct(ROW_SCHEMA)
                .put("id", id)
                .put("name", "名字-" + id)
                .put("score", (int) (id % 100))
                .put("price", BigDecimal.valueOf(id % 1000, 2))
                .put("dbl", id * 0.5)
                .put("flag", id % 2 == 0);
    }

    private static Struct simpleRow(long id, String name, int score) {
        return new Struct(SIMPLE_SCHEMA).put("id", id).put("name", name).put("score", score);
    }

    private static Struct opRow(long id, int score, int op) {
        return new Struct(OP_SCHEMA).put("id", id).put("name", "n" + id).put("score", score).put("__op", op);
    }

    private static final String SIMPLE_DDL = "CREATE TABLE %s (id BIGINT, name VARCHAR(64), score INT) " +
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4";
    private static final String PK_DDL = "CREATE TABLE %s (id BIGINT, name VARCHAR(64), score INT) " +
            "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4";

    @Test
    public void testStructRecordsFromMultipleTasks() throws Exception {
        String table = createTable("struct", "CREATE TABLE %s (id BIGINT, name VARCHAR(64), score INT, " +
                "price DECIMAL(10, 2), dbl DOUBLE, flag BOOLEAN) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4");
        Map<String, String> props = props(mode, table, table);
        int numTasks = 3;
        int rowsPerTask = 10000;
        List<Callable<Void>> loaders = new ArrayList<>();
        for (int t = 0; t < numTasks; t++) {
            StarRocksSinkTask task = startTask(props);
            long first = (long) t * rowsPerTask;
            loaders.add(() -> {
                List<SinkRecord> records = new ArrayList<>();
                for (long id = first; id < first + rowsPerTask; id++) {
                    records.add(record(table, ROW_SCHEMA, row(id), id));
                }
                putInBatches(task, records, 500);
                flush(task, props);
                return null;
            });
        }
        runInParallel(loaders);

        long total = (long) numTasks * rowsPerTask;
        long expectedScore = 0;
        BigDecimal expectedPrice = BigDecimal.ZERO;
        for (long id = 0; id < total; id++) {
            expectedScore += id % 100;
            expectedPrice = expectedPrice.add(BigDecimal.valueOf(id % 1000, 2));
        }
        List<String> stats = query(String.format("SELECT COUNT(*), COUNT(DISTINCT id), SUM(score), SUM(price), " +
                "SUM(dbl), SUM(CASE WHEN flag THEN 1 ELSE 0 END) FROM %s.%s", database, table)).get(0);
        Assert.assertEquals(String.valueOf(total), stats.get(0));
        Assert.assertEquals(String.valueOf(total), stats.get(1));
        Assert.assertEquals(String.valueOf(expectedScore), stats.get(2));
        Assert.assertEquals(0, expectedPrice.compareTo(new BigDecimal(stats.get(3))));
        Assert.assertEquals(total * (total - 1) / 2 * 0.5, Double.parseDouble(stats.get(4)), 1e-6);
        Assert.assertEquals(String.valueOf(total / 2), stats.get(5));
        Assert.assertEquals(Arrays.asList("名字-12345", "3.45"),
                query(String.format("SELECT name, price FROM %s.%s WHERE id = 12345", database, table)).get(0));
    }

    @Test
    public void testSchemalessJson() throws Exception {
        String table = createTable("schemaless", SIMPLE_DDL);
        Map<String, String> props = props(mode, table, table);
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 5000; id++) {
            Map<String, Object> value = new HashMap<>();
            value.put("id", id);
            value.put("name", "n" + id);
            value.put("score", (int) (id % 10));
            records.add(record(table, null, value, id));
        }
        putInBatches(task, records, 500);
        flush(task, props);
        Assert.assertEquals(Arrays.asList("5000", "22500"),
                query(String.format("SELECT COUNT(*), SUM(score) FROM %s.%s", database, table)).get(0));
    }

    @Test
    public void testCsv() throws Exception {
        String table = createTable("csv", SIMPLE_DDL);
        Map<String, String> props = props(mode, table, table);
        props.put("sink.properties.format", "csv");
        props.put("sink.properties.column_separator", ",");
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 5000; id++) {
            records.add(record(table, Schema.STRING_SCHEMA, id + ",n" + id + "," + (id % 10), id));
        }
        putInBatches(task, records, 500);
        flush(task, props);
        Assert.assertEquals(Arrays.asList("5000", "22500"),
                query(String.format("SELECT COUNT(*), SUM(score) FROM %s.%s", database, table)).get(0));
    }

    @Test
    public void testMultipleTables() throws Exception {
        List<String> tables = new ArrayList<>();
        List<String> topicsAndTables = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String table = createTable("multi" + i, SIMPLE_DDL);
            tables.add(table);
            topicsAndTables.add("topic_" + table);
            topicsAndTables.add(table);
        }
        Map<String, String> props = props(mode, topicsAndTables.toArray(new String[0]));
        List<Callable<Void>> loaders = new ArrayList<>();
        for (int t = 0; t < 2; t++) {
            StarRocksSinkTask task = startTask(props);
            long first = t * 3000L;
            loaders.add(() -> {
                List<SinkRecord> records = new ArrayList<>();
                for (long id = first; id < first + 3000; id++) {
                    String topic = "topic_" + tables.get((int) (id % 3));
                    records.add(record(topic, SIMPLE_SCHEMA, simpleRow(id, "n" + id, 1), id));
                }
                putInBatches(task, records, 300);
                flush(task, props);
                return null;
            });
        }
        runInParallel(loaders);
        for (String table : tables) {
            Assert.assertEquals(table, 2000, count(table));
        }
    }

    @Test
    public void testPrimaryKeyUpsertAndDelete() throws Exception {
        String table = createTable("pk", PK_DDL);
        Map<String, String> props = props(mode, table, table);
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 1000; id++) {
            records.add(record(table, OP_SCHEMA, opRow(id, (int) id, 0), id));
        }
        putInBatches(task, records, 200);
        flush(task, props);

        records.clear();
        for (long id = 0; id < 500; id++) {
            records.add(record(table, OP_SCHEMA, opRow(id, (int) id + 1000, 0), 1000 + id));
        }
        for (long id = 500; id < 750; id++) {
            records.add(record(table, OP_SCHEMA, opRow(id, 0, 1), 1000 + id));
        }
        putInBatches(task, records, 200);
        flush(task, props);

        long expectedScore = 0;
        for (long id = 0; id < 500; id++) {
            expectedScore += id + 1000;
        }
        for (long id = 750; id < 1000; id++) {
            expectedScore += id;
        }
        Assert.assertEquals(Arrays.asList("750", String.valueOf(expectedScore)),
                query(String.format("SELECT COUNT(*), SUM(score) FROM %s.%s", database, table)).get(0));
    }

    // Tasks send updates of the same keys concurrently and out of order. merge_condition must keep the
    // newest version of every key, which is how merge commit keeps the order for primary key tables.
    @Test
    public void testMergeConditionKeepsNewestVersion() throws Exception {
        String table = createTable("cond", "CREATE TABLE %s (id BIGINT, version BIGINT, val VARCHAR(32)) " +
                "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4");
        Map<String, String> props = props(mode, table, table);
        props.put("sink.properties.merge_condition", "version");
        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("version", Schema.INT64_SCHEMA)
                .field("val", Schema.STRING_SCHEMA)
                .build();
        int numTasks = 4;
        int numKeys = 2000;
        // versions[t][key]: each key gets the versions 1..numTasks, shuffled over the tasks.
        long[][] versions = new long[numTasks][numKeys];
        Random random = new Random(42);
        for (int key = 0; key < numKeys; key++) {
            List<Long> keyVersions = new ArrayList<>();
            for (long v = 1; v <= numTasks; v++) {
                keyVersions.add(v);
            }
            Collections.shuffle(keyVersions, random);
            for (int t = 0; t < numTasks; t++) {
                versions[t][key] = keyVersions.get(t);
            }
        }
        List<Callable<Void>> loaders = new ArrayList<>();
        for (int t = 0; t < numTasks; t++) {
            StarRocksSinkTask task = startTask(props);
            long[] taskVersions = versions[t];
            loaders.add(() -> {
                List<SinkRecord> records = new ArrayList<>();
                for (int key = 0; key < numKeys; key++) {
                    Struct value = new Struct(schema).put("id", (long) key).put("version", taskVersions[key])
                            .put("val", "v" + taskVersions[key]);
                    records.add(record(table, schema, value, key));
                }
                putInBatches(task, records, 100);
                flush(task, props);
                return null;
            });
        }
        runInParallel(loaders);
        Assert.assertEquals(Arrays.asList(String.valueOf(numKeys), String.valueOf(numKeys)),
                query(String.format("SELECT COUNT(*), SUM(CASE WHEN version = %d AND val = 'v%d' THEN 1 ELSE 0 END) " +
                        "FROM %s.%s", numTasks, numTasks, database, table)).get(0));
    }

    @Test
    public void testPartialUpdate() throws Exception {
        String table = createTable("partial", "CREATE TABLE %s (id BIGINT, a INT, b INT, c INT) " +
                "PRIMARY KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4");
        Schema fullSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA).field("c", Schema.INT32_SCHEMA).build();
        Map<String, String> props = props(mode, table, table);
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 1000; id++) {
            records.add(record(table, fullSchema, new Struct(fullSchema).put("id", id).put("a", 1).put("b", 1)
                    .put("c", 1), id));
        }
        putInBatches(task, records, 500);
        flush(task, props);

        Schema partialSchema = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA)
                .field("b", Schema.INT32_SCHEMA).build();
        Map<String, String> partialProps = new HashMap<>(props);
        partialProps.put("sink.properties.partial_update", "true");
        partialProps.put("sink.properties.columns", "id,b");
        StarRocksSinkTask partialTask = startTask(partialProps);
        records.clear();
        for (long id = 0; id < 1000; id++) {
            records.add(record(table, partialSchema, new Struct(partialSchema).put("id", id).put("b", 2), id));
        }
        putInBatches(partialTask, records, 500);
        flush(partialTask, partialProps);
        Assert.assertEquals("1000", queryOne(String.format(
                "SELECT COUNT(*) FROM %s.%s WHERE a = 1 AND b = 2 AND c = 1", database, table)));
    }

    @Test
    public void testLz4Compression() throws Exception {
        String table = createTable("lz4", SIMPLE_DDL);
        Map<String, String> props = props(mode, table, table);
        props.put("sink.properties.compression", "lz4_frame");
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 5000; id++) {
            records.add(record(table, SIMPLE_SCHEMA, simpleRow(id, "n" + id, 1), id));
        }
        putInBatches(task, records, 500);
        flush(task, props);
        Assert.assertEquals(5000, count(table));
    }

    private static final String NOT_NULL_DDL = "CREATE TABLE %s (id BIGINT, name VARCHAR(4) NOT NULL, score INT) " +
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 4";

    // The name of one row is too long, so it becomes NULL and violates NOT NULL.
    private static List<SinkRecord> recordsWithOneBadRow(String table) {
        List<SinkRecord> records = new ArrayList<>();
        for (long id = 0; id < 10; id++) {
            String name = id == 5 ? "a name that is too long" : "ok";
            records.add(record(table, SIMPLE_SCHEMA, simpleRow(id, name, 1), id));
        }
        return records;
    }

    @Test
    public void testMaxFilterRatioToleratesBadRows() throws Exception {
        String table = createTable("filter", NOT_NULL_DDL);
        Map<String, String> props = props(mode, table, table);
        props.put("sink.properties.max_filter_ratio", "0.5");
        StarRocksSinkTask task = startTask(props);
        task.put(recordsWithOneBadRow(table));
        flush(task, props);
        Assert.assertEquals(9, count(table));
    }

    @Test
    public void testBadRowFailsTheLoad() throws Exception {
        String table = createTable("bad", NOT_NULL_DDL);
        Map<String, String> props = props(mode, table, table);
        props.put("sink.maxretries", "0");
        StarRocksSinkTask task = startTask(props);
        task.put(recordsWithOneBadRow(table));
        try {
            flush(task, props);
            Assert.fail("preCommit should fail");
        } catch (RuntimeException e) {
            // Kafka Connect rewinds to the committed offsets and replays the records.
            Assert.assertNotNull(rootCause(e).getMessage());
        }
        Assert.assertEquals(0, count(table));
        // With sink.maxretries=0, the next put fails the task.
        try {
            task.put(recordsWithOneBadRow(table));
            Assert.fail("put should fail after the retries are used up");
        } catch (RuntimeException e) {
            // expected
        }
    }

    // A task dies after its last offset commit, and a new task replays the records after that offset.
    // The rows of a primary key table end up exactly once.
    @Test
    public void testRestartReplay() throws Exception {
        String table = createTable("replay", PK_DDL);
        Map<String, String> props = props(mode, table, table);
        StarRocksSinkTask task = startTask(props);
        List<SinkRecord> committed = new ArrayList<>();
        List<SinkRecord> uncommitted = new ArrayList<>();
        for (long id = 0; id < 10000; id++) {
            (id < 5000 ? committed : uncommitted).add(record(table, OP_SCHEMA, opRow(id, (int) id, 0), id));
        }
        putInBatches(task, committed, 500);
        flush(task, props);
        putInBatches(task, uncommitted, 500);
        // Some of the uncommitted records may already be loaded by the background flushes.
        Thread.sleep(flushIntervalMs(props));
        task.stop();

        StarRocksSinkTask newTask = startTask(props);
        putInBatches(newTask, uncommitted, 500);
        flush(newTask, props);
        Assert.assertEquals(Arrays.asList("10000", String.valueOf(10000L * 9999 / 2)),
                query(String.format("SELECT COUNT(*), SUM(score) FROM %s.%s", database, table)).get(0));
    }
}
