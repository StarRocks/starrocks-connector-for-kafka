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

import com.starrocks.connector.kafka.json.JsonConverter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class StarRocksSinkTaskTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTaskTest.class);

    private static final String TOPIC = "test_topic";

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string().optional())
            .build();

    private SinkRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        return new SinkRecord("dummy-topic", 0, null, null, recordSchema, before, 0);
    }

    @Test
    public void testGetRecordFromSinkRecord() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = null;
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertNull(row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, null, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertNull(row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, "a,b,c", 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertEquals("a,b,c", row);
        }

        {
            class Dummy {
            }
            Dummy dummy = new Dummy();
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, dummy, 0);
            String errMsg = "";
            try {
                sinkTask.getRecordFromSinkRecord(sinkRecord);
            } catch (DataException e) {
                errMsg = e.getMessage();
            }
            assertTrue(errMsg.contains("cannot be cast to"));
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = null;
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertNull(row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, null, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertNull(row);
        }

        {
            JsonConverter jsonConverter = new JsonConverter();
            sinkTask.setJsonConverter(jsonConverter);
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            String value = "{\"name\":\"北京\",\"code\":1}";
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, value, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertEquals("\"{\\\"name\\\":\\\"北京\\\",\\\"code\\\":1}\"", row);
        }

        {
            JsonConverter jsonConverter = new JsonConverter();
            sinkTask.setJsonConverter(jsonConverter);
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = createCreateRecord();
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            assertEquals("{\"id\":1,\"name\":\"myRecord\"}", row);
        }
    }

    @Test
    public void testDecimalRecord() {
        // Payload is base64 encoded byte[]{0, -100}, which is the two's complement encoding of 156 with scale=2 → 1.56
        Schema schema = Decimal.schema(2);
        BigDecimal decimal = Decimal.toLogical(schema, Base64.getDecoder().decode("AJw="));
        SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, schema, decimal, 0);
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        JsonConverter jsonConverter = new JsonConverter();
        sinkTask.setJsonConverter(jsonConverter);
        sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
        String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
        assertEquals("1.56", row);
    }

    @Test
    public void testNullRecord() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        JsonConverter jsonConverter = new JsonConverter();
        sinkTask.setJsonConverter(jsonConverter);
        sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
        final Struct value = new Struct(recordSchema);
        value.put("id", (byte) 1);
        value.put("name", null);
        SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, recordSchema, value, 0);
        String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
        assertEquals("{\"id\":1,\"name\":null}", row);
    }
}
