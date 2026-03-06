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

package com.starrocks.connector.kafka.transforms;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MicroTimestampConverterTest {

    private SinkRecord recordWithMap(Map<String, Object> value) {
        return new SinkRecord("topic", 0, null, null, null, value, 0);
    }

    private MicroTimestampConverter<SinkRecord> configure(String fields) {
        MicroTimestampConverter<SinkRecord> smt = new MicroTimestampConverter<>();
        Map<String, String> props = new HashMap<>();
        props.put(MicroTimestampConverter.FIELDS_CONFIG, fields);
        smt.configure(props);
        return smt;
    }

    @Test
    public void convertsKnownDatetimeField() {
        // 1771224496677000 µs = 2026-02-16 06:48:16 UTC
        Map<String, Object> value = new HashMap<>();
        value.put("id", 1L);
        value.put("createdAt", 1771224496677000L);
        value.put("name", "test");

        MicroTimestampConverter<SinkRecord> smt = configure("createdAt");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-02-16 06:48:16", resultValue.get("createdAt"));
        Assert.assertEquals(1L, resultValue.get("id"));
        Assert.assertEquals("test", resultValue.get("name"));
    }

    @Test
    public void convertsMultipleFields() {
        Map<String, Object> value = new HashMap<>();
        value.put("createdAt", 1771224496677000L);
        value.put("updatedAt", 1772786921013000L);
        value.put("preparedAt", (Object) null);  // nullable — must stay null

        MicroTimestampConverter<SinkRecord> smt = configure("createdAt,updatedAt,preparedAt");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertNotNull(resultValue.get("createdAt"));
        Assert.assertTrue(resultValue.get("createdAt") instanceof String);
        Assert.assertNotNull(resultValue.get("updatedAt"));
        Assert.assertTrue(resultValue.get("updatedAt") instanceof String);
        Assert.assertNull(resultValue.get("preparedAt"));  // null stays null
    }

    @Test
    public void doesNotModifyStringFields() {
        // Already-converted fields (e.g. TIMESTAMP type from Fahras) must pass through unchanged
        Map<String, Object> value = new HashMap<>();
        value.put("created_at", "2026-02-16T06:48:04Z");

        MicroTimestampConverter<SinkRecord> smt = configure("created_at");
        SinkRecord result = smt.apply(recordWithMap(value));

        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-02-16T06:48:04Z", resultValue.get("created_at"));
    }

    @Test
    public void passesThroughNonMapRecord() {
        MicroTimestampConverter<SinkRecord> smt = configure("createdAt");
        SinkRecord record = new SinkRecord("topic", 0, null, null, null, "plain-string", 0);
        SinkRecord result = smt.apply(record);
        Assert.assertSame(record, result);
    }

    @Test
    public void returnsOriginalRecordWhenNoFieldsMatch() {
        Map<String, Object> value = new HashMap<>();
        value.put("id", 42L);

        MicroTimestampConverter<SinkRecord> smt = configure("createdAt");
        SinkRecord record = recordWithMap(value);
        SinkRecord result = smt.apply(record);
        // No conversion → same record instance returned
        Assert.assertSame(record, result);
    }

    @Test
    public void customFormat() {
        Map<String, Object> value = new HashMap<>();
        value.put("createdAt", 1771224496677000L);

        MicroTimestampConverter<SinkRecord> smt = new MicroTimestampConverter<>();
        Map<String, String> props = new HashMap<>();
        props.put(MicroTimestampConverter.FIELDS_CONFIG, "createdAt");
        props.put(MicroTimestampConverter.FORMAT_CONFIG, "yyyy-MM-dd'T'HH:mm:ss");
        smt.configure(props);

        SinkRecord result = smt.apply(recordWithMap(value));
        Map<?, ?> resultValue = (Map<?, ?>) result.value();
        Assert.assertEquals("2026-02-16T06:48:16", resultValue.get("createdAt"));
    }
}
