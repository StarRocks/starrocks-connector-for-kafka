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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TimeMillisToStringTransformTest {

    private static Date timeMillis(int millis) {
        return new Date(millis);
    }

    private Schema buildSchemaWithTimeFields() {
        return SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("TMCTDR", Time.builder().optional().build())
                .field("TMLDDR", Time.builder().optional().build())
                .field("amount", Schema.FLOAT64_SCHEMA)
                .build();
    }

    private SinkRecord createRecord(Schema schema, Struct value) {
        return new SinkRecord("test-topic", 0, null, null, schema, value, 0);
    }

    @Test
    public void testAutoDetectTimeMillisFields() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = buildSchemaWithTimeFields();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put("TMCTDR", timeMillis(64957000));
        value.put("TMLDDR", timeMillis(64957000));
        value.put("amount", 100.0);

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Struct resultValue = (Struct) result.value();
        Assert.assertEquals("18:02:37", resultValue.get("TMCTDR"));
        Assert.assertEquals("18:02:37", resultValue.get("TMLDDR"));
        Assert.assertEquals(1, resultValue.get("id"));
        Assert.assertEquals("test", resultValue.get("name"));
        Assert.assertEquals(100.0, resultValue.get("amount"));

        Assert.assertEquals(Schema.Type.STRING, result.valueSchema().field("TMCTDR").schema().type());
        Assert.assertEquals(Schema.Type.STRING, result.valueSchema().field("TMLDDR").schema().type());

        transform.close();
    }

    @Test
    public void testExplicitFieldNames() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        props.put("fields", "TMCTDR");
        transform.configure(props);

        Schema schema = buildSchemaWithTimeFields();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put("TMCTDR", timeMillis(64957000));
        value.put("TMLDDR", timeMillis(64957000));
        value.put("amount", 100.0);

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Struct resultValue = (Struct) result.value();
        Assert.assertEquals("18:02:37", resultValue.get("TMCTDR"));
        Assert.assertTrue(resultValue.get("TMLDDR") instanceof Date);

        transform.close();
    }

    @Test
    public void testWithMillisecondsFormat() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        props.put("format", "HH:mm:ss.SSS");
        transform.configure(props);

        Schema schema = buildSchemaWithTimeFields();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put("TMCTDR", timeMillis(64957000));
        value.put("TMLDDR", timeMillis(64957123));
        value.put("amount", 100.0);

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Struct resultValue = (Struct) result.value();
        Assert.assertEquals("18:02:37.000", resultValue.get("TMCTDR"));
        Assert.assertEquals("18:02:37.123", resultValue.get("TMLDDR"));

        transform.close();
    }

    @Test
    public void testNullTimeValue() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = buildSchemaWithTimeFields();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put("TMCTDR", null);
        value.put("TMLDDR", timeMillis(64957000));
        value.put("amount", 100.0);

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Struct resultValue = (Struct) result.value();
        Assert.assertNull(resultValue.get("TMCTDR"));
        Assert.assertEquals("18:02:37", resultValue.get("TMLDDR"));

        transform.close();
    }

    @Test
    public void testNullRecordValue() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, null, 0);
        SinkRecord result = transform.apply(record);

        Assert.assertNull(result.value());

        transform.close();
    }

    @Test
    public void testNoTimeMillisFieldsPassThrough() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();

        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Assert.assertSame(record, result);

        transform.close();
    }

    @Test
    public void testMidnightTime() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("time", Time.builder().optional().build())
                .build();

        Struct value = new Struct(schema);
        value.put("time", timeMillis(0));

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Assert.assertEquals("00:00:00", ((Struct) result.value()).get("time"));

        transform.close();
    }

    @Test
    public void testEndOfDayTime() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("time", Time.builder().optional().build())
                .build();

        Struct value = new Struct(schema);
        value.put("time", timeMillis(86399999));

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Assert.assertEquals("23:59:59", ((Struct) result.value()).get("time"));

        transform.close();
    }

    @Test
    public void testEndOfDayTimeWithMillis() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        props.put("format", "HH:mm:ss.SSS");
        transform.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("time", Time.builder().optional().build())
                .build();

        Struct value = new Struct(schema);
        value.put("time", timeMillis(86399999));

        SinkRecord record = createRecord(schema, value);
        SinkRecord result = transform.apply(record);

        Assert.assertEquals("23:59:59.999", ((Struct) result.value()).get("time"));

        transform.close();
    }

    @Test
    public void testFormatMillisSinceMidnight() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Assert.assertEquals("18:02:37", transform.formatMillisSinceMidnight(64957000));
        Assert.assertEquals("00:00:00", transform.formatMillisSinceMidnight(0));
        Assert.assertEquals("12:00:00", transform.formatMillisSinceMidnight(43200000));
        Assert.assertEquals("23:59:59", transform.formatMillisSinceMidnight(86399000));
        Assert.assertEquals("00:00:01", transform.formatMillisSinceMidnight(1000));

        transform.close();
    }

    @Test
    public void testSchemaIsCached() {
        TimeMillisToStringTransform<SinkRecord> transform = new TimeMillisToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        transform.configure(props);

        Schema schema = buildSchemaWithTimeFields();
        Struct value1 = new Struct(schema);
        value1.put("id", 1);
        value1.put("name", "test1");
        value1.put("TMCTDR", timeMillis(64957000));
        value1.put("TMLDDR", timeMillis(64957000));
        value1.put("amount", 100.0);

        Struct value2 = new Struct(schema);
        value2.put("id", 2);
        value2.put("name", "test2");
        value2.put("TMCTDR", timeMillis(43200000));
        value2.put("TMLDDR", timeMillis(43200000));
        value2.put("amount", 200.0);

        SinkRecord result1 = transform.apply(createRecord(schema, value1));
        SinkRecord result2 = transform.apply(createRecord(schema, value2));

        Assert.assertSame(result1.valueSchema(), result2.valueSchema());

        Assert.assertEquals("18:02:37", ((Struct) result1.value()).get("TMCTDR"));
        Assert.assertEquals("12:00:00", ((Struct) result2.value()).get("TMCTDR"));

        transform.close();
    }
}
