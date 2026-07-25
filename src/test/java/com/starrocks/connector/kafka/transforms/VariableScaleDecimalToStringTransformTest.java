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

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class VariableScaleDecimalToStringTransformTest {

    private static final String FIELD_NAME = "savings_amount";

    private Schema buildSchemaWithVsdField() {
        return SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field(FIELD_NAME, VariableScaleDecimal.optionalSchema())
                .build();
    }

    private Struct vsdValue(BigDecimal decimal) {
        return VariableScaleDecimal.fromLogical(VariableScaleDecimal.optionalSchema(), new SpecialValueDecimal(decimal));
    }

    private SinkRecord createRecord(Schema schema, Struct value) {
        return new SinkRecord("test-topic", 0, null, null, schema, value, 0);
    }

    @Test
    public void testAutoDetectConvertsToString() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, vsdValue(new BigDecimal("1234.5678")));

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        Assert.assertEquals("1234.5678", resultValue.get(FIELD_NAME));
        Assert.assertEquals(Schema.Type.STRING, result.valueSchema().field(FIELD_NAME).schema().type());
        Assert.assertEquals(1, resultValue.get("id"));
        Assert.assertEquals("test", resultValue.get("name"));

        transform.close();
    }

    @Test
    public void testExplicitFieldName() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        Map<String, String> props = new HashMap<>();
        props.put("fields", FIELD_NAME);
        transform.configure(props);

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, vsdValue(new BigDecimal("-987.654321")));

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        Assert.assertEquals("-987.654321", resultValue.get(FIELD_NAME));

        transform.close();
    }

    @Test
    public void testTrailingZeroScaleIsPreserved() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, vsdValue(new BigDecimal("1235.00")));

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        // Must stay "1235.00", not collapse to "1235" - this is the precision-loss regression this
        // transform exists to prevent.
        Assert.assertEquals("1235.00", resultValue.get(FIELD_NAME));

        transform.close();
    }

    @Test
    public void testUnconstrainedHighPrecisionValue() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, vsdValue(new BigDecimal("99999999999999999999.123456789")));

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        Assert.assertEquals("99999999999999999999.123456789", resultValue.get(FIELD_NAME));

        transform.close();
    }

    @Test
    public void testZeroValue() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, vsdValue(BigDecimal.ZERO));

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        Assert.assertEquals("0", resultValue.get(FIELD_NAME));

        transform.close();
    }

    @Test
    public void testNullFieldValue() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();
        Struct value = new Struct(schema);
        value.put("id", 1);
        value.put("name", "test");
        value.put(FIELD_NAME, null);

        SinkRecord result = transform.apply(createRecord(schema, value));
        Struct resultValue = (Struct) result.value();

        Assert.assertNull(resultValue.get(FIELD_NAME));

        transform.close();
    }

    @Test
    public void testNullRecordValuePassThrough() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        SinkRecord record = new SinkRecord("test-topic", 0, null, null, null, null, 0);
        SinkRecord result = transform.apply(record);

        Assert.assertNull(result.value());

        transform.close();
    }

    @Test
    public void testNoTargetFieldsPassThrough() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

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
    public void testSchemaIsCached() {
        VariableScaleDecimalToStringTransform<SinkRecord> transform = new VariableScaleDecimalToStringTransform<>();
        transform.configure(new HashMap<>());

        Schema schema = buildSchemaWithVsdField();

        Struct value1 = new Struct(schema);
        value1.put("id", 1);
        value1.put("name", "test1");
        value1.put(FIELD_NAME, vsdValue(new BigDecimal("100.5")));

        Struct value2 = new Struct(schema);
        value2.put("id", 2);
        value2.put("name", "test2");
        value2.put(FIELD_NAME, vsdValue(new BigDecimal("200.75")));

        SinkRecord result1 = transform.apply(createRecord(schema, value1));
        SinkRecord result2 = transform.apply(createRecord(schema, value2));

        Assert.assertSame(result1.valueSchema(), result2.valueSchema());
        Assert.assertEquals("100.5", ((Struct) result1.value()).get(FIELD_NAME));
        Assert.assertEquals("200.75", ((Struct) result2.value()).get(FIELD_NAME));

        transform.close();
    }
}
