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

package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.StarRocksSinkTask;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

// Debezium represents numeric columns with no declared precision/scale (e.g. Postgres `numeric`
// with no size) using io.debezium.data.VariableScaleDecimal - a Struct{scale: int32, value: bytes} -
// rather than the fixed-scale org.apache.kafka.connect.data.Decimal logical type. These tests confirm
// that JsonConverter turns that struct back into a plain JSON number without losing precision.
public class VariableScaleDecimalConverterTest {

    private static final String FIELD_NAME = "savings_amount";

    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    private JsonNode convert(BigDecimal decimal) {
        // Build the field value against the same (optional) schema variant used for the field
        // itself below - Struct.put() validates the value's schema against the field's schema,
        // so using the non-optional VariableScaleDecimal.schema() here would fail validation.
        Struct fieldValue = VariableScaleDecimal.fromLogical(VariableScaleDecimal.optionalSchema(), new SpecialValueDecimal(decimal));
        return convertField(fieldValue);
    }

    private JsonNode convertField(Struct fieldValue) {
        Schema fieldSchema = VariableScaleDecimal.optionalSchema();
        Schema topSchema = SchemaBuilder.struct().name("test.Value").field(FIELD_NAME, fieldSchema).build();
        Struct top = new Struct(topSchema);
        top.put(FIELD_NAME, fieldValue);

        // Use the same converter configuration the sink task actually uses (decimal.format=NUMERIC),
        // rather than a converter with defaults, since that's the behavior a contribution needs to match.
        JsonConverter converter = StarRocksSinkTask.createJsonConverter();
        return converter.convertToJson(topSchema, top);
    }

    @Test
    public void testFractionalValueRetainsPrecision() {
        JsonNode node = convert(new BigDecimal("1234.5678"));
        Assert.assertEquals("{\"savings_amount\":1234.5678}", node.toString());
    }

    @Test
    public void testNegativeValueRetainsPrecision() {
        JsonNode node = convert(new BigDecimal("-987.654321"));
        Assert.assertEquals("{\"savings_amount\":-987.654321}", node.toString());
    }

    @Test
    public void testTrailingZeroScaleIsPreserved() {
        // scale=2 on a value that happens to be integral - must stay "1235.00", not collapse to "1235".
        JsonNode node = convert(new BigDecimal("1235.00"));
        Assert.assertEquals("{\"savings_amount\":1235.00}", node.toString());
    }

    @Test
    public void testSmallFractionalValue() {
        JsonNode node = convert(new BigDecimal("0.0001"));
        Assert.assertEquals("{\"savings_amount\":0.0001}", node.toString());
    }

    @Test
    public void testUnconstrainedHighPrecisionValue() {
        // The scenario VariableScaleDecimal exists for: a numeric column with no declared
        // precision/scale, which can carry more digits than any fixed-scale DECIMAL could.
        JsonNode node = convert(new BigDecimal("99999999999999999999.123456789"));
        Assert.assertEquals("{\"savings_amount\":99999999999999999999.123456789}", node.toString());
    }

    @Test
    public void testZeroValue() {
        JsonNode node = convert(BigDecimal.ZERO);
        Assert.assertEquals("{\"savings_amount\":0}", node.toString());
    }

    @Test
    public void testNullFieldIsNotDefaulted() {
        JsonNode node = convertField(null);
        Assert.assertEquals("{\"savings_amount\":null}", node.toString());
    }
}
