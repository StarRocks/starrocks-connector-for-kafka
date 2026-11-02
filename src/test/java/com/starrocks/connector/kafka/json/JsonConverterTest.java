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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.connector.kafka.StarRocksSinkTask;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class JsonConverterTest {
    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    private SchemaAndValue getSchemaAndValueFromJsonStr(String jsonStr) throws JsonProcessingException {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, Object> props = new HashMap<>();
        props.put("schemas.enable", (Object) false);
        jsonConverter.configure(props, false);
        JsonSerializer jsonSerializer = jsonConverter.getSerializer();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodeSource = objectMapper.readTree(jsonStr);
        byte[] jsonBytes = jsonSerializer.serialize("", jsonNodeSource);
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("", jsonBytes);
        return schemaAndValue;
    }

    @Test
    public void testConvertSchemalessValues() {
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("long", 1L);
        value.put("int", 2);
        value.put("double", 1.5);
        value.put("bool", true);
        value.put("string", "s");
        value.put("list", Arrays.asList(1, 2));
        value.put("map", Collections.singletonMap("k", "v"));
        String expected = "{\"long\":1,\"int\":2,\"double\":1.5,\"bool\":true,\"string\":\"s\",\"list\":[1,2]," +
                "\"map\":{\"k\":\"v\"}}";
        // The second conversion looks the schema types up in the cache.
        for (int i = 0; i < 2; i++) {
            Assert.assertEquals(expected, jsonConverter.convertToJson(null, value).toString());
        }
        try {
            jsonConverter.convertToJson(null, new Object());
            Assert.fail("A value without a schema type should fail");
        } catch (DataException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("does not have corresponding schema type"));
        }
    }

    @Test
    public void testConvertToJson() throws JsonProcessingException {
        String jsonStr = "{\"elements\":[{\"elName\":\"zll\",\"age\":1},{\"elName\":\"zll1\",\"age\":2}],\"name\":\"haha\",\"id\":1}";
        SchemaAndValue schemaAndValue = getSchemaAndValueFromJsonStr(jsonStr);
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        JsonNode jsonNodeDest = jsonConverter.convertToJson(schemaAndValue.schema(), schemaAndValue.value());
        System.out.println(jsonNodeDest.toString());
        Assert.assertEquals(jsonStr, jsonNodeDest.toString());
    }
}
