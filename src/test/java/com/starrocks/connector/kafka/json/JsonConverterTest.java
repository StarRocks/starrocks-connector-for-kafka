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
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
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
    public void testConvertToJson() throws JsonProcessingException {
        String jsonStr = "{\"elements\":[{\"elName\":\"zll\",\"age\":1},{\"elName\":\"zll1\",\"age\":2}],\"name\":\"haha\",\"id\":1}";
        SchemaAndValue schemaAndValue = getSchemaAndValueFromJsonStr(jsonStr);
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        JsonNode jsonNodeDest = jsonConverter.convertToJson(schemaAndValue.schema(), schemaAndValue.value());
        System.out.println(jsonNodeDest.toString());
        Assert.assertEquals(jsonStr, jsonNodeDest.toString());
    }
}
