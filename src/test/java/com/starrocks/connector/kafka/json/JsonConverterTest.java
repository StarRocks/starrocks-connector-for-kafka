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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonConverterTest {
    @Test
    void testConvertToJson() throws Exception {
        var jsonStr = "{\"elements\":[{\"elName\":\"zll\",\"age\":1},{\"elName\":\"zll1\",\"age\":2}],\"name\":\"haha\",\"id\":1}";
        var jsonConverter = new JsonConverter();
        var value = new ObjectMapper().readValue(jsonStr, Map.class);
        var result = jsonConverter.fromConnectData(null, value);
        assertEquals(jsonStr, result);
    }
}
