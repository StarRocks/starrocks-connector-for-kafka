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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class StarRocksSinkTaskV2Test {

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

    private SchemaAndValue getConnectDataFromStr(String msg) {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, ?> conf = Collections.emptyMap();
        jsonConverter.configure(conf, false);
        return jsonConverter.toConnectData(TOPIC, msg.getBytes());
    }

    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    @Test
    public void tableProperties() {
        StarRocksSinkTaskV2 task = new StarRocksSinkTaskV2();

        Map<String, String> props = new HashMap<>();
        props.put(
            "sink.table.test_table.properties.columns", 
            "Id, Year, Month, Reference = str_to_date(concat(Year,'-',Month,'-01'), '%Y-%m-%d')");

        Map<String, Map<String, String>> result = task.parseSinkStreamLoadTablesProperties(props);

        Map<String, String> tableProperties = result.get("test_table");
        Assert.assertNotNull(tableProperties);

        if (tableProperties != null) {
            String property = tableProperties.get("columns");
            Assert.assertNotNull(property);
        }
    }
}
