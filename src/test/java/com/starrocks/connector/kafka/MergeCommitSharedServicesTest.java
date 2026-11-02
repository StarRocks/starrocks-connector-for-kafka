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

import org.apache.kafka.common.config.ConfigException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;

import static com.starrocks.connector.kafka.MergeCommitOptionsTest.assertConfigException;

public class MergeCommitSharedServicesTest {

    @Before
    public void setUp() {
        MergeCommitSharedServices.reset();
    }

    @After
    public void tearDown() {
        MergeCommitSharedServices.reset();
    }

    private static StreamLoadProperties.Builder builder(String password, String... urls) {
        return StreamLoadProperties.builder()
                .loadUrls(urls)
                .username("user")
                .password(password)
                .defaultTableProperties(StreamLoadTableProperties.builder().database("db").table("tbl").build());
    }

    @Test
    public void testSameEndpoint() {
        MergeCommitSharedServices.check(builder("pw", "fe1:8030", "fe2:8030").build());
        // The order of the urls does not matter.
        MergeCommitSharedServices.check(builder("pw", "fe2:8030", "fe1:8030").build());
        // The SDK uses the tuning of the first task, which only logs a warning.
        MergeCommitSharedServices.check(builder("pw", "fe1:8030", "fe2:8030").setHttpThreadNum(8).build());
    }

    @Test
    public void testDifferentUrls() {
        MergeCommitSharedServices.check(builder("pw", "fe1:8030").build());
        try {
            MergeCommitSharedServices.check(builder("pw", "other-fe:8030").build());
            Assert.fail("Expected a ConfigException");
        } catch (ConfigException e) {
            Assert.assertTrue(e.getMessage().contains("http://fe1:8030"));
            Assert.assertTrue(e.getMessage().contains("http://other-fe:8030"));
            Assert.assertFalse(e.getMessage().contains("pw"));
        }
    }

    @Test
    public void testDifferentPassword() {
        MergeCommitSharedServices.check(builder("pw", "fe1:8030").build());
        assertConfigException(() -> MergeCommitSharedServices.check(builder("pw2", "fe1:8030").build()));
    }
}
