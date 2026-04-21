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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StarRocksSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnector.class);
    private Map<String, String> config; // connector configuration, provided by

    @Override
    public void start(final Map<String, String> props) {
        config = Collections.unmodifiableMap(props);
        LOG.info("StarRocks sink connector started. version is {}", version());
    }

    @Override
    public void stop() {
        LOG.info("StarRocks sink connector stopped. version is {}", version());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return StarRocksSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        return Collections.nCopies(maxTasks, config);
    }

    @Override
    public ConfigDef config() {
        return StarRocksSinkConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return Util.VERSION;
    }
}
