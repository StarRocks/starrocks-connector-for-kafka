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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.starrocks.data.load.stream.properties.StreamLoadProperties;

// For merge commit, the stream load SDK talks to StarRocks through HTTP services (FeMetaService and
// BackendHttpService) that every user in a class loader shares, and it creates them with the settings
// of the first user only. All connectors of this plugin in a Kafka Connect worker share a class loader,
// so a task whose StarRocks endpoint or credentials differ from the first merge commit task would send
// its requests to the first task's FE nodes and check its loads with the first task's credentials.
// Such a task fails to start instead. Remove this check once the SDK keys the services by their settings.
final class MergeCommitSharedServices {

    private static final Logger LOG = LoggerFactory.getLogger(MergeCommitSharedServices.class);

    // The settings the SDK created its shared services with. The SDK keeps them for the lifetime of
    // the class loader, even after all its users close, so this is never cleared either.
    private static Settings first;

    private MergeCommitSharedServices() {
    }

    static synchronized void check(StreamLoadProperties properties) {
        Settings settings = Settings.of(properties);
        if (first == null) {
            first = settings;
            return;
        }
        if (!first.endpoint.equals(settings.endpoint)) {
            throw new ConfigException(String.format("All merge commit tasks in a Kafka Connect worker must use " +
                    "the same %s, %s and %s, because the stream load SDK shares its connections to StarRocks " +
                    "among them and keeps the settings of the first task. The first task uses %s, and this task " +
                    "uses %s. Run this connector in another worker, or restart the worker after changing these " +
                    "settings.", StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL,
                    StarRocksSinkConnectorConfig.STARROCKS_USERNAME, StarRocksSinkConnectorConfig.STARROCKS_PASSWORD,
                    first.endpoint, settings.endpoint));
        }
        if (!first.tuning.equals(settings.tuning)) {
            LOG.warn("The stream load SDK shares its connections to StarRocks among the merge commit tasks in this " +
                    "worker, so this task uses {} of the first task instead of {}", first.tuning, settings.tuning);
        }
    }

    // Visible for testing
    static synchronized void reset() {
        first = null;
    }

    private static final class Settings {

        private final Endpoint endpoint;
        private final Tuning tuning;

        private Settings(Endpoint endpoint, Tuning tuning) {
            this.endpoint = endpoint;
            this.tuning = tuning;
        }

        static Settings of(StreamLoadProperties properties) {
            List<String> urls = Arrays.stream(properties.getLoadUrls()).sorted().collect(Collectors.toList());
            Endpoint endpoint = new Endpoint(urls, properties.getUsername(), sha256(properties.getPassword()));
            Tuning tuning = new Tuning(properties.getHttpThreadNum(), properties.getHttpMaxConnectionsPerRoute(),
                    properties.getHttpTotalMaxConnections(), properties.getHttpIdleConnectionTimeoutMs(),
                    properties.getNodeMetaUpdateIntervalMs());
            return new Settings(endpoint, tuning);
        }

        private static String sha256(String value) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hash = digest.digest((value == null ? "" : value).getBytes(StandardCharsets.UTF_8));
                StringBuilder builder = new StringBuilder();
                for (byte b : hash) {
                    builder.append(String.format("%02x", b));
                }
                return builder.toString();
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private static final class Endpoint {

        private final List<String> urls;
        private final String username;
        private final String passwordDigest;

        private Endpoint(List<String> urls, String username, String passwordDigest) {
            this.urls = urls;
            this.username = username;
            this.passwordDigest = passwordDigest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Endpoint)) {
                return false;
            }
            Endpoint that = (Endpoint) o;
            return urls.equals(that.urls) && Objects.equals(username, that.username)
                    && passwordDigest.equals(that.passwordDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(urls, username, passwordDigest);
        }

        // Leaves out the password.
        @Override
        public String toString() {
            return "{urls: " + urls + ", username: " + username + "}";
        }
    }

    private static final class Tuning {

        private final int httpThreadNum;
        private final int httpMaxConnections;
        private final int httpTotalMaxConnections;
        private final int httpIdleConnectionTimeoutMs;
        private final int nodeMetaUpdateIntervalMs;

        private Tuning(int httpThreadNum, int httpMaxConnections, int httpTotalMaxConnections,
                       int httpIdleConnectionTimeoutMs, int nodeMetaUpdateIntervalMs) {
            this.httpThreadNum = httpThreadNum;
            this.httpMaxConnections = httpMaxConnections;
            this.httpTotalMaxConnections = httpTotalMaxConnections;
            this.httpIdleConnectionTimeoutMs = httpIdleConnectionTimeoutMs;
            this.nodeMetaUpdateIntervalMs = nodeMetaUpdateIntervalMs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Tuning)) {
                return false;
            }
            Tuning that = (Tuning) o;
            return httpThreadNum == that.httpThreadNum && httpMaxConnections == that.httpMaxConnections
                    && httpTotalMaxConnections == that.httpTotalMaxConnections
                    && httpIdleConnectionTimeoutMs == that.httpIdleConnectionTimeoutMs
                    && nodeMetaUpdateIntervalMs == that.nodeMetaUpdateIntervalMs;
        }

        @Override
        public int hashCode() {
            return Objects.hash(httpThreadNum, httpMaxConnections, httpTotalMaxConnections,
                    httpIdleConnectionTimeoutMs, nodeMetaUpdateIntervalMs);
        }

        @Override
        public String toString() {
            return "{" + MergeCommitOptions.HTTP_THREAD_NUM + ": " + httpThreadNum
                    + ", " + MergeCommitOptions.HTTP_MAX_CONNECTIONS + ": " + httpMaxConnections
                    + ", " + MergeCommitOptions.HTTP_TOTAL_MAX_CONNECTIONS + ": " + httpTotalMaxConnections
                    + ", " + MergeCommitOptions.HTTP_IDLE_CONNECTION_TIMEOUT_MS + ": " + httpIdleConnectionTimeoutMs
                    + ", " + MergeCommitOptions.NODE_META_UPDATE_INTERVAL_MS + ": " + nodeMetaUpdateIntervalMs + "}";
        }
    }
}
