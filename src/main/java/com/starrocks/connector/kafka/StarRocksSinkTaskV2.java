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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.json.DecimalFormat;
import com.starrocks.connector.kafka.json.JsonConverter;
import com.starrocks.connector.kafka.json.JsonConverterConfig;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;


//  Please reference to: https://docs.confluent.io/platform/7.4/connect/javadocs/javadoc/org/apache/kafka/connect/sink/SinkTask.html
//  SinkTask is a Task that takes records loaded from Kafka and sends them to another system.
//  Each task instance is assigned a set of partitions by the Connect framework and will handle
//  all records received from those partitions.

//  As records are fetched from Kafka, they will be passed to the sink task using the put(Collection) API,
//  which should either write them to the downstream system or batch them for later writing.
//  Periodically, Connect will call flush(Map) to ensure that batched records are actually
//  pushed to the downstream system. Below we describe the lifecycle of a SinkTask.
//    1. Initialization: SinkTasks are first initialized using initialize(SinkTaskContext) to prepare the task's context and start(Map) to accept configuration and start any services needed for processing.
//    2. Partition Assignment: After initialization, Connect will assign the task a set of partitions using open(Collection). These partitions are owned exclusively by this task until they have been closed with close(Collection).
//    3. Record Processing: Once partitions have been opened for writing, Connect will begin forwarding records from Kafka using the put(Collection) API. Periodically, Connect will ask the task to flush records using flush(Map) as described above.
//    4. Partition Rebalancing: Occasionally, Connect will need to change the assignment of this task. When this happens, the currently assigned partitions will be closed with close(Collection) and the new assignment will be opened using open(Collection).
//    5. Shutdown: When the task needs to be shutdown, Connect will close active partitions (if there are any) and stop the task using stop()

public class StarRocksSinkTaskV2 extends SinkTask  {

    enum SinkType {
        CSV,
        JSON
    }
    private SinkType sinkType;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTaskV2.class);
    private StreamLoadManagerV2 loadManager;
    private Map<String, StreamLoadManagerV2> tablesLoadManagers;
    private Map<String, String> props;
    private StreamLoadProperties loadProperties;
    private Map<String, StreamLoadProperties> tablesLoadProperties;
    private String database;
    private Map<String, String> topic2Table;
    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;

    private JsonConverter jsonConverter;
    private long maxRetryTimes;
    private long retryCount = 0;
    private Throwable sdkException;

    private long buffMaxbytes;
    private long bufferFlushInterval;
    private long currentBufferBytes = 0;
    private long lastFlushTime = 0;

    private StreamLoadManagerV2 buildLoadManager(StreamLoadProperties loadProperties) {
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(loadProperties, true);
        manager.init();
        return manager;
    }

    private Map<String, StreamLoadManagerV2> buildTablesLoadManagers(Map<String, StreamLoadProperties> tablesLoadProperties) {
        Map<String, StreamLoadManagerV2> managers = new HashMap<>();
        
        for (Map.Entry<String, StreamLoadProperties> tableEntry : tablesLoadProperties.entrySet()) {
            StreamLoadManagerV2 manager = buildLoadManager(tableEntry.getValue());
            
            managers.put(tableEntry.getKey(), manager);
        }

        return managers;
    }

    // Data chunk size in a http request for stream load.
    // Flink connector does not open this configuration to users, so we use a fixed value here.
    private long getChunkLimit() {
        return 3 * GIGA_BYTES_SCALE;
    }

    // Timeout in millisecond to wait for 100-continue response for http client.
    // Flink connector does not open this configuration to users, so we use a fixed value here.
    private int getWaitForContinueTimeout() {
        return 3000;
    }

    // Stream load thread count
    // An HTTP thread pool is used for communication between the SDK and SR. 
    // This configuration item is used to set the number of threads in the thread pool.
    private int getIoThreadCount() {
        return 2;
    }

    private long getScanFrequency() {
        return 50L;
    }

    private String getLabelPrefix() {
        return null;
    }

    private Map<String, String> parseSinkStreamLoadDefaultProperties() {
        Map<String, String> streamLoadProps = new HashMap<>();

        props.keySet().stream()
            .filter(key -> key.startsWith(StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX))
            .forEach(key -> {
                final String value = props.get(key);
                final String subKey = key.substring((StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX).length()).toLowerCase();
                streamLoadProps.put(subKey, value);
            });

        return streamLoadProps;
    }

    public Map<String, Map<String, String>> parseSinkStreamLoadTablesProperties(Map<String, String> props) {
        Map<String, Map<String, String>> tablesStreamLoadProps = new HashMap<>();

        props.keySet().stream()
            .filter(key -> key.matches(StarRocksSinkConnectorConfig.SINK_TABLE_PROPERTIES_REGEX_PREFIX))
            .forEach(key -> {
                String[] tableProperties = key.split("\\.");

                LOG.debug(key);

                final String tableName = tableProperties[2];
                final String propertyName = tableProperties[4];
                final String value = props.get(key);

                tablesStreamLoadProps.computeIfAbsent(tableName, k -> new HashMap<>())
                    .put(propertyName, value);
            });

        return tablesStreamLoadProps;
    }

    private StreamLoadProperties buildLoadProperties() {
        String[] loadUrl = props.get(StarRocksSinkConnectorConfig.STARROCKS_LOAD_URL).split(",");
        database = props.get(StarRocksSinkConnectorConfig.STARROCKS_DATABASE_NAME);
        String format = props.getOrDefault(StarRocksSinkConnectorConfig.SINK_FORMAT, "json").toLowerCase();
        StreamLoadDataFormat dataFormat;
        if (format.equals("csv")) {
            dataFormat = new StreamLoadDataFormat.CSVFormat(StarRocksDelimiterParser
                    .parse(props.get(StarRocksSinkConnectorConfig.SINK_PROPERTIES_ROW_DELIMITER), "\n"));
            sinkType = SinkType.CSV;
        } else if (format.equals("json")) {
            dataFormat = StreamLoadDataFormat.JSON;
            sinkType = SinkType.JSON;
        } else {
            throw new RuntimeException("data format are not support");
        }
        // The default load format for the Starrocks Kafka Connector is JSON. If the format is not specified
        // in the configuration file, it needs to be added to the props.
        if (!props.containsKey(StarRocksSinkConnectorConfig.SINK_FORMAT)) {
            props.put(StarRocksSinkConnectorConfig.SINK_FORMAT, "json");
        }

        Map<String, String> streamLoadProps = parseSinkStreamLoadDefaultProperties();
        LOG.info("Starrocks sink type is {}, stream load properties: {}", sinkType, streamLoadProps);

        // The Stream SDK must force the table name, which we set to _sr_default_table.
        // _sr_default_table will not be used.
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(database)
                .table("_sr_default_table")
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(getChunkLimit())
                .addCommonProperties(streamLoadProps);

        String buffMaxbytesStr = props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES, "67108864");
        buffMaxbytes = Long.parseLong(buffMaxbytesStr);
        String connectTimeoutmsStr = props.getOrDefault(StarRocksSinkConnectorConfig.CONNECT_TIMEOUTMS, "100");
        int connectTimeoutms = Integer.parseInt(connectTimeoutmsStr);
        String username = props.get(StarRocksSinkConnectorConfig.STARROCKS_USERNAME);
        String password = props.get(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD);
        String bufferFlushIntervalStr = props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "1000");
        bufferFlushInterval = Long.parseLong(bufferFlushIntervalStr);

        StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls(loadUrl)
                .defaultTableProperties(defaultTablePropertiesBuilder.build())
                .cacheMaxBytes(buffMaxbytes)
                .connectTimeout(connectTimeoutms)
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .username(username)
                .password(password)
                .expectDelayTime(bufferFlushInterval)
                .addHeaders(streamLoadProps)
                .enableTransaction();

        return builder.build();
    }

    private Map<String, StreamLoadProperties> buildTablesLoadProperties() {
        Map<String, String> defaultStreamLoadProps = parseSinkStreamLoadDefaultProperties();
        Map<String, Map<String, String>> tablesStreamLoadPropsMap = parseSinkStreamLoadTablesProperties(props);
        Map<String, StreamLoadProperties> tablesStreamLoads = new HashMap<>();
        for (Map.Entry<String, Map<String, String>> tableEntry : tablesStreamLoadPropsMap.entrySet()) {
            LOG.info("Starrocks sink type is {}, table: {}, stream load properties: {}", sinkType, tableEntry.getKey(), tableEntry.getValue());

            Map<String, String> mergedProps = new HashMap<String, String>(defaultStreamLoadProps);
            mergedProps.putAll(tableEntry.getValue());

            StreamLoadProperties.Builder builder = StreamLoadProperties.builder()
                .loadUrls(loadProperties.getLoadUrls())
                .defaultTableProperties(loadProperties.getDefaultTableProperties())
                .cacheMaxBytes(loadProperties.getMaxCacheBytes())
                .connectTimeout(loadProperties.getConnectTimeout())
                .waitForContinueTimeoutMs(getWaitForContinueTimeout())
                .ioThreadCount(getIoThreadCount())
                .scanningFrequency(getScanFrequency())
                .labelPrefix(getLabelPrefix())
                .username(loadProperties.getUsername())
                .password(loadProperties.getPassword())
                .expectDelayTime(loadProperties.getExpectDelayTime())
                .addHeaders(mergedProps)
                .enableTransaction();

            tablesStreamLoads.put(tableEntry.getKey(), builder.build());
        }

        return tablesStreamLoads;
    }

    @Override
    public String version() {
        return Util.VERSION;
    }

    public static JsonConverter createJsonConverter() {
        JsonConverter converter = new JsonConverter();
        Map<String, Object> conf = new HashMap<>();
        conf.put(JsonConverterConfig.REPLACE_NULL_WITH_DEFAULT_CONFIG, (Object) false);
        conf.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        converter.configure(conf, false);
        return converter;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starrocks sink task starting. version is " + Util.VERSION);
        this.props = props;
        loadProperties = buildLoadProperties();
        tablesLoadProperties = buildTablesLoadProperties();
        loadManager = buildLoadManager(loadProperties);
        tablesLoadManagers = buildTablesLoadManagers(tablesLoadProperties);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = createJsonConverter();
        maxRetryTimes = Long.parseLong(props.getOrDefault(StarRocksSinkConnectorConfig.SINK_MAXRETRIES, "3"));
        LOG.info("Starrocks sink task started. version is " + Util.VERSION);
    }

    static Map<String, String> getTopicToTableMap(Map<String, String> config) {
        if (config.containsKey(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP)) {
            Map<String, String> result =
                    Util.parseTopicToTableMap(config.get(StarRocksSinkConnectorConfig.STARROCKS_TOPIC2TABLE_MAP));
            if (result != null) {
                return result;
            }
            LOG.error("Invalid Input, Topic2Table Map disabled");
        }
        return new HashMap<>();
    }

    private String getTableFromTopic(String topic) {
        return topic2Table.getOrDefault(topic, topic);
    }

    public void setJsonConverter(JsonConverter jsonConverter) {
        this.jsonConverter = jsonConverter;
    }

    public void setSinkType(SinkType sinkType) {
        this.sinkType = sinkType;
    }

    // This function is used to parse a SinkRecord and returns a String type row.
    // There are several scenarios to consider:
    // 1. If `sinkRecord` is null, return directly.
    // 2. If the `value` of `sinkRecord` is null, return directly.
    // 3. If the `valueSchema` of `sinkRecord` is null, it means that the received
    //    data does not have a defined schema. In this case, an attempt is made to
    //    parse it. If the parsing fails, a `DataException` exception will be thrown.
    public String getRecordFromSinkRecord(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            LOG.debug("Have got a null sink record");
            return null;
        }
        if (sinkRecord.value() == null) {
            LOG.debug(String.format("Sink record value is null, the record is %s", sinkRecord.toString()));
            return null;
        }
        if (sinkRecord.valueSchema() == null) {
            LOG.debug(String.format("Sink record value schema is null, the record is %s", sinkRecord.toString()));
        }

        if (sinkType == SinkType.CSV) {
            // When the sink Type is CSV, make sure that the SinkRecord.value type is String
            String row = null;
            try {
                row = (String) sinkRecord.value();
            } catch (ClassCastException e) {
                LOG.error(e.getMessage());
                throw new DataException(e.getMessage());
            }
            return row;
        } else {
            JsonNode jsonNodeDest = null;
            try {
                jsonNodeDest = jsonConverter.convertToJson(sinkRecord.valueSchema(), sinkRecord.value());
            } catch (DataException dataException) {
                LOG.error(dataException.getMessage());
                throw dataException;
            }
            return jsonNodeDest.toString();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        long start = System.currentTimeMillis();
        if (maxRetryTimes != -1) {
            if (retryCount > maxRetryTimes) {
                LOG.error("Starrocks Put failure " + retryCount + " times, which bigger than maxRetryTimes "
                            + maxRetryTimes + ", sink task will be stopped");
                assert sdkException != null;
                LOG.error("Error message is ", sdkException);
                throw new RuntimeException(sdkException);
            }
        }
        Iterator<SinkRecord> it = records.iterator();
        boolean occurException = false;
        Exception e = null;
        SinkRecord record = null;
        SinkRecord firstRecord = null;
        while (it.hasNext()) {
            record = it.next();
            if (firstRecord == null) {
                firstRecord = record;
            }
            LOG.debug("Received record: " + record.toString());

            String topic = record.topic();
            // The sdk does not provide the ability to clean up exceptions, that is to say, according to the current implementation of the SDK,
            // after an Exception occurs, the SDK must be re-initialized, which is based on flink:
            // 1. When an exception occurs, put will continue to fail, at which point we do nothing and let put move forward.
            // 2. Because the framework periodically calls the preCommit method, we can sense if an exception has occurred in
            //    this method. In the case of an exception, we initialize the new SDK and then throw an exception to the framework.
            //    In this case, the framework repulls the data from the commit point and then moves forward.
            String row = getRecordFromSinkRecord(record);
            LOG.debug("Parsed row: " + row);
            if (row == null) {
                continue;
            }

            try {
                String destinationTable = getTableFromTopic(topic);

                StreamLoadManagerV2 tableLoadManager = tablesLoadManagers.getOrDefault(destinationTable, loadManager);

                tableLoadManager.write(null, database, destinationTable, row);
                currentBufferBytes += row.getBytes().length;
            } catch (Exception writeException) {
                LOG.error("Starrocks Put error: " + writeException.getMessage() +
                          " topic, partition, offset is " + topic + ", " + record.kafkaPartition() + ", " + record.kafkaOffset());
                writeException.printStackTrace();
                occurException = true;
                e = writeException;
                break;
            }
        }

        if (occurException && e != null) {
            LOG.info("Starrocks Put occurs exception, Err {} currentBufferBytes {} recordRange [{}:{}-{}:{}] cost {}ms",
                    e.getMessage(), currentBufferBytes, 
                    firstRecord == null ? null : firstRecord.kafkaPartition(),
                    firstRecord == null ? null : firstRecord.kafkaOffset(),
                    record == null ? null : record.kafkaPartition(),
                    record == null ? null : record.kafkaOffset(), System.currentTimeMillis() - start);
        } else {
            LOG.info("Starrocks Put success, currentBufferBytes {} recordRange [{}:{}-{}:{}] cost {}ms",
                    currentBufferBytes, 
                    firstRecord == null ? null : firstRecord.kafkaPartition(),
                    firstRecord == null ? null : firstRecord.kafkaOffset(),
                    record == null ? null : record.kafkaPartition(),
                    record == null ? null : record.kafkaOffset(), System.currentTimeMillis() - start);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        long start = System.currentTimeMillis();
        // return previous offset when buffer size and flush interval are not reached
        if (currentBufferBytes < buffMaxbytes && System.currentTimeMillis() - lastFlushTime < bufferFlushInterval) {
            LOG.info("Starrocks skip preCommit currentBufferBytes {} less than buffMaxbytes {}"
                    + " or SinceLastFlushTime {} less than bufferFlushInterval {}",
                    currentBufferBytes, buffMaxbytes, System.currentTimeMillis() - lastFlushTime, bufferFlushInterval);
            return Collections.emptyMap();
        }
        
        try {
            flushManagers(offsets);

            retryCount = 0;
            sdkException = null;
            LOG.info("Starrocks preCommit for flush success offsets {} cost {}ms", offsets, System.currentTimeMillis() - start);
        } catch (Throwable e) {
            sdkException = e;
            retryCount++;

            throw new RuntimeException(e.getMessage());
        } finally {
            lastFlushTime = System.currentTimeMillis();
            currentBufferBytes = 0;
        }
        
        return offsets;
    }

    private void flushManagers(Map<TopicPartition, OffsetAndMetadata> offsets) throws Throwable {
        HashSet<String> tablesToFlush = new HashSet<>();
        for (TopicPartition topicPartition : offsets.keySet()) {
            String table = getTableFromTopic(topicPartition.topic());

            tablesToFlush.add(table);
        }

        for (String table : tablesToFlush) {
            StreamLoadManagerV2 tableManager = tablesLoadManagers.get(table);

            try {
                flushOrThrow(tableManager);
            } catch (Throwable e) {
                StreamLoadManagerV2 newTableManager = buildLoadManager(tablesLoadProperties.get(table));
                tablesLoadManagers.put(table, newTableManager);

                throw e;
            }   
        }

        try {
            flushOrThrow(loadManager);
        } catch (Exception e) {
            loadManager = buildLoadManager(loadProperties);

            throw e;
        }
        
    }

    private void flushOrThrow(StreamLoadManagerV2 manager) throws Throwable {
        manager.flush();

        if (manager.getException() != null) {
            throw manager.getException();
        }
    }

    @Override
    public void stop() {
        LOG.info("Starrocks sink task stopped. version is " + Util.VERSION);
    }
}