package com.starrocks.connector.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.starrocks.connector.kafka.json.JsonConverter;
import com.starrocks.data.load.stream.Record;
import com.starrocks.data.load.stream.StreamLoadDataFormat;
import com.starrocks.data.load.stream.properties.StreamLoadProperties;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import com.starrocks.data.load.stream.v2.StreamLoadManagerV2;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


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

public class StarRocksSinkTask extends SinkTask  {

    enum SinkType {
        CSV,
        JSON
    }

    private SinkType sinkType;
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTask.class);
    // topicname -> partiton -> offset
    private HashMap<String, HashMap<Integer, Long>> topicPartitionOffset;
    private StreamLoadManagerV2 loadManager;

    private Map<String, String> props;

    private StreamLoadProperties loadProperties;

    private String database;
    private Map<String, String> topic2Table;

    private static final long KILO_BYTES_SCALE = 1024L;
    private static final long MEGA_BYTES_SCALE = KILO_BYTES_SCALE * KILO_BYTES_SCALE;
    private static final long GIGA_BYTES_SCALE = MEGA_BYTES_SCALE * KILO_BYTES_SCALE;
    private final Map<String, String> streamLoadProps = new HashMap<>();

    private JsonConverter jsonConverter;

    private StreamLoadManagerV2 buildLoadManager(StreamLoadProperties loadProperties) {
        StreamLoadManagerV2 manager = new StreamLoadManagerV2(loadProperties, true);
        manager.init();
        return manager;
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

    private void parseSinkStreamLoadProperties() {
        props.keySet().stream()
                .filter(key -> key.startsWith(StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX))
                .forEach(key -> {
                    final String value = props.get(key);
                    final String subKey = key.substring((StarRocksSinkConnectorConfig.SINK_PROPERTIES_PREFIX).length()).toLowerCase();
                    streamLoadProps.put(subKey, value);
                });
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
        // The Stream SDK must force the table name, which we set to _sr_default_table.
        // _sr_default_table will not be used.
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(database)
                .table("_sr_default_table")
                .streamLoadDataFormat(dataFormat)
                .chunkLimit(getChunkLimit());
        String buffMaxbytesStr = props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_MAXBYTES, "67108864");
        long buffMaxbytes = Long.parseLong(buffMaxbytesStr);
        String connectTimeoutmsStr = props.getOrDefault(StarRocksSinkConnectorConfig.CONNECT_TIMEOUTMS, "100");
        int connectTimeoutms = Integer.parseInt(connectTimeoutmsStr);
        String username = props.get(StarRocksSinkConnectorConfig.STARROCKS_USERNAME);
        String password = props.get(StarRocksSinkConnectorConfig.STARROCKS_PASSWORD);
        String bufferFlushIntervalStr = props.getOrDefault(StarRocksSinkConnectorConfig.BUFFERFLUSH_INTERVALMS, "1000");
        int bufferFlushInterval = Integer.parseInt(bufferFlushIntervalStr);
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

    @Override
    public String version() {
        return Util.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        LOG.info("Starrocks sink task starting. version is " + Util.VERSION);
        this.props = props;
        parseSinkStreamLoadProperties();
        topicPartitionOffset = new HashMap<>();
        loadProperties = buildLoadProperties();
        loadManager = buildLoadManager(loadProperties);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = new JsonConverter();
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

    private String getRecordFromSinkRecord(SinkRecord sinkRecord) {
        if (sinkType == SinkType.CSV) {
            // When the sink Type is CSV, make sure that the SinkRecord type is String
            Schema schema = sinkRecord.valueSchema();
            if (schema == null || schema.type() != Schema.Type.STRING) {
                throw new RuntimeException("The sink type does not match the data type consumed");
            }
            String row = (String) sinkRecord.value();
            return row;
        } else {
            Schema schema = sinkRecord.valueSchema();
            if (schema == null || schema.type() != Schema.Type.STRUCT) {
                throw new RuntimeException("The sink type does not match the data type consumed");
            }
            JsonNode jsonNodeDest = jsonConverter.convertToJson(sinkRecord.valueSchema(), sinkRecord.value());
            String row = jsonNodeDest.toString();
            return row;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        Iterator<SinkRecord> it = records.iterator();
        boolean occurException = false;
        Exception e = null;
        while (it.hasNext()) {
            final SinkRecord record = it.next();
            LOG.debug(record.toString());
            String topic = record.topic();
            int partition = record.kafkaPartition();
            if (!topicPartitionOffset.containsKey(topic)) {
                topicPartitionOffset.put(topic, new HashMap<>());
            }
            if (topicPartitionOffset.get(topic).containsKey(partition) && topicPartitionOffset.get(topic).get(partition) >= record.kafkaOffset()) {
                continue;
            }
            topicPartitionOffset.get(topic).put(partition, record.kafkaOffset());
            // The sdk does not provide the ability to clean up exceptions, that is to say, according to the current implementation of the SDK,
            // after an Exception occurs, the SDK must be re-initialized, which is based on flink:
            // 1. When an exception occurs, put will continue to fail, at which point we do nothing and let put move forward.
            // 2. Because the framework periodically calls the preCommit method, we can sense if an exception has occurred in
            //    this method. In the case of an exception, we initialize the new SDK and then throw an exception to the framework.
            //    In this case, the framework repulls the data from the commit point and then moves forward.
            String row = getRecordFromSinkRecord(record);
            try {
                loadManager.write(null, database, getTableFromTopic(topic), row);
            } catch (Exception sdkException) {
                LOG.error("put error: " + sdkException.getMessage() +
                          " topic, partition, offset is " + topic + ", " + record.kafkaPartition() + ", " + record.kafkaOffset());
                sdkException.printStackTrace();
                occurException = true;
                e = sdkException;
                break;
            }
        }

        if (occurException && e != null) {
            LOG.info("put occurs exception, Err: " + e.getMessage());
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        loadManager.flush();
        if (loadManager.getException() != null) {
            //  When an exception occurs, we re-initialize the SDK instance.
            loadManager = buildLoadManager(loadProperties);
            //  Each time the SDK is checked for an exception, if an exception occurs, it is thrown, and the framework replays the data from the offset of the commit.
            throw new RuntimeException(loadManager.getException().getMessage());
        }
        HashMap<TopicPartition, OffsetAndMetadata> synced = new HashMap<>();
        for (TopicPartition topicPartition : offsets.keySet()) {
            if (!topicPartitionOffset.containsKey(topicPartition.topic()) || !topicPartitionOffset.get(topicPartition.topic()).containsKey(topicPartition.partition())) {
                continue;
            }
            LOG.info("commit: topic: " + topicPartition.topic() + ", partition: " + topicPartition.partition() + ", offset: " + topicPartitionOffset.get(topicPartition.topic()).get(topicPartition.partition()));
            synced.put(topicPartition, new OffsetAndMetadata(topicPartitionOffset.get(topicPartition.topic()).get(topicPartition.partition())));
        }
        return synced;
    }

    @Override
    public void stop() {
        LOG.info("Starrocks sink task stopped. version is " + Util.VERSION);
    }
}
