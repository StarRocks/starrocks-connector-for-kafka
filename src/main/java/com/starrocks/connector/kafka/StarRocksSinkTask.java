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


// SinkTask的工作过程，参考 https://docs.confluent.io/platform/7.4/connect/javadocs/javadoc/org/apache/kafka/connect/sink/SinkTask.html
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


    private StreamLoadManagerV2 buildLoadManager(StreamLoadProperties loadProperties, HashMap<String, HashMap<Integer, Long>> topicPartitionOffset) {
        KafkaListener kafkaListener = new KafkaListener(topicPartitionOffset);
        return new StreamLoadManagerV2(loadProperties, true, kafkaListener);
    }

    //    Data chunk size in a http request for stream load
    //    flink connector并没有将这个配置开放给用户，所以我们这里写使用一个固定值
    private long getChunkLimit() {
        return 3 * GIGA_BYTES_SCALE;
    }

    //    Timeout in millisecond to wait for 100-continue response for http client.
    //    flink connector并没有将这个配置开放给用户，所以我们这里写使用一个固定值
    private int getWaitForContinueTimeout() {
        return 3000;
    }

    // Stream load thread count
    // SDK和SR通信时，使用了一个HTTP线程池，这个配置项用于设置线程池线程的个数
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
        String format = props.getOrDefault(StarRocksSinkConnectorConfig.SINK_FORMAT, "json");
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
        StreamLoadTableProperties.Builder defaultTablePropertiesBuilder = StreamLoadTableProperties.builder()
                .database(database)
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
        LOG.info("Starrocks sink task started. version is " + Util.VERSION);
        this.props = props;
        parseSinkStreamLoadProperties();
        topicPartitionOffset = new HashMap<>();
        loadProperties = buildLoadProperties();
        loadManager = buildLoadManager(loadProperties, topicPartitionOffset);
        topic2Table = getTopicToTableMap(props);
        jsonConverter = new JsonConverter();
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

//    现状：
//    目前的状态是我们的SDK支持CSV和JSON两种格式，所以我们需要把SinkRecord转成
//    这两种格式。sinkRecord里面的数据是kafka Connect框架自定义的数据类型。
//    所以我们的问题域是：在kafka connect框架提供的数据格式基础上将其转换成
//    CSV或者JSON。
//    那么我可以根据sink.format这个配置项来着手
//    1. 如果sink.format == csv
//
//
    private Record getRecordFromSinkRecord(SinkRecord sinkRecord) {
        if (sinkType == SinkType.CSV) {
            // 当sink类型为CSV时，要确保SinkRecord的Type是String类型
            Schema schema = sinkRecord.valueSchema();
            if (schema == null || schema.type() != Schema.Type.STRING) {
                throw new RuntimeException("The sink type does not match the data type consumed");
            }
            String row = (String) sinkRecord.value();
            KafkaMeta meta = new KafkaMeta(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
            return new Record(row, meta);
        } else {
            Schema schema = sinkRecord.valueSchema();
            if (schema == null || schema.type() != Schema.Type.STRUCT) {
                throw new RuntimeException("The sink type does not match the data type consumed");
            }
            JsonNode jsonNodeDest = jsonConverter.convertToJson(sinkRecord.valueSchema(), sinkRecord.value());
            String row = jsonNodeDest.toString();
            KafkaMeta meta = new KafkaMeta(sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
            return new Record(row, meta);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        LOG.debug("Starrocks sink task. version is " + Util.VERSION);
        Iterator<SinkRecord> it = records.iterator();
        boolean occurException = false;
        Exception e = null;
        synchronized (topicPartitionOffset) {
            while (it.hasNext()) {
                final SinkRecord record = it.next();
                LOG.debug(record.toString());
                String topic = record.topic();
                int partition = record.kafkaPartition();
                if (!topicPartitionOffset.containsKey(topic)) {
                    topicPartitionOffset.put(topic, new HashMap<>());
                }
                if (topicPartitionOffset.get(topic).containsKey(partition) && topicPartitionOffset.get(topic).get(partition) <= record.kafkaOffset()) {
                    continue;
                }
//              sdk没有提供清理Exception的能力，也就是说按照SDK目前的实现，出现异常后，SDK必须重新初始化, 这里的做法参考了flink:
//              1. 当出现异常时，put会持续失败，此时我们不做任何事情，让put继续往前推进。
//              2. 因为框架会周期性地调用preCommit方法，这个方法里我们可以感知到是否发生了异常
//                 对于出现异常的情况，我们初始化新的SDK，然后向框架抛出异常。这种情况下，框架会
//                 重新从commit位点拉数据，然后继续往前推进。
                Record row = getRecordFromSinkRecord(record);
                try {
                    loadManager.write(null, database, getTableFromTopic(topic), row);
                } catch (Exception sdkException) {
                    occurException = true;
                    e = sdkException;
                    break;
                }
            }
        }
        if (occurException && e != null) {
            LOG.info("put occurs exception, Err: " + e.getMessage());
        }
    }

// 这个函数是kafka connect框架向kafka broker提交offset信息时被调用的。
// 传入参数表示上次put时fetch数据的位点信息
// 返回参数是connctor告知框架，我现在提交的位点信息是多少，你可以将kafka broker的位点信息同步到这个地方
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (loadManager.getException() != null) {
            //  发生异常，我们重新初始化SDK实例
            loadManager = buildLoadManager(loadProperties, topicPartitionOffset);
            //  每次check SDK是否发生异常，如果发生了异常则抛出，此时框架会重新从commit的offset处回放数据
            throw new RuntimeException(loadManager.getException().getMessage());
        }
        HashMap<TopicPartition, OffsetAndMetadata> synced = new HashMap<>();
        synchronized (topicPartitionOffset) {
            for (TopicPartition topicPartition : offsets.keySet()) {
                synced.put(topicPartition, new OffsetAndMetadata(topicPartitionOffset.get(topicPartition.topic()).get(topicPartition.partition())));
            }
        }
        return synced;
    }

    @Override
    public void stop() {
        LOG.info("Starrocks sink task stopped. version is " + Util.VERSION);
    }
}
