package com.starrocks.connector.kafka;

import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarRocksSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnectorConfig.class);

    public static final String CONFIG_GROUP_1 = "config_group1";
    public static final String STARROCKS_LOAD_URL = "starrocks.load.url";
    public static final String STARROCKS_DATABASE_NAME = "starrocks.database.name";
    public static final String SINK_FORMAT = "sink.format";
    public static final String SINK_PROPERTIES_ROW_DELIMITER = "sink.properties.row_delimiter";
    public static final String BUFFERFLUSH_MAXBYTES = "bufferflush.maxbytes";
    public static final String CONNECT_TIMEOUTMS = "connect.timeoutms";
    public static final String STARROCKS_USERNAME = "starrocks.username";
    public static final String STARROCKS_PASSWORD = "starrocks.password";
    public static final String BUFFERFLUSH_INTERVALMS = "bufferflush.intervalms";
    public static final String SINK_PROPERTIES_PREFIX = "sink.properties.";
    public static final String STARROCKS_TOPIC2TABLE_MAP = "starrocks.topic2table.map";

    public static final String[] mustRequiredConfigs = {
            STARROCKS_LOAD_URL,
            STARROCKS_DATABASE_NAME,
            STARROCKS_USERNAME,
            STARROCKS_PASSWORD
    };

//    经过测试，这里定义了connector的配置项的信息。
//    但是对于用户指定的配置文件来说，一个配置项你可以不指定，但是指定以后会通过ConfigDef定义的每个item做校验

//    根据这个语义，我们需要做的是：
//    1. 通过ConfigDef来规定每个配置项的范围
//    2. 额外加一个逻辑来判断配置项是否存在
//    3. 未定义的默认值应该如何处理
//    TODO: here. 2023-6-7-21:25
    public static ConfigDef newConfigDef() {
        return new ConfigDef()
                .define(
                        STARROCKS_LOAD_URL,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks load url",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_LOAD_URL
                ).define(
                        STARROCKS_DATABASE_NAME,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks datbase name",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_DATABASE_NAME
                ).define(
                        SINK_FORMAT,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.LOW,
                        "write to starrocks data format",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        SINK_FORMAT
                ).define(
                        BUFFERFLUSH_MAXBYTES,
                        ConfigDef.Type.LONG,
                        67108864,
                        ConfigDef.Range.between(67108864, 10737418240L),
                        ConfigDef.Importance.LOW,
                        "the size of a batch of data",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        BUFFERFLUSH_MAXBYTES
                ).define(
                        CONNECT_TIMEOUTMS,
                        ConfigDef.Type.LONG,
                        100,
                        ConfigDef.Range.between(100, 60000),
                        ConfigDef.Importance.LOW,
                        "timeout period for connecting to load-url",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        CONNECT_TIMEOUTMS
                ).define(
                        STARROCKS_USERNAME,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks username",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_USERNAME
                ).define(
                        STARROCKS_PASSWORD,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.HIGH,
                        "starrocks password",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_PASSWORD
                ).define(
                        BUFFERFLUSH_INTERVALMS,
                        ConfigDef.Type.LONG,
                        1000,
                        ConfigDef.Range.between(1000, 3600000),
                        ConfigDef.Importance.LOW,
                        "the interval at which data is sent in bulk to starrocks",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        BUFFERFLUSH_INTERVALMS
                ).define(
                        STARROCKS_TOPIC2TABLE_MAP,
                        ConfigDef.Type.STRING,
                        null,
                        new ConfigDef.NonEmptyString(),
                        ConfigDef.Importance.LOW,
                        "a mapping between the topic name and the table name",
                        CONFIG_GROUP_1,
                        0,
                        ConfigDef.Width.NONE,
                        STARROCKS_TOPIC2TABLE_MAP
                );
    }
}
