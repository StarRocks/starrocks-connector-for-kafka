package com.starrocks.connector.kafka;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class StarRocksSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkConnector.class);
    private Map<String, String> config; // connector configuration, provided by

    /**
     * start method will only be called on a clean connector, i.e. it has either just been
     * instantiated and initialized or stop () has been invoked. loads configuration and validates.
     **
     * @param parsedConfig has the configuration settings
     */
    @Override
    public void start(final Map<String, String> parsedConfig) {
        config = new HashMap<>(parsedConfig);
        LOG.info("StarRocks sink connector started. version is " + Util.VERSION);
    }

    /**
     * Stop method will be called to stop a connector, cleans up snowflake internal stages, after
     * making sure that there are no pending files to ingest.
     */
    @Override
    public void stop() {
        LOG.info("StarRocks sink connector stopped. version is " + Util.VERSION);
    }

    // Default implementation shuts down all external network connections.
    // We can make it more efficient by identifying configuration changes,
    // creating new snowflake internal stages, new snowflake tables, new pipes,
    // for newly added topics;
    // and cleaning up stages for topics that are not in the new configuration,
    // and
    // cleaning up pipes for partitions that are not in the new configuration.

    /** @return Sink task class */
    @Override
    public Class<? extends Task> taskClass() {
        return StarRocksSinkTask.class;
    }

    /**
     * taskConfigs method returns a set of configurations for SinkTasks based on the current
     * configuration, producing at most 'maxTasks' configurations
     *
     * @param maxTasks maximum number of SinkTasks for this instance of SnowflakeSinkConnector
     * @return a list containing 'maxTasks' copies of the configuration
     */
    @Override
    public List<Map<String, String>> taskConfigs(final int maxTasks) {
        final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            configs.add(config);
        }
        return configs;
    }

    // 这个函数除了做config的validate以外，不会再做额外的事情
    // 比如，配置项从String类型转换成具体的实际类型。这些动作
    // 你还是需要再Task的start方法里做。
    /** @return ConfigDef with original configuration properties */
    @Override
    public ConfigDef config() {
        return StarRocksSinkConnectorConfig.newConfigDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config result = super.validate(connectorConfigs);
        for (String config : StarRocksSinkConnectorConfig.mustRequiredConfigs) {
            if (!connectorConfigs.containsKey(config)) {
                throw new RuntimeException("You must specify" + config);
            }
        }
        return result;
    }

        /** @return connector version */
    @Override
    public String version() {
        return Util.VERSION;
    }
}
