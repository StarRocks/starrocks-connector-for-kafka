package com.starrocks.connector.kafka;

import com.starrocks.data.load.stream.Meta;

public class KafkaMeta extends Meta {
    String topic;
    int partition;
    long offset;

    public KafkaMeta(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }
}
