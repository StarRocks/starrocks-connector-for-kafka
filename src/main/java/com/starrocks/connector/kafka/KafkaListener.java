package com.starrocks.connector.kafka;

import com.starrocks.data.load.stream.CommitListener;
import com.starrocks.data.load.stream.Meta;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class KafkaListener implements CommitListener {
    private  HashMap<String, HashMap<Integer, Long>> topicPartitionOffset;
    public  KafkaListener(HashMap<String, HashMap<Integer, Long>> topicPartitionOffset) {
        this.topicPartitionOffset = topicPartitionOffset;
    }
    @Override
    public boolean afterCommit(List<Meta> metaDatas) {
        Iterator<Meta> it = metaDatas.iterator();
        synchronized (topicPartitionOffset) {
            while (it.hasNext()) {
                final KafkaMeta meta = (KafkaMeta) it.next();
                if (topicPartitionOffset.get(meta.topic).get(meta.partition) == null || topicPartitionOffset.get(meta.topic).get(meta.partition) < meta.offset) {
                    topicPartitionOffset.get(meta.topic).put(meta.partition, meta.offset);
                }
            }
        }
        return true;
    }
}
