package com.starrocks.connector.kafka.envelope;

import io.debezium.transforms.ConnectRecordUtil;
import io.debezium.transforms.ExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition.DeleteTombstoneHandling;
import io.debezium.transforms.extractnewstate.DefaultDeleteHandlingStrategy;
import org.apache.kafka.connect.connector.ConnectRecord;

import java.util.Map;

import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.HANDLE_TOMBSTONE_DELETES;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.REPLACE_NULL_WITH_DEFAULT;

public class DebeziumEnvelope<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        // override the default delete handling strategy
        // by swapping `__deleted` out for `__op`
        DeleteTombstoneHandling deleteTombstoneHandling = DeleteTombstoneHandling.parse(config.getString(HANDLE_TOMBSTONE_DELETES));
        extractRecordStrategy = new DeleteHandlingStrategy<>(
                deleteTombstoneHandling,
                config.getBoolean(REPLACE_NULL_WITH_DEFAULT)
        );
    }

    private static class DeleteHandlingStrategy<R extends ConnectRecord<R>> extends DefaultDeleteHandlingStrategy<R> {
        public static final String OP_FIELD_NAME = "__op";

        public DeleteHandlingStrategy(DeleteTombstoneHandling deleteTombstoneHandling, boolean replaceNullWithDefault) {
            super(deleteTombstoneHandling, replaceNullWithDefault);

            removedDelegate = ConnectRecordUtil.insertStaticValueDelegate(OP_FIELD_NAME, "1", replaceNullWithDefault);
            updatedDelegate = ConnectRecordUtil.insertStaticValueDelegate(OP_FIELD_NAME, "0", replaceNullWithDefault);
        }
    }
}
