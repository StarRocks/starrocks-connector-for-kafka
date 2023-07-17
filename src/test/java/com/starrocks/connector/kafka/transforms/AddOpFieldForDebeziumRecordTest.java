package com.starrocks.connector.kafka.transforms;

import io.debezium.data.Envelope;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.Test;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class AddOpFieldForDebeziumRecordTest {
    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string())
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", SchemaBuilder.int32())
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(recordSchema)
            .withSource(sourceSchema)
            .build();

    private SinkRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        final Struct payload = envelope.create(before, source, Instant.now());
        return new SinkRecord("dummy-topic", 0, null, null, envelope.schema(), payload, 0);
    }

    private SinkRecord createUpdateRecord() {
        final Struct before = new Struct(recordSchema);
        final Struct after = new Struct(recordSchema);
        final Struct source = new Struct(sourceSchema);
        final Struct transaction = new Struct(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        after.put("id", (byte) 1);
        after.put("name", "updatedRecord");
        source.put("lsn", 1234);
        transaction.put("id", "571");
        transaction.put("total_order", 42L);
        transaction.put("data_collection_order", 42L);
        final Struct payload = envelope.update(before, after, source, Instant.now());
        payload.put("transaction", transaction);
        return new SinkRecord("dummy-topic", 0, null, null, envelope.schema(), payload, 0);
    }

    private SinkRecord createDeleteRecord() {
        final Schema deleteSourceSchema = SchemaBuilder.struct()
                .field("lsn", SchemaBuilder.int32())
                .field("version", SchemaBuilder.string())
                .build();

        Envelope deleteEnvelope = Envelope.defineSchema()
                .withName("dummy.Envelope")
                .withRecord(recordSchema)
                .withSource(deleteSourceSchema)
                .build();

        final Struct before = new Struct(recordSchema);
        final Struct source = new Struct(deleteSourceSchema);

        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        source.put("lsn", 1234);
        source.put("version", "version!");
        final Struct payload = deleteEnvelope.delete(before, source, Instant.now());
        return new SinkRecord("dummy-topic", 0, null, null, envelope.schema(), payload, 0);
    }

    @Test
    public void applyUpdateRecordTest() {
        AddOpFieldForDebeziumRecord<SinkRecord> transform = new AddOpFieldForDebeziumRecord<>();
        final Map<String, String> props = new HashMap<>();
        transform.configure(props);

        SinkRecord originRecord = createUpdateRecord();
        SinkRecord record = transform.apply(originRecord);
        Assert.assertEquals("0", ((Struct) ((Struct) record.value()).get("after")).get("__op").toString());
    }

    @Test
    public void applyCreateRecordTest() {
        AddOpFieldForDebeziumRecord<SinkRecord> transform = new AddOpFieldForDebeziumRecord<>();
        final Map<String, String> props = new HashMap<>();
        transform.configure(props);

        SinkRecord originRecord = createCreateRecord();
        SinkRecord record = transform.apply(originRecord);
        Assert.assertEquals("0", ((Struct) ((Struct) record.value()).get("after")).get("__op").toString());
    }

    @Test
    public void applyDeleteRecordTest() {
        AddOpFieldForDebeziumRecord<SinkRecord> transform = new AddOpFieldForDebeziumRecord<>();
        final Map<String, String> props = new HashMap<>();
        transform.configure(props);

        SinkRecord originRecord = createDeleteRecord();
        SinkRecord record = transform.apply(originRecord);
        Assert.assertEquals("1", ((Struct) ((Struct) record.value()).get("before")).get("__op").toString());
    }
}
