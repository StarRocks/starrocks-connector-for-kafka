package com.starrocks.connector.kafka;
import com.starrocks.connector.kafka.json.JsonConverter;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class StarRocksSinkTaskTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTaskTest.class);

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string())
            .build();

    private SinkRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        return new SinkRecord("dummy-topic", 0, null, null, recordSchema, before, 0);
    }

    @Before
    public void setUp() {
        PropertyConfigurator.configure("src/test/conf/log4j.properties");
    }

    @Test
    public void testGetRecordFromSinkRecord() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = null;
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals(null, row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, null, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals(null, row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, "a,b,c", 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals("a,b,c", row);
        }

        {
            class Dummy {}
            Dummy dummy = new Dummy();
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.CSV);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, dummy, 0);
            String errMsg = "";
            try {
                String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            } catch (DataException e) {
                errMsg = e.getMessage();
            }
            Assert.assertEquals(true, errMsg.contains("cannot be cast to java.lang.String"));
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = null;
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals(null, row);
        }

        {
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, null, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals(null, row);
        }

        {
            sinkTask.setJsonConverter(new JsonConverter());
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            String value = "{\"name\":\"北京\",\"code\":1}";
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, value, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals("\"{\\\"name\\\":\\\"北京\\\",\\\"code\\\":1}\"", row);
        }

        {
            sinkTask.setJsonConverter(new JsonConverter());
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = createCreateRecord();
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals("{\"id\":1,\"name\":\"myRecord\"}", row);
        }
    }
}
