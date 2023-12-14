package com.starrocks.connector.kafka;
import com.starrocks.connector.kafka.json.JsonConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
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

import java.util.Collections;
import java.util.Map;

public class StarRocksSinkTaskTest {

    private static final Logger LOG = LoggerFactory.getLogger(StarRocksSinkTaskTest.class);

    private static final String TOPIC = "test_topic";

    final Schema recordSchema = SchemaBuilder.struct()
            .field("id", SchemaBuilder.int8())
            .field("name", SchemaBuilder.string().optional())
            .build();

    private SinkRecord createCreateRecord() {
        final Struct before = new Struct(recordSchema);
        before.put("id", (byte) 1);
        before.put("name", "myRecord");
        return new SinkRecord("dummy-topic", 0, null, null, recordSchema, before, 0);
    }

    private SchemaAndValue getConnectDataFromStr(String msg) {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, ?> conf = Collections.emptyMap();
        jsonConverter.configure(conf, false);
        return jsonConverter.toConnectData(TOPIC, msg.getBytes());
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
            Assert.assertEquals(true, errMsg.contains("cannot be cast to"));
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
            JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
            sinkTask.setJsonConverter(jsonConverter);
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            String value = "{\"name\":\"北京\",\"code\":1}";
            SinkRecord sinkRecord = new SinkRecord("dummy-topic", 0, null, null, null, value, 0);
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals("\"{\\\"name\\\":\\\"北京\\\",\\\"code\\\":1}\"", row);
        }

        {
            JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
            sinkTask.setJsonConverter(jsonConverter);
            sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
            SinkRecord sinkRecord = createCreateRecord();
            String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
            Assert.assertEquals("{\"id\":1,\"name\":\"myRecord\"}", row);
        }
    }

    @Test
    public void testDecimalRecord() {
        // BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        // Payload is base64 encoded byte[]{0, -100}, which is the two's complement encoding of 156.
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }, \"payload\": \"AJw=\" }";
        SchemaAndValue schemaAndValue = getConnectDataFromStr(msg);
        SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        sinkTask.setJsonConverter(jsonConverter);
        sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
        String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
        Assert.assertEquals("1.56", row);
    }

    @Test
    public void testNullRecord() {
        StarRocksSinkTask sinkTask = new StarRocksSinkTask();
        JsonConverter jsonConverter = StarRocksSinkTask.createJsonConverter();
        sinkTask.setJsonConverter(jsonConverter);
        sinkTask.setSinkType(StarRocksSinkTask.SinkType.JSON);
        final Struct value = new Struct(recordSchema);
        value.put("id", (byte) 1);
        value.put("name", null);
        SinkRecord sinkRecord = new SinkRecord(TOPIC, 0, null, null, recordSchema, value, 0);
        String row = sinkTask.getRecordFromSinkRecord(sinkRecord);
        Assert.assertEquals("{\"id\":1,\"name\":null}", row);
    }
}
