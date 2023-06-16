package com.starrocks.connector.kafka.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JsonConverterTest {
    private SchemaAndValue getSchemaAndValueFromJsonStr(String jsonStr) throws JsonProcessingException {
        JsonConverter jsonConverter = new JsonConverter();
        Map<String, Object> props = new HashMap<>();
        props.put("schemas.enable", (Object) false);
        jsonConverter.configure(props, false);
        JsonSerializer jsonSerializer = jsonConverter.getSerializer();
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNodeSource = objectMapper.readTree(jsonStr);
        byte[] jsonBytes = jsonSerializer.serialize("", jsonNodeSource);
        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("", jsonBytes);
        return schemaAndValue;
    }

    @Test
    public void convertToJson() throws JsonProcessingException {
        String jsonStr = "{\"elements\":[{\"elName\":\"zll\",\"age\":1},{\"elName\":\"zll1\",\"age\":2}],\"name\":\"haha\",\"id\":1}";
        SchemaAndValue schemaAndValue = getSchemaAndValueFromJsonStr(jsonStr);
        JsonConverter jsonConverter = new JsonConverter();
        JsonNode jsonNodeDest = jsonConverter.convertToJson(schemaAndValue.schema(), schemaAndValue.value());
        System.out.println(jsonNodeDest.toString());
        Assert.assertEquals(jsonStr, jsonNodeDest.toString());
    }
}
