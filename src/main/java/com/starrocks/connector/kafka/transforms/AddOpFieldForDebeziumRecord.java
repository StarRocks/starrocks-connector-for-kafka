/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.starrocks.connector.kafka.transforms;

import io.debezium.config.Configuration;
import io.debezium.transforms.SmtManager;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

// This class is a transform class that users use when they need load debezium data and SR's table model is PK.
// Note that this transform is used in conjunction with the ExtractNewRecordState transform provided by debezium.
// A configuration example is as follows:
// transforms=addfield,unwrap
// transforms.addfield.type=com.starrocks.connector.kafka.transforms.AddOpFieldForDebeziumRecord
// transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
// transforms.unwrap.drop.tombstones=true
// transforms.unwrap.delete.handling.mode=rewrite
public class AddOpFieldForDebeziumRecord<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOG = LoggerFactory.getLogger(AddOpFieldForDebeziumRecord.class);
    private SmtManager<R> smtManager;
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String OP_FIELD_NAME = "__op";
    private static final String OP = "op";
    private static final String OP_C = "c";
    private static final String OP_U = "u";
    private static final String OP_D = "d";
    private static final String OP_R = "r";
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }
        if (!smtManager.isValidEnvelope(record)) {
            return record;
        }
        if (!(record.value() instanceof Struct)) {
            return record;
        }
        Struct value = (Struct) record.value();
        // debezium data format:
        //  {
        //     op: "",
        //     after: {},
        //     before: {}
        //  }
        // Please reference to: https://debezium.io/documentation/reference/stable/connectors/mysql.html
        try {
            String op;
            try {
                op = (String) value.get(OP);
            } catch (Exception e) {
                LOG.debug("Expected operation key: `{}` in record but none was found", OP);
                return record;
            }
            if (op.equals(OP_C) || op.equals(OP_U) || op.equals(OP_R)) {
                Struct newValue = updateValue(value, AFTER);
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(), newValue, record.timestamp());
            } else if (op.equals(OP_D)) {
                Struct newValue = updateValue(value, BEFORE);
                return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), newValue.schema(), newValue, record.timestamp());
            }
            LOG.info("Operation type `{}` is not supported by this transform Class. Returning original record", op);
        } catch (Exception e) {
            return record;
        }
        return record;
    }

    private Struct updateValue(Struct value, String nestStructName) {
        if (!(value.get(nestStructName) instanceof Struct)) {
            throw new DataException("");
        }
        Struct nest = (Struct) value.get(nestStructName);
        Schema newNestSchema = makeUpdatedSchema(nest.schema());
        final Struct newNest = new Struct(newNestSchema);
        for (Field field : nest.schema().fields()) {
            newNest.put(field.name(), nest.get(field));
        }
        // Please reference to: https://docs.starrocks.io/zh-cn/latest/loading/Load_to_Primary_Key_tables
        if (nestStructName.equals(AFTER)) {
            newNest.put(OP_FIELD_NAME, 0);
        } else {
            newNest.put(OP_FIELD_NAME, 1);
        }
        Schema newValueSchema = makeUpdatedSchema(value.schema(), nestStructName, newNestSchema);
        final Struct newValue = new Struct(newValueSchema);
        for (Field field : newValueSchema.schema().fields()) {
            if (field.name().equals(nestStructName)) {
                newValue.put(field.name(), newNest);
            } else {
                if (value.get(field) == null) {
                    continue;
                }
                newValue.put(field.name(), value.get(field));
            }
        }
        return newValue;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        Schema updatedSchema = schemaUpdateCache.get(schema);
        if(updatedSchema == null) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
            for (Field field: schema.fields()) {
                builder.field(field.name(), field.schema());
            }
            builder.field(OP_FIELD_NAME, Schema.INT32_SCHEMA);
            updatedSchema = builder.build();
            schemaUpdateCache.put(schema, updatedSchema);
        }
        return updatedSchema;
    }

    private Schema makeUpdatedSchema(Schema schema, String fieldName, Schema fieldSchema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            if (field.name().equals(fieldName)) {
                builder.field(fieldName, fieldSchema);
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        smtManager = new SmtManager<>(config);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }
}
