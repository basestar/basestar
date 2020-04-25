package io.basestar.spark.aws;

/*-
 * #%L
 * basestar-spark-aws
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.spark.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBRouting;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class DynamoDBSparkSchemaUtils {

    public static StructType streamRecordStructType(final ObjectSchema schema) {

        final StructType type = SparkSchemaUtils.structType(schema);
        final List<StructField> fields = new ArrayList<>();
        fields.add(SparkSchemaUtils.field(Reserved.SCHEMA, DataTypes.StringType));
        fields.add(SparkSchemaUtils.field(Reserved.ID, DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("eventName", DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("sequenceNumber", DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("oldImage", type));
        fields.add(SparkSchemaUtils.field("newImage", type));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static StructType type(final DynamoDBRouting routing, final ObjectSchema schema, final Index index) {

        final List<StructField> fields = new ArrayList<>();
        index.projectionSchema(schema).forEach((name, type) -> fields.add(SparkSchemaUtils.field(name, type)));
        fields.add(SparkSchemaUtils.field(routing.indexPartitionName(schema, index), DataTypes.BinaryType));
        fields.add(SparkSchemaUtils.field(routing.indexSortName(schema, index), DataTypes.BinaryType));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static Row toSpark(final DynamoDBRouting routing, final ObjectSchema schema, final Index index,
                              final StructType structType, final String id, final Index.Key key,
                              final Map<String, Object> projection) {

        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        final byte[] partition = DynamoDBStorage.partition(routing, schema, index, id, key.getPartition());
        final byte[] sort = DynamoDBStorage.sort(schema, index, id, key.getSort());
        values[structType.fieldIndex(routing.indexPartitionName(schema, index))] = partition;
        values[structType.fieldIndex(routing.indexSortName(schema, index))] = sort;
        index.projectionSchema(schema).forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = SparkSchemaUtils.toSpark(type, fields[i].dataType(), projection.get(name));
        });
        Arrays.sort(fields, Comparator.comparing(StructField::name));
        return new GenericRowWithSchema(values, structType);
    }

    public static Map<String, Object> fromDynamoDB(final Map<String, AttributeValue> values) {

        final Map<String, Object> result = new HashMap<>();
        values.forEach((k, v) -> result.put(k, fromDynamoDB(v)));
        return result;
    }

    public static Object fromDynamoDB(final AttributeValue value) {

        if(value == null || value.isNULL() != null) {
            return null;
        } else if(value.isBOOL() != null) {
            return value.getBOOL();
        } else if(value.getN() != null) {
            return parseNumber(value.getN());
        } else if(value.getS() != null) {
            return value.getS();
        } else if(value.getB() != null) {
            return value.getB().array();
        } else if(value.getSS() != null) {
            return ImmutableSet.copyOf(value.getSS());
        } else if(value.getNS() != null) {
            return value.getNS().stream().map(DynamoDBSparkSchemaUtils::parseNumber)
                    .collect(Collectors.toSet());
        } else if(value.getBS() != null) {
            return value.getBS().stream().map(ByteBuffer::array)
                    .collect(Collectors.toSet());
        } else if(value.getL() != null) {
            return value.getL().stream().map(DynamoDBSparkSchemaUtils::fromDynamoDB)
                    .collect(Collectors.toList());
        } else if(value.getM() != null) {
            final Map<String, Object> result = new HashMap<>();
            value.getM().forEach((k, v) -> result.put(k, fromDynamoDB(v)));
            return result;
        } else {
            log.error("Got an ambiguous empty item, returning null");
            return null;
        }
    }

    private static Number parseNumber(final String str) {

        if(str.contains(".")) {
            return Double.valueOf(str);
        } else {
            return Long.valueOf(str);
        }
    }

    public static Map<String, AttributeValue> toDynamoDB(final Map<String, Object> values) {

        return values.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> toDynamoDB(entry.getValue())));
    }
    
    public static AttributeValue toDynamoDB(final Object value) {

        if(value == null) {
            return new AttributeValue().withNULL(true);
        } else if(value instanceof Boolean) {
            return new AttributeValue().withBOOL((Boolean)value);
        } else if(value instanceof Number) {
            return new AttributeValue().withN(value.toString());
        } else if(value instanceof String) {
            return new AttributeValue().withS(value.toString());
        } else if(value instanceof byte[]) {
            return new AttributeValue().withB(ByteBuffer.wrap((byte[])value));
        } else if(value instanceof Collection) {
            return new AttributeValue().withL(((Collection<?>)value).stream().map(DynamoDBSparkSchemaUtils::toDynamoDB)
                            .collect(Collectors.toList()));
        } else if(value instanceof Map) {
            return new AttributeValue().withM(((Map<?, ?>)value).entrySet().stream()
                            .collect(Collectors.toMap(
                                    entry -> entry.getKey().toString(),
                                    entry -> toDynamoDB(entry.getValue()))));
        } else {
            throw new IllegalStateException();
        }
    }

    public static String id(final Map<String, AttributeValue> values) {

        return (String)fromDynamoDB(values.get(Reserved.ID));
    }

    public static Long version(final Map<String, AttributeValue> values) {

        return (Long)fromDynamoDB(values.get(Reserved.VERSION));
    }

    public static String schema(final Map<String, AttributeValue> values) {

        return (String)fromDynamoDB(values.get(Reserved.SCHEMA));
    }

    public static Map<String, AttributeValue> tombstone(final Map<String, AttributeValue> before) {

        final String schema = DynamoDBSparkSchemaUtils.schema(before);
        final String id = DynamoDBSparkSchemaUtils.id(before);
        final Long version = DynamoDBSparkSchemaUtils.version(before);
        assert schema != null && id != null && version != null;
        return ImmutableMap.of(
                Reserved.SCHEMA, new AttributeValue().withS(schema),
                Reserved.ID, new AttributeValue().withS(id),
                Reserved.VERSION, new AttributeValue().withN(Long.toString(version + 1)),
                Reserved.DELETED, new AttributeValue().withBOOL(true)
        );
    }
}
