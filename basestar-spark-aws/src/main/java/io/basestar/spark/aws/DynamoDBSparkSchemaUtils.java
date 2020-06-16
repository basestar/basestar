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
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.spark.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBLegacyUtils;
import io.basestar.storage.dynamodb.DynamoDBRouting;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

@Slf4j
public class DynamoDBSparkSchemaUtils {

    public static StructType streamRecordStructType(final ObjectSchema schema) {

        final StructType type = SparkSchemaUtils.structType(schema, null);
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
        index.projectionSchema(schema).forEach((name, type) -> fields.add(SparkSchemaUtils.field(name, type, null)));
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

    public static Map<String, AttributeValue> tombstone(final Map<String, AttributeValue> before) {

        final String schema = DynamoDBLegacyUtils.schema(before);
        final String id = DynamoDBLegacyUtils.id(before);
        final Long version = DynamoDBLegacyUtils.version(before);
        assert schema != null && id != null && version != null;
        return ImmutableMap.of(
                Reserved.SCHEMA, new AttributeValue().withS(schema),
                Reserved.ID, new AttributeValue().withS(id),
                Reserved.VERSION, new AttributeValue().withN(Long.toString(version + 1)),
                Reserved.DELETED, new AttributeValue().withBOOL(true)
        );
    }
}
