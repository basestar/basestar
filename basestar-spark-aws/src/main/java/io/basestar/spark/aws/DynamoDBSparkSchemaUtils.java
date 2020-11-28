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
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBLegacyUtils;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBStrategy;
import io.basestar.util.Name;
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

        final StructType type = SparkSchemaUtils.structType(schema, ImmutableSet.of());
        final List<StructField> fields = new ArrayList<>();
        fields.add(SparkSchemaUtils.field(ObjectSchema.SCHEMA, DataTypes.StringType));
        fields.add(SparkSchemaUtils.field(ObjectSchema.ID, DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("eventName", DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("sequenceNumber", DataTypes.StringType));
        fields.add(SparkSchemaUtils.field("oldImage", type));
        fields.add(SparkSchemaUtils.field("newImage", type));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static StructType type(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index) {

        final List<StructField> fields = new ArrayList<>();
        index.projectionSchema(schema).forEach((name, type) -> fields.add(SparkSchemaUtils.field(name, type, null)));
        fields.add(SparkSchemaUtils.field(strategy.indexPartitionName(schema, index), DataTypes.BinaryType));
        fields.add(SparkSchemaUtils.field(strategy.indexSortName(schema, index), DataTypes.BinaryType));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static Row toSpark(final DynamoDBStrategy strategy, final ObjectSchema schema, final Index index,
                              final StructType structType, final String id, final Index.Key.Binary key,
                              final Map<String, Object> projection) {

        final Map<String, Set<Name>> branches = Name.branch(schema.getExpand());
        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        final byte[] partition = DynamoDBStorage.partition(strategy, schema, index, id, key.getPartition());
        final byte[] sort = DynamoDBStorage.sort(schema, index, id, key.getSort());
        values[structType.fieldIndex(strategy.indexPartitionName(schema, index))] = partition;
        values[structType.fieldIndex(strategy.indexSortName(schema, index))] = sort;
        index.projectionSchema(schema).forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = SparkSchemaUtils.toSpark(type, branches.get(name), fields[i].dataType(), projection.get(name));
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
                ObjectSchema.SCHEMA, new AttributeValue().withS(schema),
                ObjectSchema.ID, new AttributeValue().withS(id),
                ObjectSchema.VERSION, new AttributeValue().withN(Long.toString(version + 1)),
                Reserved.DELETED, new AttributeValue().withBOOL(true)
        );
    }
}
