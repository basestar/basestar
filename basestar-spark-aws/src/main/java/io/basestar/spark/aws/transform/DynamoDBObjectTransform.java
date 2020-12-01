package io.basestar.spark.aws.transform;

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
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBLegacyUtils;
import io.basestar.storage.dynamodb.DynamoDBStorage;
import io.basestar.storage.dynamodb.DynamoDBStrategy;
import io.basestar.util.Nullsafe;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class DynamoDBObjectTransform implements Transform<Dataset<Row>, RDD<Map<String, AttributeValue>>> {

    private final DynamoDBStrategy strategy;

    private final ObjectSchema schema;

    private final Format format;

    private final Map<String, Use<?>> extraMetadata;

    public interface Format {

        Map<String, AttributeValue> item(DynamoDBStrategy strategy, ObjectSchema schema, Map<String, Object> data);

        Format ITEM = (strategy, schema, data) -> {

            final String id = Instance.getId(data);
            return DynamoDBLegacyUtils.toLegacy(DynamoDBStorage.objectItem(strategy, schema, id, data));
        };

        Format OBJECT_KEY = (strategy, schema, data) -> {

            final String id = Instance.getId(data);
            return DynamoDBLegacyUtils.toLegacy(DynamoDBStorage.objectKey(strategy, schema, id));
        };

        Format HISTORY_KEY = (strategy, schema, data) -> {

            final String id = Instance.getId(data);
            final Long version = Instance.getVersion(data);
            return DynamoDBLegacyUtils.toLegacy(DynamoDBStorage.historyKey(strategy, schema, id, version));
        };
    }

    @lombok.Builder(builderClassName = "Builder")
    DynamoDBObjectTransform(final DynamoDBStrategy strategy, final ObjectSchema schema, final Format format,
                            final Map<String, Use<?>> extraMetadata) {

        this.strategy = Nullsafe.require(strategy);
        this.schema = Nullsafe.require(schema);
        this.format = Nullsafe.orDefault(format, Format.ITEM);
        this.extraMetadata = Nullsafe.orDefault(extraMetadata);
    }

    @Override
    public RDD<Map<String, AttributeValue>> accept(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> {
            final Map<String, Object> data = SparkSchemaUtils.fromSpark(schema, schema.getExpand(), extraMetadata, row);
            return format.item(strategy, schema, data);
        }).rdd();
    }
}
