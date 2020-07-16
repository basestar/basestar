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
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBLegacyUtils;
import io.basestar.util.Nullsafe;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

public class DynamoDBDatasetOutputTransform implements Transform<Dataset<Row>, RDD<Map<String, AttributeValue>>> {

    private final InstanceSchema schema;

    private final Map<String, Use<?>> extraMetadata;

    @lombok.Builder(builderClassName = "Builder")
    DynamoDBDatasetOutputTransform(final InstanceSchema schema, final Map<String, Use<?>> extraMetadata) {

        this.schema = Nullsafe.require(schema);
        this.extraMetadata = Nullsafe.option(extraMetadata);
    }

    @Override
    public RDD<Map<String, AttributeValue>> accept(final Dataset<Row> input) {

        return input.toJavaRDD().map(row -> {
            final Map<String, Object> data = SparkSchemaUtils.fromSpark(schema, extraMetadata, row);
            return DynamoDBLegacyUtils.toItem(data);
        }).rdd();
    }
}
