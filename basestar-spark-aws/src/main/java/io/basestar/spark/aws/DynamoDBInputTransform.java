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
import io.basestar.schema.InstanceSchema;
import io.basestar.spark.SparkUtils;
import io.basestar.spark.Transform;
import lombok.RequiredArgsConstructor;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

@RequiredArgsConstructor
public class DynamoDBInputTransform implements Transform<RDD<Map<String, AttributeValue>>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final StructType structType;

    public DynamoDBInputTransform(final InstanceSchema schema) {

        this.schema = schema;
        this.structType = DynamoDBSparkUtils.structType(schema);
    }

    @Override
    public Dataset<Row> apply(final RDD<Map<String, AttributeValue>> input) {

        final SQLContext sqlContext = new SQLContext(input.sparkContext());
        return sqlContext.createDataFrame(input.toJavaRDD()
                .map(values -> {
                    final Map<String, Object> data = DynamoDBSparkUtils.fromDynamoDB(values);
                    return SparkUtils.toSpark(schema, structType, data);
                }), structType);
    }
}
