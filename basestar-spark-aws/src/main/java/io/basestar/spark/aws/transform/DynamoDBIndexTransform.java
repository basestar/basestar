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

import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.spark.aws.DynamoDBSparkSchemaUtils;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBStrategy;
import lombok.Builder;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

/**
 * Be careful when using this with mutable index keys
 */

@Builder(builderClassName = "Builder")
public class DynamoDBIndexTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final DynamoDBStrategy strategy;

    private final ObjectSchema schema;

    private final Index index;

    @Override
    public Dataset<Row> accept(final Dataset<Row> df) {

        final StructType structType = DynamoDBSparkSchemaUtils.type(strategy, schema, index);

        return df.flatMap((FlatMapFunction<Row, Row>) row -> {

            final Map<String, Object> initial = SparkSchemaUtils.fromSpark(schema, row);
            final String id = Instance.getId(initial);
            final Map<Index.Key, Map<String, Object>> records = index.readValues(initial);

            return records.entrySet().stream()
                    .map(e -> DynamoDBSparkSchemaUtils.toSpark(strategy, schema, index, structType, id, e.getKey(), e.getValue()))
                    .iterator();

        }, RowEncoder.apply(structType));
    }
}
