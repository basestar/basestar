package io.basestar.spark.aws;

/*-
 * #%L
 * basestar-spark-aws
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import io.basestar.spark.SparkUtils;
import io.basestar.spark.Transform;
import io.basestar.storage.dynamodb.DynamoDBRouting;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class DynamoDBIndexTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final DynamoDBRouting routing;

    private final ObjectSchema schema;

    private final Index index;

    public DynamoDBIndexTransform(final DynamoDBRouting routing, final ObjectSchema schema, final Index index) {

        this.routing = routing;
        this.schema = schema;
        this.index = index;
    }

    @Override
    public Dataset<Row> apply(final Dataset<Row> df) {

        final StructType structType = DynamoDBSparkUtils.type(routing, schema, index);

        return df.flatMap((FlatMapFunction<Row, Row>) row -> {

            final Map<String, Object> initial = SparkUtils.fromSpark(schema, row);
            final String id = Instance.getId(initial);
            final Map<Index.Key, Map<String, Object>> records = index.readValues(initial);

            return records.entrySet().stream()
                    .map(e -> DynamoDBSparkUtils.toSpark(routing, schema, index, structType, id, e.getKey(), e.getValue()))
                    .iterator();

        }, RowEncoder.apply(structType));
    }
}
