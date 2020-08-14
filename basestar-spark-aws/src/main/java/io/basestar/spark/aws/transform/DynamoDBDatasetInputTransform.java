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
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.storage.dynamodb.DynamoDBLegacyUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class DynamoDBDatasetInputTransform implements Transform<RDD<Map<String, AttributeValue>>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Set<Name> expand;

    private final Map<String, Use<?>> extraMetadata;

    private final StructType structType;

    @lombok.Builder(builderClassName = "Builder")
    public DynamoDBDatasetInputTransform(final InstanceSchema schema, final Map<String, Use<?>> extraMetadata) {

        this.schema = Nullsafe.require(schema);
        if(this.schema instanceof LinkableSchema) {
            this.expand = ((LinkableSchema) this.schema).getExpand();
        } else {
            this.expand = Collections.emptySet();
        }
        this.extraMetadata = Nullsafe.option(extraMetadata);
        this.structType = SparkSchemaUtils.structType(this.schema, this.expand, this.extraMetadata);
    }

    @Override
    public Dataset<Row> accept(final RDD<Map<String, AttributeValue>> input) {

        final InstanceSchema schema = this.schema;
        final Set<Name> expand = this.expand;
        final Map<String, Use<?>> extraMetadata = this.extraMetadata;
        final StructType structType = this.structType;
        final SQLContext sqlContext = SQLContext.getOrCreate(input.sparkContext());
        return sqlContext.createDataFrame(input.toJavaRDD()
                .map(values -> {
                    final Map<String, Object> data = DynamoDBLegacyUtils.fromItem(values);
                    return SparkSchemaUtils.toSpark(schema, expand, extraMetadata, structType, data);
                }), structType);
    }
}
