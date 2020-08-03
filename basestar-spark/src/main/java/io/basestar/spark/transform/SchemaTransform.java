package io.basestar.spark.transform;

/*-
 * #%L
 * basestar-spark
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

import com.google.common.collect.ImmutableSet;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Nullsafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nullable;
import java.util.Map;

@Slf4j
public class SchemaTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final InstanceSchema schema;

    private final Map<String, Use<?>> extraMetadata;

    private final StructType structType;

    @lombok.Builder(builderClassName = "Builder")
    SchemaTransform(final InstanceSchema schema, final Map<String, Use<?>> extraMetadata, @Nullable final StructType structType) {

        this.schema = Nullsafe.require(schema);
        this.extraMetadata = Nullsafe.option(extraMetadata);
        this.structType = Nullsafe.option(structType, () -> SparkSchemaUtils.structType(this.schema, ImmutableSet.of(), this.extraMetadata));
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return input.map((MapFunction<Row, Row>) row -> {
            final Map<String, Object> object = schema.create(SparkSchemaUtils.fromSpark(schema, row), false, false);
            return SparkSchemaUtils.toSpark(schema, extraMetadata, structType, object);
        }, RowEncoder.apply(structType));
    }
}
