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
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.migration.Migration;
import io.basestar.schema.use.UseAny;
import io.basestar.spark.util.NamingConvention;
import io.basestar.spark.util.SparkSchemaUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class MigrateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ObjectSchema targetSchema;

    private final Migration migration;

    public MigrateTransform(final ObjectSchema targetSchema, final Migration migration) {

        this.targetSchema = targetSchema;
        this.migration = migration;
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> df) {

        final ObjectSchema targetSchema = this.targetSchema;
        final StructType targetType = SparkSchemaUtils.structType(targetSchema, ImmutableSet.of());
        return df.map((MapFunction<Row, Row>) row -> {

            @SuppressWarnings("unchecked")
            final Map<String, Object> initial = (Map<String, Object>)SparkSchemaUtils.fromSpark(UseAny.DEFAULT, NamingConvention.DEFAULT, null, row);
            final Map<String, Object> migrated = migration.migrate(targetSchema, initial);
            return SparkSchemaUtils.toSpark(targetSchema, targetSchema.getExpand(), targetType, migrated);

        }, RowEncoder.apply(targetType));
    }
}
