package io.basestar.spark;

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

import io.basestar.schema.ObjectSchema;
import io.basestar.schema.migration.SchemaMigration;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class MigrateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ObjectSchema sourceSchema;

    private final ObjectSchema targetSchema;

    private final SchemaMigration migration;

    public MigrateTransform(final ObjectSchema sourceSchema, final ObjectSchema targetSchema, final SchemaMigration migration) {

        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.migration = migration;
    }

    @Override
    public Dataset<Row> apply(final Dataset<Row> df) {

        final StructType targetType = SparkUtils.structType(targetSchema);
        return df.map((MapFunction<Row, Row>) row -> {

            final Map<String, Object> initial = SparkUtils.fromSpark(sourceSchema, row);
            final Map<String, Object> migrated = migration.migrate(sourceSchema, targetSchema, initial);
            return SparkUtils.toSpark(targetSchema, targetType, migrated);

        }, RowEncoder.apply(targetType));
    }
}
