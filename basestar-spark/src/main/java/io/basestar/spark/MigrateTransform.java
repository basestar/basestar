package io.basestar.spark;

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
