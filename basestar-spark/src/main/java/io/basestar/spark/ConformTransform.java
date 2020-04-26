package io.basestar.spark;

import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;


/**
 * Force the input to match the field order in the provided schema.
 */

public class ConformTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final StructType structType;

    @lombok.Builder(builderClassName = "Builder")
    public ConformTransform(final StructType structType) {

        this.structType = Nullsafe.require(structType);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        return input.map(ScalaUtils.scalaFunction(row -> SparkSchemaUtils.conform(row, structType)), RowEncoder.apply(structType));
    }
}
