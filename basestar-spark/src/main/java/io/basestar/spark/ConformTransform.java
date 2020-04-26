package io.basestar.spark;

import io.basestar.util.Nullsafe;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;


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

        return input.map(new ConformFunction(structType), RowEncoder.apply(structType));
    }

    @RequiredArgsConstructor
    public static class ConformFunction extends AbstractFunction1<Row, Row> implements Function1<Row, Row>, Serializable {

        private final StructType structType;

        @Override
        public Row apply(final Row row) {

            return SparkSchemaUtils.conform(row, structType);
        }
    }
}
