package io.basestar.spark.transform;

import io.basestar.schema.layout.Layout;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.Set;

public class LayoutTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final Layout inputLayout;

    private final Layout outputLayout;

    private final Set<Name> expand;

    private final StructType outputStructType;

    @lombok.Builder(builderClassName = "Builder")
    LayoutTransform(final Layout inputLayout, final Layout outputLayout, final Set<Name> expand) {

        this.inputLayout = inputLayout;
        this.outputLayout = outputLayout;
        this.expand = Nullsafe.option(expand);
        this.outputStructType = SparkSchemaUtils.structType(this.outputLayout, this.expand);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Layout inputLayout = this.inputLayout;
        final Layout outputLayout = this.outputLayout;
        final Set<Name> expand = this.expand;
        final StructType outputStructType = this.outputStructType;
        return input.map((MapFunction<Row, Row>) row -> {

            final Map<String, Object> initial = SparkSchemaUtils.fromSpark(inputLayout, expand, row);
            final Map<String, Object> canonical = inputLayout.unapplyLayout(expand, initial);
            final Map<String, Object> result = outputLayout.applyLayout(expand, canonical);
            return SparkSchemaUtils.toSpark(outputLayout, expand, outputStructType, result);

        }, RowEncoder.apply(outputStructType));
    }
}
