package io.basestar.spark.query;

import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Layout;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkSchemaUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@lombok.Builder(builderClassName = "Builder")
public class AggStepTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<String> group;

    private final Map<String, Aggregate> aggregates;

    private final Layout inputLayout;

    private final Layout outputLayout;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructType outputStructType = SparkSchemaUtils.structType(outputLayout);

        final ColumnResolver<Row> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);

        final InferenceContext inferenceContext = InferenceContext.from(inputLayout.getSchema());
        final SparkExpressionVisitor expressionVisitor = new SparkExpressionVisitor(k -> columnResolver.resolve(input, k), inferenceContext);

        final Column[] aggs = aggregates.entrySet().stream()
                .map(v -> expressionVisitor.visit(v.getValue()).as(v.getKey()))
                .toArray(Column[]::new);

        final Dataset<Row> output = input.groupBy(group.stream().map(input::col).toArray(Column[]::new))
                .agg(aggs[0], Arrays.copyOfRange(aggs, 1, aggs.length));

        return output.map((MapFunction<Row, Row>)(row -> SparkSchemaUtils.conform(row, outputStructType)),
                RowEncoder.apply(outputStructType));
    }
}
