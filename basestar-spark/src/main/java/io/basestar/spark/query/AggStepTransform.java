package io.basestar.spark.query;

import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Layout;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.transform.Transform;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
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

        final int inputPartitions = input.rdd().partitions().length;

        final Column[] groupCols = group.stream().map(input::col).toArray(Column[]::new);
        final Dataset<Row> grouped = input.groupBy(groupCols)
                .agg(aggs[0], Arrays.copyOfRange(aggs, 1, aggs.length));

        final Dataset<Row> output = grouped.map((MapFunction<Row, Row>)(row -> SparkRowUtils.conform(row, outputStructType)),
                RowEncoder.apply(outputStructType));

        log.warn("Aggregation of ({}) has i/o partitions {}->{}", aggregates.keySet(), inputPartitions, output.rdd().partitions().length);

        return output;
    }
}
