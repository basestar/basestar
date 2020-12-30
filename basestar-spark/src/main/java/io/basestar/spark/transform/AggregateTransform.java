package io.basestar.spark.transform;

import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Layout;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import lombok.extern.slf4j.Slf4j;
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
public class AggregateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<String> group;

    private final Map<String, Aggregate> aggregates;

    private final Layout inputLayout;

    private final Layout outputLayout;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructType outputStructType = SparkSchemaUtils.structType(outputLayout);

        final SparkExpressionVisitor expressionVisitor = new SparkExpressionVisitor(k -> SparkRowUtils.resolveName(input, k));

        final Column[] aggs = aggregates.entrySet().stream()
                .map(v -> expressionVisitor.visit(v.getValue()).as(v.getKey()))
                .toArray(Column[]::new);

        final Column[] groupCols = group.stream().map(input::col).toArray(Column[]::new);
        final Dataset<Row> grouped = input.groupBy(groupCols)
                .agg(aggs[0], Arrays.copyOfRange(aggs, 1, aggs.length));

        return grouped.map(SparkUtils.map(row -> SparkRowUtils.conform(row, outputStructType)),
                RowEncoder.apply(outputStructType));
    }
}
