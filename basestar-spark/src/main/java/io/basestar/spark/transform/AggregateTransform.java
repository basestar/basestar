package io.basestar.spark.transform;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Layout;
import io.basestar.schema.Reserved;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
@lombok.Builder(builderClassName = "Builder")
public class AggregateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final List<Pair<String, Expression>> group;

    private final Map<String, Aggregate> aggregates;

    private final Layout inputLayout;

    private final Layout outputLayout;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final StructType outputStructType = SparkSchemaUtils.structType(outputLayout);

        final SparkExpressionVisitor expressionVisitor = new SparkExpressionVisitor(k -> {

            if(k.first().equals(Reserved.THIS)) {
                if(k.size() == 1) {
                    return functions.struct(Arrays.stream(input.columns()).map(functions::col).toArray(Column[]::new));
                } else {
                    return SparkRowUtils.resolveName(input, k.withoutFirst());
                }
            } else {
                return SparkRowUtils.resolveName(input, k);
            }
        });

        final Column[] aggCols = aggregates.entrySet().stream()
                .map(v -> expressionVisitor.visit(v.getValue().bind(Context.init())).as(v.getKey()))
                .toArray(Column[]::new);

        final Column[] groupCols = group.stream()
                .map(e -> expressionVisitor.visit(e.getSecond().bind(Context.init())).as(e.getFirst()))
                .toArray(Column[]::new);

        final Dataset<Row> grouped = input.groupBy(groupCols)
                .agg(aggCols[0], Arrays.copyOfRange(aggCols, 1, aggCols.length));

        return grouped.map(SparkUtils.map(row -> SparkRowUtils.conform(row, outputStructType)),
                RowEncoder.apply(outputStructType));
    }
}
