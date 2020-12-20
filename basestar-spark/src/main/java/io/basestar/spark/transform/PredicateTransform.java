package io.basestar.spark.transform;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.resolver.ColumnResolver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

@Slf4j
@Getter
@lombok.Builder(builderClassName = "Builder")
public class PredicateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final Layout inputLayout;

    private final Expression predicate;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final InferenceContext inference = InferenceContext.from(inputLayout);
        final ColumnResolver<Row> columnResolver = ColumnResolver.lowercase(ColumnResolver::nested);
        final Column condition = new SparkExpressionVisitor(name -> columnResolver.resolve(input, name), inference)
                .visit(predicate);

        return input.filter(condition);
    }
}
