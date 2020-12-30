package io.basestar.spark.transform;

import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.util.SparkRowUtils;
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

        final Column condition = new SparkExpressionVisitor(name -> SparkRowUtils.resolveName(input, name))
                .visit(predicate);

        return input.filter(condition);
    }
}
