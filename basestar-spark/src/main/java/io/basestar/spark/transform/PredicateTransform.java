package io.basestar.spark.transform;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.Reserved;
import io.basestar.spark.util.SparkSchemaUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Map;

@Slf4j
@Getter
@lombok.Builder(builderClassName = "Builder")
public class PredicateTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final Layout inputLayout;

    private final Expression predicate;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Expression expression = this.predicate;
        final Layout inputLayout = this.inputLayout;

        return input.filter((FilterFunction<Row>) row -> {

            final Map<String, Object> object = SparkSchemaUtils.fromSpark(inputLayout, row);
            try {
                final Context context = Context.init(object).with(Reserved.THIS, object);
                return expression.evaluatePredicate(context);
            } catch (final Exception e) {
                log.error("Failed to process filter expression " + expression + " (" + e.getMessage() + ")");
                return false;
            }
        });
    }
}
