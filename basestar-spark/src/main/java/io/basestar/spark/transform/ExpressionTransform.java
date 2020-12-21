package io.basestar.spark.transform;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Layout;
import io.basestar.schema.Reserved;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Getter
@lombok.Builder(builderClassName = "Builder")
public class ExpressionTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final Layout inputLayout;

    private final Layout outputLayout;

    private final Map<String, Expression> expressions;

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        final Map<String, Expression> expressions = this.expressions;
        final Layout inputLayout = this.inputLayout;
        final Layout outputLayout = this.outputLayout;

        final StructType outputType = SparkSchemaUtils.structType(outputLayout);

        return input.map(SparkUtils.map(row -> {

            final Map<String, Object> object = SparkSchemaUtils.fromSpark(inputLayout, row);
            final Context context = Context.init(object).with(Reserved.THIS, object);
            final Map<String, Object> output = new HashMap<>();
            for(final Map.Entry<String, Expression> entry : expressions.entrySet()) {
                try {
                    output.put(entry.getKey(), entry.getValue().evaluate(context));
                } catch (final Exception e) {
                    log.error("Failed to process expression " + entry.getValue() + " (" + e.getMessage() + ")");
                }
            }
            return SparkSchemaUtils.toSpark(outputLayout, outputType, output);
        }), RowEncoder.apply(outputType));
    }
}
