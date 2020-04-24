package io.basestar.spark;

import io.basestar.expression.Expression;
import io.basestar.schema.Id;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseString;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.util.Nullsafe;
import io.basestar.util.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ExpressionTransform implements Transform<Dataset<Row>, Dataset<Row>> {

    private final ObjectSchema schema;

    @lombok.Builder(builderClassName = "Builder")
    public ExpressionTransform(final ObjectSchema schema) {

        this.schema = Nullsafe.require(schema);
    }

    @Override
    public Dataset<Row> accept(final Dataset<Row> input) {

        Dataset<Row> output = input;
        final Id id = schema.getId();
        if(id != null && id.getExpression() != null) {
            final Column col = apply(input, id.getExpression(), UseString.DEFAULT);
            output = output.withColumn(Reserved.ID, col);
        }
        for(final Property property : schema.getAllProperties().values()) {
            if(property.getExpression() != null) {
                output = output.withColumn(property.getName(), apply(output, property.getExpression(), property.getType()));
            }
        }
        return output;
    }

    private Column apply(final Dataset<Row> ds, final Expression expression, final Use<?> type) {

        return visitor(ds).visit(expression).cast(SparkSchemaUtils.type(type));
    }

    private SparkExpressionVisitor visitor(final Dataset<Row> ds) {

        return new SparkExpressionVisitor(path -> {
            assert path.size() == 2 && path.isChild(Path.of(Reserved.THIS));
            return ds.col(path.get(1));
        });
    }
}
