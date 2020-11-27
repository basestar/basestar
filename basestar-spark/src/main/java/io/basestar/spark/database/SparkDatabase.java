package io.basestar.spark.database;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.resolver.ColumnResolver;
import io.basestar.spark.resolver.SchemaResolver;
import io.basestar.spark.transform.SortTransform;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nullable;

public class SparkDatabase {

    private final SchemaResolver resolver;

    @Nullable
    private final Namespace namespace;

    @lombok.Builder(builderClassName = "Builder")
    SparkDatabase(final SchemaResolver resolver, @Nullable final Namespace namespace) {

        this.resolver = Nullsafe.require(resolver);
        this.namespace = namespace;
    }

    public QueryChain<Row> from(final String schema) {

        return from(Nullsafe.require(namespace).requireLinkableSchema(schema));
    }

    public QueryChain<Row> from(final Name schema) {

        return from(Nullsafe.require(namespace).requireLinkableSchema(schema));
    }

    public QueryChain<Row> from(final LinkableSchema schema) {

        return (query, sort, expand) -> {

            final Dataset<Row> input = resolver.resolve(schema, expand);

            Dataset<Row> output = input;
            if(query != null) {
                final Expression bound = query.bind(Context.init());
                final SparkExpressionVisitor visitor = new SparkExpressionVisitor(
                        name -> ColumnResolver.nested(input, name)
                );
                output = output.filter(bound.visit(visitor));
            }
            if(!sort.isEmpty()) {
                final SortTransform<Row> transform = SortTransform.<Row>builder()
                        .sort(sort)
                        .build();
                output = transform.accept(output);
            }

//            System.out.println("Final output for " + schema.getQualifiedName() + ":\n" + output.showString(10, 80, false));

            return output;
        };
    }
}
