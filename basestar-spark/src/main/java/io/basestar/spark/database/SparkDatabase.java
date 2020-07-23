package io.basestar.spark.database;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.transform.SortTransform;
import io.basestar.spark.util.ColumnResolver;
import io.basestar.spark.util.DatasetResolver;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nullable;

public class SparkDatabase {

    private final DatasetResolver resolver;

    private final ColumnResolver<Row> columnResolver;

    @Nullable
    private final Namespace namespace;

    @lombok.Builder(builderClassName = "Builder")
    SparkDatabase(final DatasetResolver resolver, final ColumnResolver<Row> columnResolver, @Nullable final Namespace namespace) {

        this.resolver = Nullsafe.require(resolver);
        this.columnResolver = Nullsafe.option(columnResolver, ColumnResolver::nested);
        this.namespace = namespace;
    }

    public QueryChain<Row> from(final String schema) {

        return from(Nullsafe.require(namespace).requireInstanceSchema(schema));
    }

    public QueryChain<Row> from(final Name schema) {

        return from(Nullsafe.require(namespace).requireInstanceSchema(schema));
    }

    public QueryChain<Row> from(final InstanceSchema schema) {

        return (query, sort, expand) -> {

            final Dataset<Row> input = resolver.resolve(schema, columnResolver, expand);
            Dataset<Row> output = input;
            if(query != null) {
                final Expression bound = query.bind(Context.init());
                final SparkExpressionVisitor visitor = new SparkExpressionVisitor(
                        name -> columnResolver.resolve(input, name)
                );
                output = output.filter(bound.visit(visitor));
                output = output.filter(bound.visit(visitor));
            }
            if(!sort.isEmpty()) {
                final SortTransform<Row> transform = SortTransform.<Row>builder()
                        .columnResolver(columnResolver)
                        .sort(sort)
                        .build();
                output = transform.accept(output);
            }
            return output;
        };
    }
}
