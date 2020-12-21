package io.basestar.spark.database;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Namespace;
import io.basestar.spark.query.QueryResolver;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.apache.spark.sql.Row;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

public class SparkDatabase {

    private final QueryResolver resolver;

    @Nullable
    private final Namespace namespace;

    @lombok.Builder(builderClassName = "Builder")
    SparkDatabase(final QueryResolver resolver, @Nullable final Namespace namespace) {

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

            final Set<Name> mergedExpand;
            if(expand.contains(QueryChain.DEFAULT_EXPAND)) {
                mergedExpand = new HashSet<>(expand);
                mergedExpand.remove(QueryChain.DEFAULT_EXPAND);
                mergedExpand.addAll(schema.getExpand());
            } else {
                mergedExpand = expand;
            }

            return resolver.resolve(schema, query, sort, mergedExpand).dataset();


            /*final Dataset<Row> input = resolver.resolve(schema, expand);

            final InferenceContext inferenceContext = new InferenceContext.FromSchema(schema);

            Dataset<Row> output = input;
            if(query != null) {
                final Expression bound = query.bind(Context.init());
                final SparkExpressionVisitor visitor = new SparkExpressionVisitor(
                        name -> ColumnResolver.nested(input, name),
                        inferenceContext
                );
                output = output.filter(bound.visit(visitor));
            }
            if(!sort.isEmpty()) {
                final SortTransform<Row> transform = SortTransform.<Row>builder()
                        .sort(sort)
                        .build();
                output = transform.accept(output);
            }

            return output;*/
        };
    }
}
