package io.basestar.spark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.*;
import io.basestar.storage.view.QueryPlanner;
import io.basestar.storage.view.QueryStage;
import io.basestar.storage.view.QueryStageVisitor;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public interface QueryResolver {

    default Query<Row> resolve(final LinkableSchema schema) {

        return resolve(schema, ImmutableSet.of());
    }

    default Query<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {

        return resolve(schema, Constant.TRUE, ImmutableList.of(), expand);
    }

    Query<Row> resolve(LinkableSchema schema, Expression query, List<Sort> sort, Set<Name> expand);

    default Source<Dataset<Row>> ofSource(final LinkableSchema schema) {

        return ofSource(schema, ImmutableSet.of());
    }

    default Source<Dataset<Row>> ofSource(final LinkableSchema schema, final Set<Name> expand) {

        return sink -> sink.accept(resolve(schema, expand).dataset());
    }

    default Caching caching() {

        return new Caching(this);
    }

    static QueryResolver ofSources(final Function<LinkableSchema, Dataset<Row>> fn) {

        return (schema, query, sort, expand) -> {
            assert query.isConstant();
            assert sort.isEmpty();
            assert expand.isEmpty();
            final Dataset<Row> result = fn.apply(schema);
            if(query.evaluatePredicate(Context.init())) {
                return () -> result;
            } else {
                final SQLContext sc = result.sparkSession().sqlContext();
                return () -> sc.createDataFrame(ImmutableList.of(), result.schema());
            }
        };
    }

    @RequiredArgsConstructor
    class Caching implements QueryResolver, AutoCloseable {

        private final Map<Key, Dataset<Row>> results = new HashMap<>();

        private final QueryResolver resolver;

        @Override
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

            return () -> results.computeIfAbsent(new Key(schema, query, sort, expand),
                    ignored -> resolver.resolve(schema, query, sort, expand).dataset().cache());
        }

        @Override
        public void close() {

            results.forEach((k, v) -> v.unpersist());
        }

        @Data
        private static class Key {

            private final LinkableSchema schema;

            private final Expression query;

            private final List<Sort> sort;

            private final Set<Name> expand;
        }
    }

    class Automatic extends QueryPlanner.Default<Stage> implements QueryResolver, QueryStageVisitor<Stage> {

        private final QueryResolver resolver;

        public Automatic(final QueryResolver resolver) {

            this.resolver = resolver;
        }

        @Override
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

            return plan(this, schema, query, sort, expand);
        }

        @Override
        public Stage aggregate(final Stage input, final List<String> group, final Map<String, Aggregate> aggregates, final Map<String, Use<?>> output) {

            return input.aggregate(group, aggregates, output);
        }

        @Override
        public Stage empty(final LinkableSchema schema, final Set<Name> expand) {

            return Stage.empty(resolver, schema, expand);
        }

        @Override
        public Stage expand(final Stage input, final LinkableSchema schema, final Set<Name> expand) {

            return input.expand(this, schema, expand);
        }

        @Override
        public Stage filter(final Stage input, final Expression condition) {

            return input.filter(condition);
        }

        @Override
        public Stage map(final Stage input, final Map<String, Expression> expressions, final Map<String, Use<?>> output) {

            return input.map(expressions, output);
        }

        @Override
        public Stage sort(final Stage input, final List<Sort> sort) {

            return input.sort(sort);
        }

        @Override
        public Stage source(final LinkableSchema schema) {

            return Stage.source(resolver, schema);
        }

        @Override
        public Stage union(final List<Stage> inputs) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Stage schema(final Stage input, final InstanceSchema schema) {

            return input.schema(schema);
        }
    }

    class Combining implements QueryResolver {

        private final QueryResolver baseline;

        private final QueryResolver overlay;

        private final Combiner combiner;

        private final String joinType;

        public Combining(final QueryResolver baseline, final QueryResolver overlay) {

            this(baseline, overlay, null);
        }

        public Combining(final QueryResolver baseline, final QueryResolver overlay, final Combiner combiner) {

            this(baseline, overlay, combiner, null);
        }

        public Combining(final QueryResolver baseline, final QueryResolver overlay, final Combiner combiner, final String joinType) {

            this.baseline = Nullsafe.require(baseline);
            this.overlay = Nullsafe.require(overlay);
            this.combiner = Nullsafe.orDefault(combiner, Combiner.SIMPLE);
            this.joinType = Nullsafe.orDefault(joinType, Combiner.DEFAULT_JOIN_TYPE);
        }

        @Override
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

            final Dataset<Row> baseline = this.baseline.resolve(schema, query, sort, expand).dataset();
            final Dataset<Row> overlay = this.overlay.resolve(schema, query, sort, expand).dataset();
            return () -> combiner.apply(schema, expand, baseline, overlay, joinType);
        }
    }

    interface Stage extends Query<Row>, QueryStage {

        static Stage from(final Query<Row> query, final Layout layout) {

            return from(query, layout, Constant.TRUE, ImmutableList.of());
        }

        static Stage from(final Query<Row> query, final Layout layout, final Expression filter, final List<Sort> sort) {

            return new Stage() {

                @Override
                public Dataset<Row> dataset() {

                    return query.dataset();
                }

                @Override
                public Layout getLayout() {

                    return layout;
                }

                @Override
                public Expression getFilter() {

                    return filter;
                }

                @Override
                public List<Sort> getSort() {

                    return sort;
                }
            };
        }

        default Stage then(final Transform<Dataset<Row>, Dataset<Row>> transform, final Layout layout) {

            return then(transform, layout, getFilter());
        }

        default Stage then(final Transform<Dataset<Row>, Dataset<Row>> transform, final Layout layout, final Expression filter) {

            return then(transform, layout, filter, getSort());
        }

        default Stage then(final Transform<Dataset<Row>, Dataset<Row>> transform, final Layout layout, final Expression filter, final List<Sort> sort) {

            return from(Query.super.then(transform), layout, filter, sort);
        }

        static Stage source(final QueryResolver resolver, final LinkableSchema schema) {

            return from(resolver.resolve(schema), schema);
        }

        static Stage empty(final QueryResolver resolver, final LinkableSchema schema, final Set<Name> expand) {

            final Layout output = Layout.simple(schema.getSchema(), expand);
            return from(resolver.resolve(schema, Constant.FALSE, ImmutableList.of(), expand), output);
        }

        default Stage aggregate(final List<String> group, final Map<String, Aggregate> aggregates, final Map<String, Use<?>> output) {

            final Layout inputLayout = getLayout();
            final Layout outputLayout = Layout.simple(output);

            return then(AggregateTransform.builder()
                            .group(group)
                            .aggregates(aggregates)
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout).build(), outputLayout);
        }

        default Stage expand(final QueryResolver resolver, final LinkableSchema schema, final Set<Name> expand) {

            final Layout outputLayout = Layout.simple(schema.getSchema(), expand);

            return then(ExpandTransform.builder()
                            .expand(expand)
                            .schema(schema)
                            .resolver(resolver).build(), outputLayout);
        }

        default Stage filter(final Expression condition) {

            final Layout inputLayout = getLayout();

            return then(PredicateTransform.builder()
                            .inputLayout(inputLayout)
                            .predicate(condition).build(), inputLayout, condition);
        }

        default Stage map(final Map<String, Expression> expressions, final Map<String, Use<?>> output) {

            final Layout inputLayout = getLayout();
            final Layout outputLayout = Layout.simple(output);

            return then(ExpressionTransform.builder()
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout)
                            .expressions(expressions).build(), outputLayout);
        }

        default Stage sort(final List<Sort> sort) {

            final Layout inputLayout = getLayout();

            return then(SortTransform.<Row>builder()
                            .sort(sort).build(), inputLayout, getFilter(), sort);
        }

        default Stage schema(final InstanceSchema schema) {

            return then(SchemaTransform.builder()
                            .schema(schema).build(), schema);
        }
    }
}
