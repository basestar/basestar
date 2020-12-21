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
import io.basestar.storage.view.QueryStageVisitor;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pair;
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

    class Automatic implements QueryResolver, QueryStageVisitor<Query<Row>> {

        private final QueryPlanner planner;

        private final QueryResolver resolver;

        public Automatic(final QueryResolver resolver) {

            this(QueryPlanner.Default.INSTANCE, resolver);
        }

        public Automatic(final QueryPlanner planner, final QueryResolver resolver) {

            this.planner = planner;
            this.resolver = resolver;
        }

        @Override
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

            return planner.plan(this, schema, query, sort, expand);
        }

        @Override
        public Pair<Query<Row>, Layout> aggregate(final Pair<Query<Row>, Layout> input, final List<String> group, final Map<String, Aggregate> aggregates, final Map<String, Use<?>> output) {

            final Layout inputLayout = input.getSecond();
            final Layout outputLayout = Layout.simple(output);

            return Pair.of(input.getFirst()
                    .then(AggregateTransform.builder()
                            .group(group)
                            .aggregates(aggregates)
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout).build()), outputLayout);
        }

        @Override
        public Pair<Query<Row>, Layout> empty(final LinkableSchema schema, final Set<Name> expand) {

            final Layout output = Layout.simple(schema.getSchema(), expand);
            return Pair.of(resolver.resolve(schema, Constant.FALSE, ImmutableList.of(), expand), output);
        }

        @Override
        public Pair<Query<Row>, Layout> expand(final Pair<Query<Row>, Layout> input, final LinkableSchema schema, final Set<Name> expand) {

            final Layout outputLayout = Layout.simple(schema.getSchema(), expand);

            return Pair.of(input.getFirst()
                    .then(ExpandTransform.builder()
                            .expand(expand)
                            .schema(schema)
                            .resolver(this).build()), outputLayout);
        }

        @Override
        public Pair<Query<Row>, Layout> filter(final Pair<Query<Row>, Layout> input, final Expression condition) {

            final Layout inputLayout = input.getSecond();

            return Pair.of(input.getFirst()
                    .then(PredicateTransform.builder()
                            .inputLayout(inputLayout)
                            .predicate(condition).build()), inputLayout);
        }

        @Override
        public Pair<Query<Row>, Layout> map(final Pair<Query<Row>, Layout> input, final Map<String, Expression> expressions, final Map<String, Use<?>> output) {

            final Layout inputLayout = input.getSecond();
            final Layout outputLayout = Layout.simple(output);

            return Pair.of(input.getFirst()
                    .then(ExpressionTransform.builder()
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout)
                            .expressions(expressions).build()), outputLayout);
        }

        @Override
        public Pair<Query<Row>, Layout> sort(final Pair<Query<Row>, Layout> input, final List<Sort> sort) {

            final Layout inputLayout = input.getSecond();

            return Pair.of(input.getFirst()
                    .then(SortTransform.<Row>builder()
                            .sort(sort).build()), inputLayout);
        }

        @Override
        public Pair<Query<Row>, Layout> source(final LinkableSchema schema) {

            return Pair.of(resolver.resolve(schema), schema);
        }

        @Override
        public Pair<Query<Row>, Layout> union(final List<Pair<Query<Row>, Layout>> inputs) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Pair<Query<Row>, Layout> schema(final Pair<Query<Row>, Layout> input, final InstanceSchema schema) {

            return Pair.of(input.getFirst()
                    .then(SchemaTransform.builder()
                            .schema(schema).build()), schema);
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
}
