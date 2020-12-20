package io.basestar.spark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.*;
import io.basestar.storage.view.*;
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

    default Source<Dataset<Row>> source(final LinkableSchema schema) {

        return source(schema, ImmutableSet.of());
    }

    default Source<Dataset<Row>> source(final LinkableSchema schema, final Set<Name> expand) {

        return sink -> sink.accept(resolve(schema, expand).dataset());
    }

    default Caching caching() {

        return new Caching(this);
    }

    static QueryResolver source(final Function<LinkableSchema, Dataset<Row>> fn) {

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
        public Query resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

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

    class Automatic implements QueryResolver, QueryStage.Visitor<Query<Row>> {

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

            final QueryStage stage = planner.plan(schema, query, sort, expand);
            return stage.visit(this);
        }

        @Override
        public Query<Row> visitAgg(final AggStage stage) {

            final Layout inputLayout = stage.getInput().outputLayout();
            final Layout outputLayout = stage.getOutputLayout();

            return stage.getInput().visit(this)
                    .then(AggregateTransform.builder()
                            .group(stage.getGroup())
                            .aggregates(stage.getAggregates())
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout).build());
        }

        @Override
        public Query<Row> visitEmpty(final EmptyStage stage) {

            return resolver.resolve(stage.getSchema(), Constant.FALSE, ImmutableList.of(), stage.getExpand());
        }

        @Override
        public Query<Row> visitExpand(final ExpandStage stage) {

            return stage.getInput().visit(this)
                    .then(ExpandTransform.builder()
                            .expand(stage.getExpand())
                            .schema(stage.getSchema())
                            .resolver(this).build());
        }

        @Override
        public Query<Row> visitFilter(final FilterStage stage) {

            return stage.getInput().visit(this)
                    .then(PredicateTransform.builder()
                            .inputLayout(stage.getInput().outputLayout())
                            .predicate(stage.getCondition()).build());
        }

        @Override
        public Query<Row> visitMap(final MapStage stage) {

            final Layout outputLayout = stage.getOutputLayout();

            return stage.getInput().visit(this)
                    .then(ExpressionTransform.builder()
                            .inputLayout(stage.getInput().outputLayout())
                            .outputLayout(outputLayout)
                            .expressions(stage.getOutputs()).build());
        }

        @Override
        public Query<Row> visitSchema(final SchemaStage stage) {

            return stage.getInput().visit(this)
                    .then(SchemaTransform.builder().schema(stage.getSchema()).build());
        }

        @Override
        public Query<Row> visitSort(final SortStage stage) {

            return stage.getInput().visit(this)
                    .then(SortTransform.<Row>builder().sort(stage.getSort()).build());
        }

        @Override
        public Query<Row> visitSource(final SourceStage stage) {

            return resolver.resolve(stage.getSchema());
        }

        @Override
        public Query<Row> visitUnion(final UnionStage stage) {

            throw new UnsupportedOperationException();
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
