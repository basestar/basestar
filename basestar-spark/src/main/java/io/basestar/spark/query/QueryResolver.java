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
import io.basestar.schema.ViewSchema;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.from.Join;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseComposite;
import io.basestar.schema.util.Bucket;
import io.basestar.spark.combiner.Combiner;
import io.basestar.spark.expression.SparkExpressionVisitor;
import io.basestar.spark.source.Source;
import io.basestar.spark.transform.*;
import io.basestar.spark.util.SparkRowUtils;
import io.basestar.spark.util.SparkSchemaUtils;
import io.basestar.spark.util.SparkUtils;
import io.basestar.storage.query.QueryPlanner;
import io.basestar.storage.query.QueryStageVisitor;
import io.basestar.util.*;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface QueryResolver {

    default Query<Row> resolve(final LinkableSchema schema) {

        return resolve(schema, ImmutableSet.of());
    }

    default Query<Row> resolve(final LinkableSchema schema, final Set<Name> expand) {

        return resolve(schema, Constant.TRUE, ImmutableList.of(), expand);
    }

    default Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return resolve(schema, query, sort, expand, null);
    }

    Query<Row> resolve(LinkableSchema schema, Expression query, List<Sort> sort, Set<Name> expand, Set<Bucket> buckets);

//    default Stage sql(final String sql, final InstanceSchema schema, final Map<String, Stage> with) {
//
//        SparkSession session = null;
//        for(final Map.Entry<String, Stage> entry : with.entrySet()) {
//            final String as = entry.getKey();
//            final Stage query = entry.getValue();
//            final Dataset<Row> ds = query.dataset();
//            ds.registerTempTable(as);
//            session = ds.sparkSession();
//        }
//        if(session != null) {
//            Dataset<Row> result = session.sql(sql);
//            if(schema instanceof ViewSchema) {
//                final ViewSchema view = (ViewSchema)schema;
//                final FromSql from = (FromSql)view.getFrom();
//                final StructField idField = SparkRowUtils.field(view.id(), DataTypes.BinaryType);
//                final StructType sourceType = result.schema();
//                final StructType targetType = SparkRowUtils.append(sourceType, idField);
//                result = result.map(SparkUtils.map(row -> {
//                    final List<Object> keys = new ArrayList<>();
//                    for(final String name : from.getPrimaryKey()) {
//                        keys.add(SparkSchemaUtils.fromSpark(SparkRowUtils.get(row, name)));
//                    }
//                    final byte[] id = BinaryKey.from(keys).getBytes();
//                    return SparkRowUtils.append(row, idField, id);
//                }), RowEncoder.apply(targetType));
//            }
//            final Dataset<Row> tmp = result;
//            return Stage.from(() -> tmp, schema);
//        } else {
//            throw new IllegalStateException("SQL query must define at least one source");
//        }
//    }

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

        return ofSources((schema, buckets) -> fn.apply(schema));
    }

    static QueryResolver ofSources(final BiFunction<LinkableSchema, Set<Bucket>, Dataset<Row>> fn) {

        return (schema, query, sort, expand, buckets) -> {
            assert query.isConstant();
            assert sort.isEmpty();
            assert expand.isEmpty();
            final Dataset<Row> result = fn.apply(schema, buckets);
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
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand, final Set<Bucket> buckets) {

            return () -> results.computeIfAbsent(new Key(schema, query, sort, expand),
                    ignored -> resolver.resolve(schema, query, sort, expand, buckets).dataset().cache());
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

    class Automatic extends QueryPlanner.AggregateSplitting<Stage> implements QueryResolver, QueryStageVisitor<Stage> {

        private final QueryResolver resolver;

        public Automatic(final QueryResolver resolver) {

            this(resolver, ViewSchema::isMaterialized);
        }

        public Automatic(final QueryResolver resolver, final Predicate<ViewSchema> materialized) {

            super(materialized);
            this.resolver = resolver;
        }

        @Override
        protected boolean sortBeforeExpand(final LinkableSchema schema, final List<Sort> sort) {

            return false;
        }

        @Override
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand, final Set<Bucket> buckets) {

            return plan(this, schema, query, sort, expand, buckets);
        }

        @Override
        public Stage agg(final Stage input, final List<String> group, final Map<String, TypedExpression<?>> expressions) {

            return input.aggregate(group, expressions);
        }

        @Override
        public Stage empty(final LinkableSchema schema, final Set<Name> expand) {

            return Stage.empty(resolver, schema, expand);
        }

        @Override
        public Stage expand(final Stage input, final LinkableSchema schema, final Set<Name> expand, final Set<Bucket> buckets) {

            return input.expand(this, schema, expand, buckets);
        }

        @Override
        public Stage filter(final Stage input, final Expression condition) {

            return input.filter(condition);
        }

        @Override
        public Stage map(final Stage input, final Map<String, TypedExpression<?>> expressions) {

            return input.map(expressions);
        }

        @Override
        public Stage sort(final Stage input, final List<Sort> sort) {

            return input.sort(sort);
        }

        @Override
        public Stage source(final LinkableSchema schema, final Set<Bucket> buckets) {

            return Stage.source(resolver, schema, buckets);
        }

        @Override
        public Stage union(final List<Stage> inputs, final boolean all) {

            if(inputs.isEmpty()) {
                throw new IllegalStateException("Cannot create empty union");
            }
            final Map<String, Use<?>> schema = new HashMap<>();
            inputs.forEach(input -> input.getLayout().getSchema().forEach((name, type) -> {
                final Use<?> existing = schema.get(name);
                if(existing != null) {
                    final Use<?> common = Use.commonBase(existing, type);
                    schema.put(name, common);
                } else {
                    schema.put(name, type);
                }
            }));
            final Layout layout = Layout.simple(schema);
            final StructType structType = SparkSchemaUtils.structType(layout);
            return Stage.from(() -> inputs.stream()
                    .map(input -> input.dataset().map(SparkUtils.map(row -> SparkRowUtils.conform(row, structType)), RowEncoder.apply(structType)))
                    .reduce(Dataset::union).orElseThrow(IllegalStateException::new), layout);
        }

        @Override
        public Stage conform(final Stage input, final InstanceSchema schema, final Set<Name> expand) {

            return input.conform(schema, expand);
        }

        @Override
        public Stage sql(final String sql, final InstanceSchema schema, final Map<String, Stage> with) {

            throw new UnsupportedOperationException();
//            return resolver.sql(sql, schema, with);
        }

        @Override
        public Stage join(final Stage left, final Stage right, final Join join) {

            final String leftAs = join.getLeft().getAlias();
            final String rightAs = join.getRight().getAlias();

            final Map<String, Use<?>> schema = new HashMap<>();
            schema.putAll(left.getLayout().getSchema());
            schema.putAll(right.getLayout().getSchema());
            if(join.getLeft().hasAlias()) {
                schema.put(leftAs, new UseComposite(left.getLayout().getSchema()));
            }
            if(join.getRight().hasAlias()) {
                schema.put(rightAs, new UseComposite(right.getLayout().getSchema()));
            }
            final Layout layout = Layout.simple(schema);
            final StructType leftType = SparkSchemaUtils.structType(left.getLayout());
            final StructType rightType = SparkSchemaUtils.structType(right.getLayout());
            final StructType structType = SparkSchemaUtils.structType(layout);

            final String joinType = join.getType().name().toLowerCase();

            return Stage.from(
                    () -> {

                        final Dataset<Row> leftDs = left.dataset().as(leftAs);
                        final Dataset<Row> rightDs = right.dataset().as(rightAs);

                        final Function<Name, Column> columnResolver = name -> {
                            if(name.first().equals(leftAs)) {
                                return SparkRowUtils.resolveName(leftDs, name.withoutFirst());
                            } else if(name.first().equals(rightAs)) {
                                return SparkRowUtils.resolveName(rightDs, name.withoutFirst());
                            } else if(SparkRowUtils.findField(leftType, name.first()).isPresent()) {
                                return SparkRowUtils.resolveName(leftDs, name);
                            } else if(SparkRowUtils.findField(rightType, name.first()).isPresent()) {
                                return SparkRowUtils.resolveName(rightDs, name);
                            } else {
                                throw new IllegalStateException("Column " + name + " not found in join");
                            }
                        };
                        final SparkExpressionVisitor visitor = new SparkExpressionVisitor(columnResolver);
                        final Column condition = visitor.visit(join.getOn().bind(Context.init()));

                        return leftDs.joinWith(rightDs, condition, joinType).map(SparkUtils.map(tuple -> {

                            final Map<String, Object> result = new HashMap<>();
                            for(final StructField field : leftType.fields()) {
                                result.put(field.name(), SparkRowUtils.get(tuple._1(), field.name()));
                            }
                            for(final StructField field : rightType.fields()) {
                                result.put(field.name(), SparkRowUtils.get(tuple._2(), field.name()));
                            }
                            if(join.getLeft().hasAlias()) {
                                final Row leftRow = SparkRowUtils.conform(tuple._1(), leftType);
                                result.put(leftAs, leftRow);
                            }
                            if(join.getRight().hasAlias()) {
                                final Row rightRow = SparkRowUtils.conform(tuple._2(), rightType);
                                result.put(rightAs, rightRow);
                            }
                            return SparkRowUtils.create(structType, result);

                        }), RowEncoder.apply(structType));
                    },
                    layout
            );
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
        public Query<Row> resolve(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand, final Set<Bucket> buckets) {

            final Dataset<Row> baseline = this.baseline.resolve(schema, query, sort, expand, buckets).dataset();
            final Dataset<Row> overlay = this.overlay.resolve(schema, query, sort, expand, buckets).dataset();
            return () -> combiner.apply(schema, expand, baseline, overlay, joinType);
        }
    }

    interface Stage extends Query<Row> {

        Layout getLayout();

        static Stage from(final Query<Row> query, final Layout layout) {

            return new Stage() {

                @Override
                public Dataset<Row> dataset() {

                    return query.dataset();
                }

                public Layout getLayout() {

                    return layout;
                }
            };
        }

        default Stage then(final Transform<Dataset<Row>, Dataset<Row>> transform, final Layout layout) {

            return from(Query.super.then(transform), layout);
        }

        static Stage source(final QueryResolver resolver, final LinkableSchema schema, final Set<Bucket> buckets) {

            return from(resolver.resolve(schema, Constant.TRUE, ImmutableList.of(), ImmutableSet.of(), buckets), Layout.simple(schema.getSchema(), ImmutableSet.of()));
        }

        static Stage empty(final QueryResolver resolver, final LinkableSchema schema, final Set<Name> expand) {

            final Layout output = Layout.simple(schema.getSchema(), expand);
            return from(resolver.resolve(schema, Constant.FALSE, ImmutableList.of(), expand, null), output);
        }

        default Stage aggregate(final List<String> group, final Map<String, TypedExpression<?>> expressions) {

            final Layout inputLayout = getLayout();
            final Layout outputLayout = Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));

            final List<Pair<String, Expression>> groups = group.stream().map(k -> {

                final TypedExpression<?> e = expressions.get(k);
                if(e == null) {
                    throw new UnsupportedOperationException("Grouped name " + k + " must appear in expressions");
                } else {
                    return Pair.of(k, expressions.get(k).getExpression());
                }

            }).collect(Collectors.toList());

            final Map<String, Aggregate> aggregates = expressions.entrySet().stream()
                    .filter(e -> e.getValue().getExpression().isAggregate())
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> (Aggregate)e.getValue().getExpression()));

            return then(AggregateTransform.builder()
                            .group(groups)
                            .aggregates(aggregates)
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout).build(), outputLayout);
        }

        default Stage expand(final QueryResolver resolver, final LinkableSchema schema, final Set<Name> expand, final Set<Bucket> buckets) {

            final Layout outputLayout = Layout.simple(schema.getSchema(), expand);

            return then(ExpandTransform.builder()
                            .expand(expand)
                            .schema(schema)
                            .buckets(buckets)
                            .resolver(resolver).build(), outputLayout);
        }

        default Stage filter(final Expression condition) {

            final Layout inputLayout = getLayout();

            return then(PredicateTransform.builder()
                            .inputLayout(inputLayout)
                            .predicate(condition).build(), inputLayout);
        }

        default Stage map(final Map<String, TypedExpression<?>> expressions) {

            final Layout inputLayout = getLayout();
            final Layout outputLayout = Layout.simple(Immutable.transformValues(expressions, (k, v) -> v.getType()));

            return then(ExpressionTransform.builder()
                            .inputLayout(inputLayout)
                            .outputLayout(outputLayout)
                            .expressions(Immutable.transformValues(expressions, (k, v) -> v.getExpression())).build(),
                    outputLayout);
        }

        default Stage sort(final List<Sort> sort) {

            final Layout inputLayout = getLayout();

            return then(SortTransform.<Row>builder()
                            .sort(sort).build(), inputLayout);
        }

        default Stage conform(final InstanceSchema schema, final Set<Name> expand) {

            return then(SchemaTransform.builder()
                            .schema(schema).expand(expand).build(), Layout.simple(schema.getSchema(), expand));
        }
    }
}
