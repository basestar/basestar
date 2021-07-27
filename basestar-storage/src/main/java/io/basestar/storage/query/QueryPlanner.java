package io.basestar.storage.query;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.ExpressionVisitor;
import io.basestar.expression.aggregate.AggregateExtractingVisitor;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Property;
import io.basestar.schema.Reserved;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.from.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.util.Bucket;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;
import lombok.RequiredArgsConstructor;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface QueryPlanner<T> {

    default T plan(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

        return plan(visitor, schema, expression, sort, expand, null);
    }

    T plan(QueryStageVisitor<T> visitor, LinkableSchema schema, Expression expression, List<Sort> sort, Set<Name> expand, Set<Bucket> buckets);

    class Default<T> implements QueryPlanner<T> {

        private final Predicate<ViewSchema> materialized;

        public Default(final boolean ignoreMaterialization) {

            this(view -> view.isMaterialized() && !ignoreMaterialization);
        }

        public Default(final Predicate<ViewSchema> materialized) {

            this.materialized = materialized;
        }

        @Override
        public T plan(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand, final Set<Bucket> buckets) {

            return stage(visitor, schema, expression, sort, expand, buckets);
        }

        protected T stage(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Expression filter, final List<Sort> sort, final Set<Name> expand, final Set<Bucket> buckets) {

            final boolean constFilter = filter != null && filter.isConstant();
            if(constFilter && !filter.evaluatePredicate(Context.init())) {

                return visitor.empty(schema, expand);

            } else {

                final Expression remainingFilter = constFilter ? null : filter;
                final Set<Name> remainingExpand;
                if(expand != null) {
                    remainingExpand = new HashSet<>(expand);
                    remainingExpand.removeAll(schema.getExpand());
                } else {
                    remainingExpand = null;
                }

                T stage = stage(visitor, schema, buckets);

                stage = preExpandFilter(visitor, stage, schema, remainingFilter);
                stage = preExpandSort(visitor, stage, schema, sort);
                if(remainingExpand != null && !remainingExpand.isEmpty()) {
                    stage = visitor.expand(stage, schema, remainingExpand, buckets);
                }
                stage = postExpandFilter(visitor, stage, schema, remainingFilter);
                stage = postExpandSort(visitor, stage, schema, sort);

                return stage;
            }
        }

        protected boolean filterBeforeExpand(final LinkableSchema schema, final Expression filter) {

            return schema.requiredExpand(filter.names()).isEmpty();
        }

        protected T preExpandFilter(final QueryStageVisitor<T> visitor, final T stage, final LinkableSchema schema, final Expression filter) {

            if(filter == null || !filterBeforeExpand(schema, filter)) {
                return stage;
            } else {
                return visitor.filter(stage, filter);
            }
        }

        protected T postExpandFilter(final QueryStageVisitor<T> visitor, final T stage, final LinkableSchema schema, final Expression filter) {

            if(filter == null || filterBeforeExpand(schema, filter)) {
                return stage;
            } else {
                return visitor.filter(stage, filter);
            }
        }

        protected boolean sortBeforeExpand(final LinkableSchema schema, final List<Sort> sort) {

            final Set<Name> names = sort.stream().map(Sort::getName).collect(Collectors.toSet());
            return schema.requiredExpand(names).isEmpty();
        }

        protected T preExpandSort(final QueryStageVisitor<T> visitor, final T stage, final LinkableSchema schema, final List<Sort> sort) {

            if(sort == null || sort.isEmpty() || !sortBeforeExpand(schema, sort)) {
                return stage;
            } else {
                return visitor.sort(stage, sort);
            }
        }

        protected T postExpandSort(final QueryStageVisitor<T> visitor, final T stage, final LinkableSchema schema, final List<Sort> sort) {

            if(sort == null || sort.isEmpty() || sortBeforeExpand(schema, sort)) {
                return stage;
            } else {
                return visitor.sort(stage, sort);
            }
        }

        protected T stage(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Set<Bucket> buckets) {

            if (schema instanceof ViewSchema) {
                return viewStage(visitor, (ViewSchema)schema, buckets);
            } else {
                return refStage(visitor, schema, buckets);
            }
        }

        protected T refStage(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Set<Bucket> buckets) {

            return visitor.conform(visitor.source(schema, buckets), schema, schema.getExpand());
        }

        protected T viewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema, final Set<Bucket> buckets) {

            if(materialized.test(schema)) {
                return refStage(visitor, schema, buckets);
//            } else if(schema.getFrom() instanceof FromSql) {
//                final FromSql from = (FromSql)schema.getFrom();
//                final T result = visitor.sql(from.getSql(), schema, Immutable.transformValues(from.getUsing(),
//                        (k, v) -> {
//                            final FromSchema from2 = (FromSchema)v;
//                            return stage(visitor, from2.getSchema(), Constant.TRUE, from2.getSort(), from2.getExpand(), null);
//                        }));
//                return visitor.conform(result, schema, schema.getExpand());
            } else {
                if (schema.isAggregating() || schema.isGrouping()) {
                    return visitor.conform(aggViewStage(visitor, schema, buckets), schema, schema.getExpand());
                } else {
                    return visitor.conform(mapViewStage(visitor, schema, buckets), schema, schema.getExpand());
                }
            }
        }

        protected T viewFrom(final QueryStageVisitor<T> visitor, final ViewSchema schema, final Set<Bucket> buckets) {

            return viewFrom(visitor, schema.getFrom(), schema.getWhere(), buckets);
        }

        protected T viewFrom(final QueryStageVisitor<T> visitor, final From from, final Expression where, final Set<Bucket> buckets) {

            return from.visit(new FromVisitor<T>() {
                @Override
                public T visitAgg(final FromAgg from) {

                    final T result = from.getFrom().visit(this);

                    final InferenceContext inference = from.getFrom().inferenceContext();

                    final List<String> group = from.getGroup();
                    final Map<String, TypedExpression<?>> expressions = from.typedAgg();

                    return aggViewStage(visitor, result, inference, group, expressions, buckets);
                }

                @Override
                public T visitAlias(final FromAlias from) {

                    return from.getFrom().visit(this);
                }

                @Override
                public T visitFilter(final FromFilter from) {

                    final T result = from.getFrom().visit(this);
                    return visitor.filter(result, from.getCondition());
                }

                @Override
                public T visitJoin(final FromJoin from) {

                    return viewFromJoin(visitor, from, where, buckets);
                }

                @Override
                public T visitMap(final FromMap from) {

                    final T result = from.getFrom().visit(this);
                    return visitor.map(result, from.typedMap());
                }

                @Override
                public T visitSchema(final FromSchema from) {

                    return viewFromSchema(visitor, from, where, buckets);
                }

                @Override
                public T visitSort(final FromSort from) {

                    final T result = from.getFrom().visit(this);
                    return visitor.sort(result, from.getSort());
                }

                @Override
                public T visitUnion(final FromUnion from) {

                    return viewFromUnion(visitor, from, where, buckets);
                }
            });
        }

//        protected T viewFrom(final QueryStageVisitor<T> visitor, final From from, final Expression where, final Set<Bucket> buckets) {
//
//            T result = viewFromImpl(visitor, from, where, buckets);
//            if (from.hasSelect()) {
//                if(from.isGrouping() || from.isAggregating()) {
//                    result = visitor.agg(result, from.getGroup(), from.selectExpressions());
//                } else {
//                    result = visitor.map(result, from.selectExpressions());
//                }
//            }
//            if(from.hasSort()) {
//                result = visitor.sort(result, from.getSort());
//            }
//            return result;
//        }
//
//        protected T viewFromImpl(final QueryStageVisitor<T> visitor, final From from, final Expression where, final Set<Bucket> buckets) {
//
//            if(from instanceof FromSchema) {
//                return viewFromSchema(visitor, (FromSchema)from, where, buckets);
//            } else if(from instanceof FromJoin) {
//                return viewFromJoin(visitor, (FromJoin)from, where, buckets);
//            } else if(from instanceof FromUnion) {
//                return viewFromUnion(visitor, (FromUnion) from, where, buckets);
//            } else {
//                throw new UnsupportedOperationException("View type " + from.getClass() + " not supported");
//            }
//        }

        protected T viewFromSchema(final QueryStageVisitor<T> visitor, final FromSchema from, final Expression where, final Set<Bucket> buckets) {

            final LinkableSchema fromSchema = from.getSchema();

//            final List<Sort> sort = from.getSort();
            return stage(visitor, fromSchema, where, Immutable.list(), from.getExpand(), buckets);
        }

        protected T viewFromJoin(final QueryStageVisitor<T> visitor, final FromJoin from, final Expression where, final Set<Bucket> buckets) {

            final Join join = from.getJoin();

            final T left = viewFrom(visitor, join.getLeft(), Constant.TRUE, buckets);
            final T right = viewFrom(visitor, join.getRight(), Constant.TRUE, buckets);

            final T result = visitor.join(left, right, join);
            return where == null ? result : visitor.filter(result, where);
        }

        protected T viewFromUnion(final QueryStageVisitor<T> visitor, final FromUnion from, final Expression where, final Set<Bucket> buckets) {

            final List<T> inputs = from.getUnion().stream().map(v -> viewFrom(visitor, v, Constant.TRUE, buckets))
                    .collect(Collectors.toList());

            final T result = visitor.union(inputs, false);
            return where == null ? result : visitor.filter(result, where);
        }

        protected Map<String, TypedExpression<?>> viewExpressions(final ViewSchema schema) {

            final Map<String, TypedExpression<?>> output = new HashMap<>();
            for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
                final String name = entry.getKey();
                final Property property = entry.getValue();
                final TypedExpression<?> expression = Nullsafe.orDefault(property.getTypedExpression(),
                        () -> TypedExpression.from(new NameConstant(property.getName()), property.getType()));
                output.put(name, expression);
            }
            // Group names can be either properties in the view, or simple names of members / metadata in from
            final From from = schema.getFrom();
            final InferenceContext context = from.inferenceContext();
            for(final String name : schema.getGroup()) {
                if(!output.containsKey(name)) {
                    final Use<?> typeOf = context.requireTypeOf(Name.of(name));
                    output.put(name, TypedExpression.from(new NameConstant(name), typeOf));
                }
            }
            return output;
        }


        protected T aggViewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema, final Set<Bucket> buckets) {

            final T fromStage = viewFrom(visitor, schema, null);

            final InferenceContext inference = schema.getFrom().inferenceContext()
                    .overlay(Reserved.THIS, InferenceContext.from(schema));

            final List<String> group = schema.getGroup();
            final Map<String, TypedExpression<?>> expressions = viewExpressions(schema);

            return aggViewStage(visitor, fromStage, inference, group, expressions, buckets);
        }

        protected T mapViewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema, final Set<Bucket> buckets) {

            final From from = schema.getFrom();
            final Map<String, TypedExpression<?>> expressions = new HashMap<>(viewExpressions(schema));
            // Must copy the view __key field or it will be lost
            expressions.put(schema.id(), TypedExpression.from(from.id(), from.typeOfId()));
            final T stage = viewFrom(visitor, schema, buckets);
            return visitor.map(stage, expressions);
        }

        protected T aggViewStage(QueryStageVisitor<T> visitor, T from, InferenceContext inference,
                                 List<String> group, Map<String, TypedExpression<?>> expressions, Set<Bucket> buckets) {

            return visitor.agg(from, group, expressions);
        }
    }

    class AggregateSplitting<T> extends Default<T> {

        public AggregateSplitting(final boolean ignoreMaterialization) {

            super(ignoreMaterialization);
        }

        public AggregateSplitting(final Predicate<ViewSchema> ignoreMaterialization) {

            super(ignoreMaterialization);
        }

        @RequiredArgsConstructor
        private static class CommonExpressionExtractingVisitor implements ExpressionVisitor.Defaulting<Expression> {

            private final Map<String, Expression> expressions;

            @Override
            public Expression visitDefault(final Expression expression) {

                final Optional<NameConstant> found = expressions.entrySet().stream()
                        .filter(e -> e.getValue().equals(expression))
                        .map(e -> new NameConstant(e.getKey()))
                        .findFirst();

                if (found.isPresent()) {
                    return found.get();
                } else {
                    return expression.copy(this::visit);
                }
            }
        }

        @Override
        protected T aggViewStage(final QueryStageVisitor<T> visitor, final T from, final InferenceContext inference,
                                 final List<String> group, final Map<String, TypedExpression<?>> expressions, final Set<Bucket> buckets) {

            // Move complex expressions around aggregates into a post-map stage
            final Map<String, TypedExpression<?>> postAgg = new HashMap<>();
            boolean requiresPostAgg = false;
            final AggregateExtractingVisitor extractAggregates = new AggregateExtractingVisitor();
            for (final Map.Entry<String, TypedExpression<?>> entry : expressions.entrySet()) {
                final String name = entry.getKey();
                final TypedExpression<?> typedExpr = Nullsafe.require(entry.getValue());
                final Expression expr = typedExpr.getExpression();
                final Map<String, Expression> otherExpressions = new HashMap<>();
                expressions.forEach((k, v) -> {
                    if (!k.equals(entry.getKey())) {
                        otherExpressions.put(k, v.getExpression());
                    }
                });
                final CommonExpressionExtractingVisitor extractCommonExpressions = new CommonExpressionExtractingVisitor(otherExpressions);
                if (expr.hasAggregates()) {
                    requiresPostAgg = requiresPostAgg || !expr.isAggregate();
                    final Expression withoutAggregates = extractAggregates.visit(expr);
                    final Expression withCommon = extractCommonExpressions.visit(withoutAggregates);
                    postAgg.put(name, TypedExpression.from(withCommon, typedExpr.getType()));
                } else if (group.contains(name)) {
                    postAgg.put(name, TypedExpression.from(new NameConstant(name), typedExpr.getType()));
                } else {
                    requiresPostAgg = true;
                    final Expression withCommon = extractCommonExpressions.visit(expr);
                    postAgg.put(name, TypedExpression.from(withCommon, typedExpr.getType()));
                }
            }

            final Map<String, ? extends Expression> extractedAgg;
            if(requiresPostAgg) {
                extractedAgg = extractAggregates.getAggregates();
            } else {
                extractedAgg = expressions.entrySet().stream()
                        .filter(e -> !group.contains(e.getKey()))
                        .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().getExpression()));
            }

            // Replace non-constant aggregate args with lookups to a pre-map stage
            boolean requiresPreAgg = false;
            final Map<String, TypedExpression<?>> agg = new HashMap<>();
            final Map<String, TypedExpression<?>> preAgg = new HashMap<>();
            for(final String name : group) {
                final TypedExpression<?> expr = Nullsafe.require(expressions.get(name));
                requiresPreAgg = requiresPreAgg || !isSimpleName(expr.getExpression());
                preAgg.put(name, expr);
                agg.put(name, new TypedExpression<>(new NameConstant(name), expr.getType()));
            }
            for(final Map.Entry<String, ? extends Expression> entry : extractedAgg.entrySet()) {
                final String name = entry.getKey();
                final Expression original = entry.getValue();
                final List<Expression> args = new ArrayList<>();
                for(final Expression expr : original.expressions()) {
                    if(expr.isConstant()) {
                        args.add(expr);
                    } else if(isSimpleName(expr)) {
                        args.add(expr);
                        preAgg.put(((NameConstant) expr).getName().first(), inference.typed(expr));
                    } else {
                        requiresPreAgg = true;
                        final String id = "_" + expr.digest();
                        args.add(new NameConstant(id));
                        preAgg.put(id, inference.typed(expr));
                    }
                }
                final Use<?> typeOf = inference.requireTypeOf(original);
                agg.put(name, TypedExpression.from(original.copy(args), typeOf));
            }

            T stage = from;
            if(requiresPreAgg) {
                stage = visitor.map(stage, preAgg);
            }
            stage = visitor.agg(stage, group, agg);
            if(requiresPostAgg) {
                stage = visitor.map(stage, postAgg);
            }
            return stage;
        }

        protected boolean isSimpleName(final Expression expr) {

            if(expr instanceof NameConstant) {
                final Name name = ((NameConstant) expr).getName();
                // Handling of this in aggregates (e.g. collectList(this)) is more
                // intuitive if we allow a preAgg stage
                return name.size() == 1 && !name.first().equals(Reserved.THIS);
            } else {
                return false;
            }
        }
    }
}
