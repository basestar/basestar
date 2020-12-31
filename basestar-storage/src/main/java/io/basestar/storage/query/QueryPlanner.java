package io.basestar.storage.query;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.AggregateExtractingVisitor;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.use.Use;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;

import java.util.*;
import java.util.stream.Collectors;

public interface QueryPlanner<T extends QueryStage> {

    T plan(QueryStageVisitor<T> visitor, LinkableSchema schema, Expression expression, List<Sort> sort, Set<Name> expand);

    class Default<T extends QueryStage> implements QueryPlanner<T> {

        @Override
        public T plan(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            return stage(visitor, schema, expression, sort, expand);
        }

        protected T stage(final QueryStageVisitor<T> visitor, final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            final boolean constExpr = expression != null && expression.isConstant();
            if(constExpr && !expression.evaluatePredicate(Context.init())) {
                return visitor.empty(schema, expand);
            } else {
                T stage = stage(visitor, schema);
                final Set<Name> remainingExpand = Immutable.removeAll(expand, stage.getLayout().getExpand());
                if(!remainingExpand.isEmpty()) {
                    stage = visitor.expand(stage, schema, remainingExpand);
                }
                // TODO check if expression is already covered in the stage
                if (!constExpr && expression != null) {
                    stage = visitor.filter(stage, expression);
                }
                // TODO check enclosing/equivalent rather than only equal
                if(!sort.isEmpty() && !sort.equals(stage.getSort())) {
                    stage = visitor.sort(stage, sort);
                }
                return stage;
            }
        }

        protected T stage(final QueryStageVisitor<T> visitor, final LinkableSchema schema) {

            if (schema instanceof ViewSchema) {
                return viewStage(visitor, (ViewSchema)schema);
            } else {
                return refStage(visitor, (ReferableSchema)schema);
            }
        }

        protected T refStage(final QueryStageVisitor<T> visitor, final ReferableSchema schema) {

            return visitor.conform(visitor.source(schema), schema);
        }

        protected T viewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema) {

            if (schema.isAggregating() || schema.isGrouping()) {
                return visitor.conform(aggViewStage(visitor, schema), schema);
            } else {
                return visitor.conform(mapViewStage(visitor, schema), schema);
            }
        }

        protected T viewFrom(final QueryStageVisitor<T> visitor, final ViewSchema schema) {

            final ViewSchema.From from = schema.getFrom();
            final LinkableSchema fromSchema = from.getSchema();
            return stage(visitor, fromSchema, schema.getWhere(), schema.getSort(), from.getExpand());
        }

        protected Map<String, TypedExpression<?>> viewExpressions(final ViewSchema schema) {

            final Map<String, TypedExpression<?>> output = new HashMap<>();
            for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
                final String name = entry.getKey();
                final Property property = entry.getValue();
                final TypedExpression<?> expression = Nullsafe.require(property.getTypedExpression());
                output.put(name, expression);
            }
            // Group names can be either properties in the view, or simple names of members / metadata in from
            final LinkableSchema fromSchema = schema.getFrom().getSchema();
            for(final String name : schema.getGroup()) {
                if(!output.containsKey(name)) {
                    final Use<?> typeOf = fromSchema.typeOf(Name.of(name));
                    output.put(name, TypedExpression.from(new NameConstant(name), typeOf));
                }
            }
            return output;
        }

        protected T aggViewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema) {

            final T stage = viewFrom(visitor, schema);
            return visitor.agg(stage, schema.getGroup(), viewExpressions(schema));
        }

        protected T mapViewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema) {

            // Must copy id into the view __key field or it will be lost
            final LinkableSchema fromSchema = schema.getFrom().getSchema();
            final Map<String, TypedExpression<?>> expressions = new HashMap<>(viewExpressions(schema));
            expressions.put(schema.id(), TypedExpression.from(new NameConstant(fromSchema.id()), fromSchema.typeOfId()));
            final T stage = viewFrom(visitor, schema);
            return visitor.map(stage, expressions);
        }
    }

    class AggregateSplitting<T extends QueryStage> extends Default<T> {

        @Override
        protected T aggViewStage(final QueryStageVisitor<T> visitor, final ViewSchema schema) {

            final LinkableSchema fromSchema = schema.getFrom().getSchema();
            final List<String> group = schema.getGroup();
            final Map<String, TypedExpression<?>> expressions = viewExpressions(schema);

            // Move complex expressions around aggregates into a post-map stage
            final Map<String, TypedExpression<?>> postAgg = new HashMap<>();
            boolean requiresPostAgg = false;
            final AggregateExtractingVisitor extractAggregates = new AggregateExtractingVisitor();
            for(final Map.Entry<String, TypedExpression<?>> entry : expressions.entrySet()) {
                final String name = entry.getKey();
                final TypedExpression<?> typedExpr = Nullsafe.require(entry.getValue());
                final Expression expr = typedExpr.getExpression();
                if(expr.hasAggregates()) {
                    requiresPostAgg = requiresPostAgg || !expr.isAggregate();
                    final Expression withoutAggregates = extractAggregates.visit(expr);
                    postAgg.put(name, TypedExpression.from(withoutAggregates, typedExpr.getType()));
                } else if(group.contains(name)) {
                    postAgg.put(name, TypedExpression.from(new NameConstant(name), typedExpr.getType()));
                } else {
                    throw new IllegalStateException("Property " + name + " must be group or aggregate");
                }
            }

            final InferenceContext inference = InferenceContext.from(fromSchema)
                    .overlay(Reserved.THIS, InferenceContext.from(schema));

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
            for(final String name : schema.getGroup()) {
                final TypedExpression<?> expr = Nullsafe.require(expressions.get(name));
                requiresPreAgg = requiresPreAgg || !isSimpleName(expr.getExpression());
                preAgg.put(name, expr);
                agg.put(name, expr);
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
                final Use<?> typeOf = inference.typeOf(original);
                agg.put(name, TypedExpression.from(original.copy(args), typeOf));
            }

            T stage = viewFrom(visitor, schema);
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
