package io.basestar.storage.view;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.expression.aggregate.AggregateExtractingVisitor;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.expression.InferenceVisitor;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseBinary;
import io.basestar.schema.use.UseString;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;

import java.util.*;

public interface QueryPlanner {

    QueryStage plan(LinkableSchema schema, Expression expression, List<Sort> sort, Set<Name> expand);

    class Default implements QueryPlanner {

        public static final QueryPlanner INSTANCE = new Default();

        @Override
        public QueryStage plan(final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            final boolean constExpr = expression != null && expression.isConstant();
            if(constExpr && !expression.evaluatePredicate(Context.init())) {
                return new EmptyStage(schema, expand);
            } else {
                QueryStage stage = stage(schema);
                final Set<Name> expandAfter = new HashSet<>(expand);
                if (!constExpr && expression != null) {
                    final Set<Name> expandBefore = schema.requiredExpand(expression.names());
                    if(!expandBefore.isEmpty()) {
                        expandBefore.forEach(e1 -> expandAfter.removeIf(e2 -> e2.isChildOrEqual(e1)));
                        stage = new ExpandStage(stage, schema, expandBefore);
                    }
                    stage = new FilterStage(stage, expression);
                }
                if(!sort.isEmpty()) {
                    stage = new SortStage(stage, sort);
                }
                if(!expandAfter.isEmpty()) {
                    stage = new ExpandStage(stage, schema, expand);
                }
                return stage;
            }
        }

        protected QueryStage stage(final LinkableSchema schema) {

            if (schema instanceof ViewSchema) {
                return viewStage((ViewSchema)schema);
            } else {
                return refStage((ReferableSchema)schema);
            }
        }

        protected QueryStage refStage(final ReferableSchema schema) {

            return new SchemaStage(new SourceStage(schema), schema);
        }

        protected QueryStage viewStage(final ViewSchema schema) {

            final ViewSchema.From from = schema.getFrom();
            final LinkableSchema fromSchema = from.getSchema();
            QueryStage stage = plan(fromSchema, schema.getWhere(), schema.getSort(), from.getExpand());
            final Map<String, Use<?>> outputSchema = schema.getSchema();
            if (schema.isAggregating() || schema.isGrouping()) {
                final List<String> group = schema.getGroup();

                // Extract aggregates and create output map stage
                final Map<String, Expression> output = new HashMap<>();
                final AggregateExtractingVisitor visitor = new AggregateExtractingVisitor();
                for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
                    final String name = entry.getKey();
                    final Expression expr = Nullsafe.require(entry.getValue().getExpression());
                    if(expr.isAggregate()) {
                        final Expression withoutAggregates = visitor.visit(expr);
                        output.put(name, withoutAggregates);
                    } else if(group.contains(name)) {
                        output.put(name, new NameConstant(name));
                    } else {
                        throw new IllegalStateException("Property " + name + " must be group or aggregate");
                    }
                }

                final Map<String, Expression> input = new HashMap<>();
                final Map<String, Use<?>> inputSchema = new HashMap<>();
                final Map<String, Use<?>> aggSchema = new HashMap<>();
                for(final Map.Entry<String, Property> entry : schema.getGroupProperties().entrySet()) {
                    final String name = entry.getKey();
                    final Property property = entry.getValue();
                    input.put(name, Nullsafe.require(property.getExpression()));
                    inputSchema.put(name, property.getType());
                    aggSchema.put(name, property.getType());
                }

                final InferenceContext context = InferenceContext.from(from.getSchema())
                        .overlay(Reserved.THIS, InferenceContext.from(schema));
                final InferenceVisitor inference = new InferenceVisitor(context);

                // Replace non-constant aggregate args with lookups to the first map stage
                final Map<String, Aggregate> aggregates = new HashMap<>();
                for(final Map.Entry<String, Aggregate> entry : visitor.getAggregates().entrySet()) {
                    final String name = entry.getKey();
                    final Aggregate aggregate = entry.getValue();
                    final List<Expression> args = new ArrayList<>();
                    for(final Expression expr : aggregate.expressions()) {
                        if(expr.isConstant()) {
                            args.add(expr);
                        } else {
                            final String id = "_" + expr.digest();
                            args.add(new NameConstant(id));
                            input.put(id, expr);
                            inputSchema.put(id, inference.visit(expr));
                        }
                    }
                    aggregates.put(name, aggregate.copy(args));
                    aggSchema.put(name, inference.visit(aggregate));
                }

                stage = new MapStage(stage, input, Layout.simple(inputSchema, ImmutableSet.of()));
                stage = new AggStage(stage, group, aggregates, Layout.simple(aggSchema, ImmutableSet.of()));
                stage = new MapStage(stage, output, Layout.simple(outputSchema, ImmutableSet.of()));
                stage = new SchemaStage(stage, schema);

            } else {
                final Map<String, Expression> input = new HashMap<>();
                final Map<String, Use<?>> inputSchema = new HashMap<>();
                for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
                    final String name = entry.getKey();
                    final Property property = entry.getValue();
                    input.put(name, Nullsafe.require(property.getExpression()));
                    inputSchema.put(name, property.getType());
                }
                if(fromSchema instanceof ReferableSchema) {
                    input.put(Reserved.PREFIX + ReferableSchema.ID, new NameConstant(ReferableSchema.ID));
                    inputSchema.put(Reserved.PREFIX + ReferableSchema.ID, UseString.DEFAULT);
                } else {
                    input.put(ViewSchema.KEY, new NameConstant(ViewSchema.KEY));
                    inputSchema.put(ViewSchema.KEY, UseBinary.DEFAULT);
                }
                stage = new MapStage(stage, input, Layout.simple(inputSchema, ImmutableSet.of()));
                stage = new SchemaStage(stage, schema);
            }
            return stage;
        }
    }
}
