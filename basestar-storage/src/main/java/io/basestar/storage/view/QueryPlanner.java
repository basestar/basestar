package io.basestar.storage.view;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.schema.*;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public interface QueryPlanner {

    QueryStage outputStage(LinkableSchema schema, Expression expression, List<Sort> sort, Set<Name> expand);

    class Default implements QueryPlanner {

        @Override
        public QueryStage outputStage(final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            if (schema instanceof ViewSchema) {
                return viewOutputStage((ViewSchema)schema, expression, sort, expand);
            } else {
                return refOutputStage((ReferableSchema)schema, expression, sort, expand);
            }
        }

        private QueryStage refOutputStage(final ReferableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            if(schema instanceof InterfaceSchema) {
                final List<QueryStage> sourceStages = schema.getConcreteExtended().stream()
                        .map(s -> objectOutputStage(s, expression, sort, expand))
                        .collect(Collectors.toList());
                return new UnionStage(sourceStages);
            } else {
                return objectOutputStage((ObjectSchema)schema, expression, sort, expand);
            }
        }

        private QueryStage objectOutputStage(final ObjectSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            return filterSortExpand(new SourceStage(schema), schema, expression, sort, Immutable.copyRemoveAll(expand, schema.getExpand()));
        }

        private QueryStage viewOutputStage(final ViewSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            final ViewSchema.From from = schema.getFrom();
            final Expression where = Nullsafe.orDefault(schema.getWhere(), Constant.TRUE);
            QueryStage stage = outputStage(from.getSchema(), where, ImmutableList.of(), from.getExpand());
            // TODO: Must have a map stage that splits aggregates to opt<map> -> agg -> opt<map>
            if (schema.isAggregating() || schema.isGrouping()) {
                stage = new AggStage(stage, schema.getGroup(), ImmutableMap.of());
            }
            stage = new MapStage(stage, Immutable.transformValues(schema.getProperties(), (k, v) -> v.getExpression()));
            return filterSortExpand(stage, schema, expression, sort, expand);
        }

        private QueryStage filterSortExpand(final QueryStage input, final LinkableSchema schema, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

            QueryStage output = input;
            if (expression.isConstant()) {
                if(!expression.evaluateAs(Boolean.class, Context.init())) {
                    return new EmptyStage();
                }
            } else {
                output = new FilterStage(output, expression);
            }
            if(!expand.isEmpty()) {
                final Map<String, Set<Name>> branches = Name.branch(expand);
                final Map<String, ExpandStage.Join> joins = branches.entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey, e -> join(schema, e.getKey(), e.getValue())
                ));
                output = new ExpandStage(output, joins);
            }
            if(!sort.isEmpty()) {
                output = new SortStage(output, sort);
            }
            return output;
        }

        private ExpandStage.Join join(final LinkableSchema schema, final String first, final Set<Name> rest) {

            final Member member = schema.requireMember(first, true);
            if(member instanceof Link) {
                final Link link = (Link)member;
                final QueryStage stage = outputStage(link.getSchema(), link.getExpression(), ImmutableList.of(), rest);
                return new ExpandStage.Join(stage, Constant.TRUE);
            } else {
                return new ExpandStage.Join(new EmptyStage(), Constant.TRUE);
            }
        }
    }
}
