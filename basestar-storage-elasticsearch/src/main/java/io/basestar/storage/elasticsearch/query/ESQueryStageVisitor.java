package io.basestar.storage.elasticsearch.query;

import io.basestar.expression.Expression;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.storage.elasticsearch.ElasticsearchStrategy;
import io.basestar.storage.query.QueryStageVisitor;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RequiredArgsConstructor
public class ESQueryStageVisitor implements QueryStageVisitor<ESQueryStage> {

    private final ElasticsearchStrategy strategy;

    @Override
    public ESQueryStage empty(final LinkableSchema schema, final Set<Name> expand) {

        return new ESQueryStage.Empty(Layout.simple(schema.getSchema(), expand));
    }

    @Override
    public ESQueryStage expand(final ESQueryStage input, final LinkableSchema schema, final Set<Name> expand) {

        return input;
    }

    @Override
    public ESQueryStage filter(final ESQueryStage input, final Expression condition) {

        return input.filter(condition);
    }

    @Override
    public ESQueryStage agg(final ESQueryStage input, final List<String> group, final Map<String, TypedExpression<?>> aggregates) {

        return input.agg(group, aggregates);
    }

    @Override
    public ESQueryStage map(final ESQueryStage input, final Map<String, TypedExpression<?>> expressions) {

        return input.map(expressions);
    }

    @Override
    public ESQueryStage sort(final ESQueryStage input, final List<io.basestar.util.Sort> sort) {

        return input.sort(sort);
    }

    @Override
    public ESQueryStage source(final LinkableSchema schema) {

        return new ESQueryStage.Source(strategy, schema);
    }

    @Override
    public ESQueryStage union(final List<ESQueryStage> inputs) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ESQueryStage conform(final ESQueryStage input, final InstanceSchema schema, final Set<Name> expand) {

        return input;
    }

    @Override
    public ESQueryStage sql(final String sql, final InstanceSchema schema, final Map<String, ESQueryStage> with) {

        throw new UnsupportedOperationException();
    }
}
