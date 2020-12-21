package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Layout;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface QueryStageVisitor<T> {

    Pair<T, Layout> aggregate(Pair<T, Layout> input, List<String> group, Map<String, Aggregate> aggregates, Map<String, Use<?>> output);

    Pair<T, Layout> empty(LinkableSchema schema, Set<Name> expand);

    Pair<T, Layout> expand(Pair<T, Layout> input, LinkableSchema schema, Set<Name> expand);

    Pair<T, Layout> filter(Pair<T, Layout> input, Expression condition);

    Pair<T, Layout> map(Pair<T, Layout> input, Map<String, Expression> expressions, Map<String, Use<?>> output);

    Pair<T, Layout> sort(Pair<T, Layout> input, List<Sort> sort);

    Pair<T, Layout> source(LinkableSchema schema);

    Pair<T, Layout> union(List<Pair<T, Layout>> inputs);

    Pair<T, Layout> schema(Pair<T, Layout> input, InstanceSchema schema);
}
