package io.basestar.storage.view;

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.use.Use;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface QueryStageVisitor<T extends QueryStage> {

    T aggregate(T input, List<String> group, Map<String, Aggregate> aggregates, Map<String, Use<?>> output);

    T empty(LinkableSchema schema, Set<Name> expand);

    T expand(T input, LinkableSchema schema, Set<Name> expand);

    T filter(T input, Expression condition);

    T map(T input, Map<String, Expression> expressions, Map<String, Use<?>> output);

    T sort(T input, List<Sort> sort);

    T source(LinkableSchema schema);

    T union(List<T> inputs);

    T schema(T input, InstanceSchema schema);
}
