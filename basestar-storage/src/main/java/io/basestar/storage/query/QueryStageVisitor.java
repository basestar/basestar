package io.basestar.storage.query;

import io.basestar.expression.Expression;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface QueryStageVisitor<T> {

    T empty(LinkableSchema schema, Set<Name> expand);

    T expand(T input, LinkableSchema schema, Set<Name> expand);

    T filter(T input, Expression condition);

    T agg(T input, List<String> group, Map<String, TypedExpression<?>> aggregates);

    T map(T input, Map<String, TypedExpression<?>> expressions);

    T sort(T input, List<Sort> sort);

    T source(LinkableSchema schema);

    T union(List<T> inputs);

    T conform(T input, InstanceSchema schema, Set<Name> expand);
}
