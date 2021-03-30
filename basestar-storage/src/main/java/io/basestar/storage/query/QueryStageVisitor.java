package io.basestar.storage.query;

import io.basestar.expression.Expression;
import io.basestar.schema.InstanceSchema;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.expression.TypedExpression;
import io.basestar.schema.from.Join;
import io.basestar.schema.util.Bucket;
import io.basestar.util.Name;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface QueryStageVisitor<T> {

    T empty(LinkableSchema schema, Set<Name> expand);

    T expand(T input, LinkableSchema schema, Set<Name> expand, Set<Bucket> buckets);

    T filter(T input, Expression condition);

    T agg(T input, List<String> group, Map<String, TypedExpression<?>> aggregates);

    T map(T input, Map<String, TypedExpression<?>> expressions);

    T sort(T input, List<Sort> sort);

    T source(LinkableSchema schema, Set<Bucket> buckets);

    T union(List<T> inputs);

    T conform(T input, InstanceSchema schema, Set<Name> expand);

    @Deprecated
    T sql(String sql, InstanceSchema schema, Map<String, T> with);

    T join(T left, T right, Join join);
}
