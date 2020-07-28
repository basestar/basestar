package io.basestar.graphql.wiring;

import io.basestar.expression.Expression;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Subscriber {

    CompletableFuture<?> subscribe(ObjectSchema schema, Expression expression, Set<Name> expand);
}
