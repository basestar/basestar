package io.basestar.graphql.subscription;

import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriberContext {

    default CompletableFuture<?> subscribe(final ObjectSchema schema, final String id, final String alias, final Set<Name> names) {

        return subscribe(schema, new Eq(new NameConstant(ObjectSchema.ID_NAME), new Constant(id)), alias, names);
    }

    CompletableFuture<?> subscribe(ObjectSchema schema, Expression expression, String alias, Set<Name> names);
}
