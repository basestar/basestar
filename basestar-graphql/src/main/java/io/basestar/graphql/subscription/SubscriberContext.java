package io.basestar.graphql.subscription;

import io.basestar.schema.ObjectSchema;
import io.basestar.util.Name;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface SubscriberContext {

    CompletableFuture<?> subscribe(ObjectSchema schema, String id, String alias, Set<Name> names);
}
