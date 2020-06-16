package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.expression.Expression;
import io.basestar.storage.util.Pager;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Subscriber {

    CompletableFuture<?> create(Caller caller, String sub, String channel, Expression expression, Set<Subscription.Key> keys, Set<Path> expand);

    Pager<Subscription> listBySubAndChannel(String sub, String channel, PagingToken token);

    Pager<Subscription> listBySub(String sub, PagingToken token);

    Pager<Subscription> listByKeys(Set<Subscription.Key> keys, PagingToken token);

    CompletableFuture<?> delete(Set<String> ids);
}
