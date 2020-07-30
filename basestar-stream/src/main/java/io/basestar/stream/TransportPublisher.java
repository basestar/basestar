package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.schema.ObjectSchema;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface TransportPublisher<T> extends Publisher {

    interface Transport<T> {

        CompletableFuture<?> send(String sub, String channel, T message);
    }

    default CompletableFuture<?> publish(final Caller caller, final ObjectSchema schema, final String sub, final String channel, final SubscriptionInfo info, final Change change) {

        return message(caller, schema, sub, channel, info, change)
                .thenCompose(message -> message.map(v -> transport().send(sub, channel, v))
                        .orElseGet(() -> CompletableFuture.completedFuture(null)));
    }

    Transport<? super T> transport();

    CompletableFuture<? extends Optional<? extends T>> message(Caller caller, ObjectSchema schema, String sub, String channel, SubscriptionInfo info, Change change);
}
