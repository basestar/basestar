package io.basestar.stream;

import io.basestar.auth.Caller;
import io.basestar.event.Handler;
import io.basestar.stream.event.SubscriptionPublishEvent;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface TransportPublisher<T> extends Handler<SubscriptionPublishEvent> {

    interface Transport<T> {

        CompletableFuture<?> send(String sub, String channel, T message);
    }

    default CompletableFuture<?> publish(final Caller caller, final String sub, final String channel, final Change change, final SubscriptionMetadata metadata) {

        return message(caller, sub, channel, change, metadata)
                .thenCompose(message -> message.map(v -> transport().send(sub, channel, v))
                        .orElseGet(() -> CompletableFuture.completedFuture(null)));
    }

    Transport<? super T> transport();

    CompletableFuture<? extends Optional<? extends T>> message(Caller caller, String sub, String channel, Change change, SubscriptionMetadata metadata);

    @Override
    default CompletableFuture<?> handle(final SubscriptionPublishEvent event, final Map<String, String> metadata) {

        return publish(event.getCaller(), event.getSub(), event.getChannel(), event.getChange(), event.getMetadata());
    }
}
