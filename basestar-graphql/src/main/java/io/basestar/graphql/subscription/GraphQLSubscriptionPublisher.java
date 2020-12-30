package io.basestar.graphql.subscription;

import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.event.Handler;
import io.basestar.graphql.GraphQLStrategy;
import io.basestar.graphql.api.GraphQLAPI;
import io.basestar.graphql.api.GraphQLWebsocketAPI;
import io.basestar.graphql.transform.GraphQLResponseTransform;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.Change;
import io.basestar.stream.SubscriptionMetadata;
import io.basestar.stream.event.SubscriptionPublishEvent;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class GraphQLSubscriptionPublisher implements Handler<SubscriptionPublishEvent> {

    private final Database database;

    interface Transport {

        CompletableFuture<?> send(String sub, String channel, GraphQLWebsocketAPI.ResponseBody message);
    }

    private final Transport transport;

    private final GraphQLResponseTransform responseTransform;

    @lombok.Builder(builderClassName = "Builder")
    public GraphQLSubscriptionPublisher(final Database database,
                                        final GraphQLStrategy strategy,
                                        final Transport transport) {

        this.database = database;
        this.transport = transport;
        this.responseTransform = Nullsafe.orDefault(strategy, GraphQLStrategy.DEFAULT).responseTransform();
    }

    protected Transport transport() {

        return transport;
    }

    @Override
    public CompletableFuture<?> handle(final SubscriptionPublishEvent event, final Map<String, String> metadata) {

        return publish(event.getCaller(), event.getSub(), event.getChannel(), event.getChange(), event.getMetadata());
    }

    protected CompletableFuture<?> publish(final Caller caller, final String sub, final String channel, final Change change, final SubscriptionMetadata metadata) {

        return message(caller, sub, channel, change, metadata)
                .thenCompose(message -> message.map(v -> transport().send(sub, channel, v))
                        .orElseGet(() -> CompletableFuture.completedFuture(null)));
    }

    protected CompletableFuture<Optional<GraphQLWebsocketAPI.ResponseBody>> message(final Caller caller, final String sub, final String channel, final Change change,  final SubscriptionMetadata info) {

        if(info instanceof GraphQLSubscriptionMetadata) {
            final GraphQLSubscriptionMetadata gqlInfo = (GraphQLSubscriptionMetadata)info;

            if(change.getAfter() != null) {
                final String alias = gqlInfo.getAlias();
                final Set<Name> names = gqlInfo.getNames();
                final ObjectSchema schema = database.namespace().requireObjectSchema(change.getSchema());
                final Set<Name> expand = schema.requiredExpand(names);
                return database.expand(caller, change.getAfter(), expand)
                        .thenApply(after -> {
                            final Map<String, Object> data = new HashMap<>();
                            data.put(alias, responseTransform.toResponse(schema, after));
                            final GraphQLAPI.ResponseBody payload = new GraphQLAPI.ResponseBody(data, null, null);
                            final GraphQLWebsocketAPI.ResponseBody response = new GraphQLWebsocketAPI.ResponseBody();
                            response.setId(channel);
                            response.setType("data");
                            response.setPayload(payload);
                            return Optional.of(response);
                        });
            } else {
                return CompletableFuture.completedFuture(Optional.empty());
            }

        } else {
            throw new IllegalStateException("Subscription info of type " + info.getClass() + " not expected");
        }
    }
}
