package io.basestar.graphql.subscription;

import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.graphql.GraphQLStrategy;
import io.basestar.graphql.api.GraphQLAPI;
import io.basestar.graphql.api.GraphQLWebsocketAPI;
import io.basestar.graphql.transform.GraphQLResponseTransform;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.Change;
import io.basestar.stream.SubscriptionMetadata;
import io.basestar.stream.TransportPublisher;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class GraphQLSubscriptionPublisher implements TransportPublisher<GraphQLWebsocketAPI.ResponseBody> {

    private final Database database;

    private final Transport<? super GraphQLWebsocketAPI.ResponseBody> transport;

    private final GraphQLResponseTransform responseTransform;

    @lombok.Builder(builderClassName = "Builder")
    public GraphQLSubscriptionPublisher(final Database database,
                                        final GraphQLStrategy strategy,
                                        final Transport<? super GraphQLWebsocketAPI.ResponseBody> transport) {

        this.database = database;
        this.transport = transport;
        this.responseTransform = Nullsafe.orDefault(strategy, GraphQLStrategy.DEFAULT).responseTransform();
    }

    @Override
    public Transport<? super GraphQLWebsocketAPI.ResponseBody> transport() {

        return transport;
    }

    @Override
    public CompletableFuture<Optional<GraphQLWebsocketAPI.ResponseBody>> message(final Caller caller, final String sub, final String channel, final Change change,  final SubscriptionMetadata info) {

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
