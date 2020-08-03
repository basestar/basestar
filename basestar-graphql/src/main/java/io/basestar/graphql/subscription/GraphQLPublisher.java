package io.basestar.graphql.subscription;

import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.graphql.api.GraphQLAPI;
import io.basestar.graphql.api.GraphQLWebsocketAPI;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.Change;
import io.basestar.stream.SubscriptionInfo;
import io.basestar.stream.TransportPublisher;
import io.basestar.util.Name;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class GraphQLPublisher implements TransportPublisher<GraphQLWebsocketAPI.ResponseBody> {

    private final Database database;

    private final Transport<? super GraphQLWebsocketAPI.ResponseBody> transport;

    @lombok.Builder(builderClassName = "Builder")
    public GraphQLPublisher(final Database database,
                            final Transport<? super GraphQLWebsocketAPI.ResponseBody> transport) {

        this.database = database;
        this.transport = transport;
    }

    @Override
    public Transport<? super GraphQLWebsocketAPI.ResponseBody> transport() {

        return transport;
    }

    @Override
    public CompletableFuture<Optional<GraphQLWebsocketAPI.ResponseBody>> message(final Caller caller, final ObjectSchema schema, final String sub, final String channel, final SubscriptionInfo info, final Change change) {

        if(info instanceof GraphQLSubscriptionInfo) {
            final GraphQLSubscriptionInfo gqlInfo = (GraphQLSubscriptionInfo)info;

            if(change.getAfter() != null) {
                final String alias = gqlInfo.getAlias();
                final Set<Name> names = gqlInfo.getNames();
                final Set<Name> expand = schema.requiredExpand(names);
                return database.expand(caller, change.getAfter(), expand)
                        .thenApply(after -> {
                            final Map<String, Object> data = new HashMap<>();
                            data.put(alias, GraphQLUtils.toResponse(schema, after));
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
