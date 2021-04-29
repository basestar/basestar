package io.basestar.graphql;

import graphql.GraphQL;
import io.basestar.api.APIFormat;
import io.basestar.api.APIRequest;
import io.basestar.auth.Caller;
import io.basestar.database.DatabaseServer;
import io.basestar.event.Emitter;
import io.basestar.graphql.api.GraphQLAPI;
import io.basestar.graphql.api.GraphQLWebsocketAPI;
import io.basestar.graphql.subscription.SubscriberIdSource;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.stream.DefaultHub;
import io.basestar.stream.Hub;
import io.basestar.stream.Subscriptions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

public class GraphQLWebsocketAPITest {

    private Namespace namespace() throws Exception {

        return Namespace.load(GraphQLTest.class.getResource("schema.yml"));
    }

    private GraphQL graphQL(final Namespace namespace) throws Exception {

        final MemoryStorage storage = MemoryStorage.builder().build();
        final DatabaseServer databaseServer = DatabaseServer.builder().namespace(namespace).storage(storage).build();

        return GraphQLAdaptor.builder().database(databaseServer).namespace(namespace).build().graphQL();
    }

    @Test
    public void testSubscribeUnsubscribe() throws Exception {

        final Namespace namespace = namespace();

        final Subscriptions subscriptions = mock(Subscriptions.class);
        when(subscriptions.subscribe(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(subscriptions.unsubscribe(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        final Emitter emitter = Emitter.skip();

        final SubscriberIdSource subscriberIdSource = ignored -> CompletableFuture.completedFuture("sub1");

        final Hub hub = DefaultHub.builder().subscriptions(subscriptions).namespace(namespace).emitter(emitter).build();
        final GraphQLWebsocketAPI api = GraphQLWebsocketAPI.builder().graphQL(graphQL(namespace)).hub(hub).subscriberIdSource(subscriberIdSource).build();

        final GraphQLAPI.RequestBody subscribe = new GraphQLAPI.RequestBody();
        subscribe.setQuery("subscription { subscribeTest1(id: \"x\") { id } }");

        final GraphQLWebsocketAPI.RequestBody start = new GraphQLWebsocketAPI.RequestBody();
        start.setId("1");
        start.setType("start");
        start.setPayload(subscribe);

        api.handle(APIRequest.request(Caller.SUPER, APIRequest.Method.POST, "", APIFormat.JSON, start)).get();

        verify(subscriptions, times(1)).subscribe(eq(Caller.SUPER), eq("sub1"), eq("1"), any(), any(), any(), any());

        final GraphQLWebsocketAPI.RequestBody stop = new GraphQLWebsocketAPI.RequestBody();
        stop.setId("1");
        stop.setType("stop");

        api.handle(APIRequest.request(Caller.SUPER, APIRequest.Method.POST, "", APIFormat.JSON, stop)).get();

        verify(subscriptions, times(1)).unsubscribe("sub1", "1");
    }
}
