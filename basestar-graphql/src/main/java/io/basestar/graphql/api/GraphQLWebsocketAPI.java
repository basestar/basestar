package io.basestar.graphql.api;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import graphql.ExecutionInput;
import graphql.GraphQL;
import graphql.GraphQLContext;
import io.basestar.api.API;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Caller;
import io.basestar.graphql.subscription.GraphQLSubscriptionMetadata;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.graphql.subscription.SubscriberIdSource;
import io.basestar.stream.Hub;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class GraphQLWebsocketAPI implements API {

    private final GraphQL graphQL;

    private final SubscriberIdSource subscriberIdSource;

    private final Hub hub;

    // FIXME: TEMPORARY HACK
    private final Map<String, Caller> callerCache = new HashMap<>();

    @lombok.Builder(builderClassName = "Builder")
    GraphQLWebsocketAPI(final GraphQL graphQL, final SubscriberIdSource subscriberIdSource, final Hub hub) {

        this.graphQL = Nullsafe.require(graphQL);
        this.subscriberIdSource = Nullsafe.require(subscriberIdSource);
        this.hub = Nullsafe.require(hub);
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        try {

            switch (request.getMethod()) {
                case HEAD:
                case OPTIONS:
                    return CompletableFuture.completedFuture(APIResponse.success(request));
                default:
                    final RequestBody body = request.readBody(RequestBody.class);
                    return handle(request, body)
                            .exceptionally(e -> {
                                log.error("Uncaught: {}", e.getMessage(), e);
                                return response(request, "connection_error", body.getId(), null);
                            });
            }

        } catch (final Exception e) {

            log.error("GraphQL query failed", e);
            return CompletableFuture.completedFuture(APIResponse.error(request, e));
        }
    }

    private CompletableFuture<APIResponse> handle(final APIRequest request, final RequestBody requestBody) {

        try {
            // Empty messages seem to be sent as keep-alives
            if(requestBody.getType() == null || requestBody.getType().isEmpty()) {
                return CompletableFuture.completedFuture(response(request, "connection_keep_alive"));
            }
            switch (requestBody.getType().toLowerCase()) {
                case "connection_init": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        caller(sub, request);
                        return CompletableFuture.completedFuture(response(request, "connection_ack"));
                    });
                }
                case "connection_terminate": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = caller(sub, request);
                        return hub.unsubscribeAll(caller, sub)
                                .thenApply(ignored -> response(request, "complete"));
                    });
                }
                case "start": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = caller(sub, request);
                        final ExecutionInput input = requestBody.toInput(hub, caller, sub);
                        return query(request, requestBody.getId(), input);
                    });
                }
                case "stop": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = caller(sub, request);
                        return hub.unsubscribe(caller, sub, requestBody.getId())
                                .thenApply(ignored -> response(request, "complete"));
                    });
                }
                default:
                    log.info("Received unknown message of type: {}", requestBody.getType());
                    return CompletableFuture.completedFuture(response(request, "connection_keep_alive"));
            }
        } catch (final Exception e) {
            log.error("Uncaught: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(response(request, "connection_error", requestBody.getId(), null));
        }
    }

    private Caller caller(final String sub, final APIRequest request) {

        return callerCache.computeIfAbsent(sub, ignored -> request.getCaller());
    }

    private CompletableFuture<APIResponse> query(final APIRequest request, final String id, final ExecutionInput input) {

        log.debug("GraphQL request {}", input);
        return graphQL.executeAsync(input)
                .thenApply(response -> {
                    log.debug("GraphQL response {}", response);
                    return response(request, "data", id, GraphQLAPI.ResponseBody.from(response));
                });
    }

    private APIResponse response(final APIRequest request, final String type) {

        return response(request, type, null, null);
    }

    private APIResponse response(final APIRequest request, final String type, final String id, final GraphQLAPI.ResponseBody payload) {

        final ResponseBody responseBody = new ResponseBody();
        responseBody.setType(type);
        responseBody.setId(id);
        responseBody.setPayload(payload);
        final Multimap<String, String> headers = HashMultimap.create();
        headers.put("Sec-Websocket-Protocol", "graphql-ws");
        return APIResponse.response(request, 200, headers, responseBody);
    }

    @Data
    public static class RequestBody {

        private String type;

        private String id;

        private GraphQLAPI.RequestBody payload;

        private final Map<String, Object> extra = new HashMap<>();

        @JsonAnySetter
        public void putExtra(final String key, final Object value) {

            extra.put(key, value);
        }

        public ExecutionInput toInput(final Hub hub, final Caller caller, final String sub) {

            final String channel = this.getId();
            final SubscriberContext subscriberContext = (schema, expression, alias, names, query) -> {
                final GraphQLSubscriptionMetadata info = new GraphQLSubscriptionMetadata(alias, names, query);
                return hub.subscribe(caller, sub, channel, schema.getQualifiedName(), expression, info);
            };

            return ExecutionInput.newExecutionInput()
                    .operationName(payload.getOperationName())
                    .query(payload.getQuery())
                    .variables(Nullsafe.orDefault(payload.getVariables()))
                    .context(GraphQLContext.newContext().of("caller", caller, "subscriber", subscriberContext).build())
                    .build();
        }
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ResponseBody {

        private String type;

        private String id;

        private GraphQLAPI.ResponseBody payload;
    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        return CompletableFuture.completedFuture(new OpenAPI());
    }
}
