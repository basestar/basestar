package io.basestar.graphql.api;

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
import io.basestar.graphql.subscription.SubscribeHandler;
import io.basestar.graphql.subscription.UnsubscribeHandler;
import io.basestar.graphql.wiring.Subscriber;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class GraphQLWebsocketAPI implements API {

    public interface SubscriptionResolver {

        CompletableFuture<String> resolveSub(APIRequest request);

        static SubscriptionResolver fromHeader(final String headerName) {

            return request -> {
                final String value = request.getFirstHeader(headerName);
                if(value == null) {
                    throw new IllegalStateException("Subscription request header missing");
                }
                return CompletableFuture.completedFuture(value);
            };
        }
    }

    private final GraphQL graphQL;

    private final SubscriptionResolver subscriptionResolver;

    private final SubscribeHandler subscribeHandler;

    private final UnsubscribeHandler unsubscribeHandler;

    @lombok.Builder(builderClassName = "Builder")
    GraphQLWebsocketAPI(final GraphQL graphQL, final SubscriptionResolver subscriptionResolver,
                        final SubscribeHandler subscribeHandler, final UnsubscribeHandler unsubscribeHandler) {

        this.graphQL = Nullsafe.require(graphQL);
        this.subscriptionResolver = Nullsafe.require(subscriptionResolver);
        this.subscribeHandler = Nullsafe.require(subscribeHandler);
        this.unsubscribeHandler = Nullsafe.require(unsubscribeHandler);
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
                    return handle(request, body);
            }

        } catch (final Exception e) {

            log.error("GraphQL query failed", e);
            return CompletableFuture.completedFuture(APIResponse.error(request, e));
        }
    }

    private CompletableFuture<APIResponse> handle(final APIRequest request, final RequestBody requestBody) {

        try {
            switch (requestBody.getType().toLowerCase()) {
                case "connection_init": {
                    return CompletableFuture.completedFuture(response(request, "connection_ack"));
                }
                case "connection_terminate": {
                    return subscriptionResolver.resolveSub(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        return unsubscribeHandler.unsubscribeAll(caller, sub)
                                .thenApply(ignored -> APIResponse.success(request));
                    });
                }
                case "start": {
                    return subscriptionResolver.resolveSub(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        final ExecutionInput input = requestBody.toInput(subscribeHandler, caller, sub);
                        return query(request, requestBody.getId(), input);
                    });
                }
                case "stop": {
                    return subscriptionResolver.resolveSub(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        return unsubscribeHandler.unsubscribe(caller, sub, requestBody.getId())
                                .thenApply(ignored -> APIResponse.success(request));
                    });
                }
                default:
                    throw new IllegalStateException("Received unknown message of type: " + requestBody.getType());
            }
        } catch (final Exception e) {
            log.error("Uncaught: {}", e.getMessage(), e);
            return CompletableFuture.completedFuture(response(request, "connection_error", requestBody.getId(), null));
        }
    }

    private CompletableFuture<APIResponse> query(final APIRequest request, final String id, final ExecutionInput input) {

        log.info("GraphQL request {}", input);
        return graphQL.executeAsync(input)
                .thenApply(response -> {
                    log.info("GraphQL response {}", response);
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

        public ExecutionInput toInput(final SubscribeHandler subscribeHandler, final Caller caller, final String sub) {

            final Subscriber subscriber = (schema, expression, expand) -> subscribeHandler.subscribe(caller, sub, id, schema.getQualifiedName().toString(), expression, expand);

            return ExecutionInput.newExecutionInput()
                    .operationName(payload.getOperationName())
                    .query(payload.getQuery())
                    .variables(Nullsafe.option(payload.getVariables()))
                    .context(GraphQLContext.newContext().of("caller", caller, "subscriber", subscriber).build())
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
