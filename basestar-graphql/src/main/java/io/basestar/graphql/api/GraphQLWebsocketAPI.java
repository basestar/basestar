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
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.graphql.subscription.GraphQLSubscriptionInfo;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.graphql.subscription.SubscriberIdSource;
import io.basestar.schema.ObjectSchema;
import io.basestar.stream.Subscribable;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class GraphQLWebsocketAPI implements API {

    private final GraphQL graphQL;

    private final SubscriberIdSource subscriberIdSource;

    private final Subscribable subscribable;

    @lombok.Builder(builderClassName = "Builder")
    GraphQLWebsocketAPI(final GraphQL graphQL, final SubscriberIdSource subscriberIdSource,
                        final Subscribable subscribable) {

        this.graphQL = Nullsafe.require(graphQL);
        this.subscriberIdSource = Nullsafe.require(subscriberIdSource);
        this.subscribable = Nullsafe.require(subscribable);
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
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        return subscribable.unsubscribeAll(caller, sub)
                                .thenApply(ignored -> APIResponse.success(request));
                    });
                }
                case "start": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        final ExecutionInput input = requestBody.toInput(subscribable, caller, sub);
                        return query(request, requestBody.getId(), input);
                    });
                }
                case "stop": {
                    return subscriberIdSource.subscriberId(request).thenCompose(sub -> {
                        Nullsafe.require(sub);
                        final Caller caller = request.getCaller();
                        return subscribable.unsubscribe(caller, sub, requestBody.getId())
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

        public ExecutionInput toInput(final Subscribable subscribable, final Caller caller, final String sub) {

            final SubscriberContext subscriberContext = (schema, id, alias, names) -> {
                final Expression expression = new Eq(new NameConstant(ObjectSchema.ID_NAME), new Constant(id));
                final GraphQLSubscriptionInfo info = new GraphQLSubscriptionInfo(alias, names);
                return subscribable.subscribe(caller, sub, id, schema.getQualifiedName().toString(), expression, info);
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
