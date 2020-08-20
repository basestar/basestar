package io.basestar.graphql.api;

/*-
 * #%L
 * basestar-graphql
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import graphql.*;
import io.basestar.api.API;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Caller;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class GraphQLAPI implements API {

    private final GraphQL graphQL;

    @lombok.Builder(builderClassName = "Builder")
    public GraphQLAPI(final GraphQL graphQL) {

        this.graphQL = graphQL;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {


            final Caller caller = request.getCaller();

            switch(request.getMethod()) {
                case HEAD:
                case OPTIONS:
                    return CompletableFuture.completedFuture(APIResponse.success(request));
                case GET:
                    final String query = request.getFirstQuery("query");
                    return query(request, ExecutionInput.newExecutionInput(query).build());
                case POST:
                    final RequestBody body = request.readBody(RequestBody.class);
                    return query(request, body.toInput(caller));
                default:
                    return CompletableFuture.completedFuture(APIResponse.error(request, ExceptionMetadata.notFound()));
            }

    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        return CompletableFuture.completedFuture(new OpenAPI());
    }

    private CompletableFuture<APIResponse> query(final APIRequest request, final ExecutionInput input) {

        if(log.isDebugEnabled()) {
            log.debug("GraphQL request: {}", input.toString().replaceAll("\n", " "));
        }
        return graphQL.executeAsync(input)
                .thenApply(response -> {
                    if(log.isDebugEnabled()) {
                        log.debug("GraphQL response: {}", response.toString().replaceAll("\n", " "));
                    }
                    return APIResponse.success(request, ResponseBody.from(response));
                });
    }

    @Data
    public static class RequestBody {

        private String operationName;

        private String query;

        private Map<String, Object> variables;

        private final Map<String, Object> extra = new HashMap<>();

        @JsonAnySetter
        public void putExtra(final String key, final Object value) {

            extra.put(key, value);
        }

        public ExecutionInput toInput(final Caller caller) {

            return ExecutionInput.newExecutionInput()
                    .operationName(operationName)
                    .query(query)
                    .variables(Nullsafe.orDefault(variables))
                    .context(GraphQLContext.newContext().of("caller", caller).build())
                    .build();
        }
    }

    @Data
    public static class ResponseBody {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Object data;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final List<ResponseError> errors;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final Map<Object, Object> extensions;

        public static ResponseBody from(final ExecutionResult response) {

            final List<ResponseError> errors = Optional.ofNullable(response.getErrors())
                    .map(errs -> errs.stream().map(ResponseError::from).collect(Collectors.toList()))
                    .orElse(null);
            return new ResponseBody(response.getData(), errors, response.getExtensions());
        }
    }

    @Data
    private static class ResponseError {

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final String message;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final List<Object> path;

        public static ResponseError from(final GraphQLError error) {

            return new ResponseError(error.getMessage(), error.getPath());
        }
    }
}
