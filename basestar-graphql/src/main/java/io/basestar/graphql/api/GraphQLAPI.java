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

import com.fasterxml.jackson.annotation.JsonInclude;
import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import io.basestar.api.API;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.util.Nullsafe;
import lombok.Data;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class GraphQLAPI implements API {

    private final GraphQL graphQL;

    public GraphQLAPI(final GraphQL graphQL) {

        this.graphQL = graphQL;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        try {

//            if(request.getPath().isEmpty()) {
                switch(request.getMethod()) {
                    case HEAD:
                    case OPTIONS:
                        return CompletableFuture.completedFuture(APIResponse.success(request));
                    case GET:
                        final String query = request.getFirstQuery("query");
                        return query(request, ExecutionInput.newExecutionInput(query).build());
                    case POST:
                        final Request req;
                        try(final InputStream is = request.readBody()) {
                             req = request.getContentType().getMapper().readValue(is, Request.class);
                        }
                        return query(request, req.toInput());
                    default:
                        return CompletableFuture.completedFuture(APIResponse.error(request, ExceptionMetadata.notFound()));
                }

//            } else {
//                return CompletableFuture.completedFuture(APIResponse.error(request, ExceptionMetadata.notFound()));
//            }

        } catch (final Exception e) {

            return CompletableFuture.completedFuture(APIResponse.error(request, e));
        }
    }

    private CompletableFuture<APIResponse> query(final APIRequest request, final ExecutionInput input) {

        return graphQL.executeAsync(input)
                .thenApply(response -> APIResponse.success(request, Response.from(response)));
    }

    @Data
    private static class Request {

        private String operationName;

        private String query;

        private Map<String, Object> variables;

        public ExecutionInput toInput() {

            return ExecutionInput.newExecutionInput()
                    .operationName(operationName)
                    .query(query)
                    .variables(Nullsafe.of(variables))
                    .build();
        }
    }

    @Data
    private static class Response {

        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Object data;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final List<GraphQLError> errors;

        @JsonInclude(JsonInclude.Include.NON_EMPTY)
        private final Map<Object, Object> extensions;

        public static Response from(final ExecutionResult response) {

            return new Response(response.getData(), response.getErrors(), response.getExtensions());
        }
    }
}
