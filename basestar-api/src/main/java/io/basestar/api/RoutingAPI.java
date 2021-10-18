package io.basestar.api;

/*-
 * #%L
 * basestar-api
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

import io.swagger.v3.oas.models.OpenAPI;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RoutingAPI implements API {

    private final Map<String, API> apis;

    public RoutingAPI(final Map<String, API> apis) {

        this.apis = apis.entrySet().stream().collect(Collectors.toMap(e -> normalize(e.getKey()), Map.Entry::getValue));
    }

    private String normalize(final String path) {

        return addLeadingSlash(removeTrailingSlash(path));
    }

    private String removeTrailingSlash(final String path) {

        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        } else {
            return path;
        }
    }

    private String addLeadingSlash(final String path) {

        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

        // FIXME: use longest match
        final String path = request.getPath();
        for (final Map.Entry<String, API> entry : apis.entrySet()) {
            final String key = entry.getKey();
            if (path.equals(key)) {
                return entry.getValue().handle(new APIRequest.Delegating(request) {
                    @Override
                    public String getPath() {
                        return "/";
                    }
                });
            } else if (path.startsWith(key + "/")) {
                return entry.getValue().handle(new APIRequest.Delegating(request) {
                    @Override
                    public String getPath() {
                        return path.substring(key.length());
                    }
                });
            }
        }
        final API defaultAPI = apis.get("/");
        if (defaultAPI != null) {
            return defaultAPI.handle(request);
        } else {
            return CompletableFuture.completedFuture(APIResponse.response(request, 404, null));
        }
    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        final Map<String, CompletableFuture<OpenAPI>> futures = apis.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        v -> v.getValue().openApi()
                ));

        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture<?>[0])).thenApply(ignored -> {

            final List<OpenAPI> results = futures.entrySet().stream()
                    .map(e -> OpenAPIUtils.prefix(e.getValue().getNow(new OpenAPI()), e.getKey()))
                    .collect(Collectors.toList());
            return OpenAPIUtils.merge(results);
        });
    }
}
