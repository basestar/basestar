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

import com.google.common.collect.Maps;
import io.swagger.v3.oas.models.OpenAPI;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RoutingAPI implements API {

    private final Map<String, API> apis;

    public RoutingAPI(final Map<String, API> apis) {

        this.apis = apis.entrySet().stream().collect(Collectors.toMap(e -> normalize(e.getKey()), Map.Entry::getValue));
    }

    private String normalize(final String path) {

        if(!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        for(final Map.Entry<String, API> entry : apis.entrySet()) {
            if(request.getPath().startsWith(entry.getKey())) {
                return entry.getValue().handle(new APIRequest.Delegating(request));
            }
        }
        return CompletableFuture.completedFuture(APIResponse.response(request, 404, null));
    }

    @Override
    public OpenAPI openApi() {

        return OpenAPIUtils.merge(Maps.transformValues(apis, API::openApi));
    }
}
