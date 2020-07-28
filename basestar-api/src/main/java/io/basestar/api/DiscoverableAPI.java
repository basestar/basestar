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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("Guava")
public class DiscoverableAPI implements API {

    private final API api;

    private final String path;

    private final Supplier<CompletableFuture<OpenAPI>> openApi = Suppliers.memoize(this::rebuildOpenApi);

    @lombok.Builder(builderClassName = "Builder")
    DiscoverableAPI(final API api, final String path) {

        this.api = api;
        this.path = Nullsafe.option(path, "/api");
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

        if(request.getPath().equals(path)) {
            return openApi().thenApply(v -> APIResponse.success(request, v));
        } else {
            return api.handle(request);
        }
    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        return openApi.get();
    }

    private CompletableFuture<OpenAPI> rebuildOpenApi() {

        return api.openApi();
    }
}
