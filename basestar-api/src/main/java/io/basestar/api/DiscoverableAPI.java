package io.basestar.api;

import com.google.common.base.Suppliers;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class DiscoverableAPI implements API {

    private final API api;

    private final String path;

    private final Supplier<OpenAPI> openApi = Suppliers.memoize(this::rebuildOpenApi);

    @lombok.Builder(builderClassName = "Builder")
    DiscoverableAPI(final API api, final String path) {

        this.api = api;
        this.path = Nullsafe.option(path, "/api");
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        if(request.getPath().equals(path)) {
            return CompletableFuture.supplyAsync(() -> APIResponse.success(request, openApi()));
        } else {
            return api.handle(request);
        }
    }

    @Override
    public OpenAPI openApi() {

        return openApi.get();
    }

    private OpenAPI rebuildOpenApi() {

        return api.openApi();
    }
}
