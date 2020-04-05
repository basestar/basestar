package io.basestar.api;

import java.util.concurrent.CompletableFuture;

public interface API {

    CompletableFuture<APIResponse> handle(APIRequest request);
}
