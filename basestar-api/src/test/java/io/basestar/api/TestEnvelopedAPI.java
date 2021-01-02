package io.basestar.api;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.swagger.v3.oas.models.OpenAPI;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestEnvelopedAPI {

    @Test
    void testEnveloped() throws Exception {

        final EnvelopedAPI api = new EnvelopedAPI(new API() {
            @Override
            public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

                return CompletableFuture.completedFuture(APIResponse.response(request, 200, ImmutableMap.of(
                        "method", request.getMethod().toString(),
                        "path", request.getPath(),
                        "allowed", request.getHeaders().get("x-allowedheader"),
                        "denied", request.getHeaders().get("x-deniedheader")
                )));
            }

            @Override
            public CompletableFuture<OpenAPI> openApi() {

                return CompletableFuture.completedFuture(null);
            }
        });

        final EnvelopedAPI.RequestBody body = new EnvelopedAPI.RequestBody();
        body.setMethod(APIRequest.Method.PUT);
        body.setPath("/another/path");
        body.setHeaders(ImmutableMap.of(
                "X-AllowedHeader", "allowed",
                "X-DeniedHeader", "not allowed"
        ));

        final APIResponse response = api.handle(APIRequest.request(Caller.SUPER, APIRequest.Method.POST, "/", ImmutableMap.of(),
                ImmutableMap.of("X-DeniedHeader", "allowed"), APIFormat.JSON, body)).join();

        assertEquals(200, response.getStatusCode());
        assertEquals(APIFormat.JSON, response.getContentType());
        final Map<?, ?> values = response.readAs(Map.class);
        assertEquals(APIRequest.Method.PUT.toString(), values.get("method"));
        assertEquals("/another/path", values.get("path"));
        assertEquals(ImmutableList.of("allowed"), values.get("allowed"));
        assertEquals(ImmutableList.of("allowed"), values.get("denied"));
    }

}
