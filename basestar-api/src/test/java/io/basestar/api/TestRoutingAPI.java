package io.basestar.api;

import com.google.common.collect.ImmutableMap;
import io.basestar.auth.Caller;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRoutingAPI {

    @Test
    void testRouting() throws Exception {

        final RoutingAPI api = new RoutingAPI(ImmutableMap.of(
                "", mockAPI("root"),
                "r1", mockAPI("r1"),
                "/r2", mockAPI("r2"),
                "r3", new RoutingAPI(ImmutableMap.of(
                        "r1", mockAPI("r3:1"),
                        "r2", mockAPI("r3:2")
                ))
        ));

        assertEquals("root", checkAPI(api, ""));
        assertEquals("root", checkAPI(api, "/"));
        assertEquals("r1", checkAPI(api, "/r1"));
        assertEquals("r2", checkAPI(api, "/r2"));
        assertEquals("r3:1", checkAPI(api, "/r3/r1"));
        assertEquals("r3:2", checkAPI(api, "/r3/r2"));

        final OpenAPI openAPI = api.openApi().join();
        assertTrue(openAPI.getPaths().containsKey("/"));
        assertTrue(openAPI.getPaths().containsKey("/r1/"));
        assertTrue(openAPI.getPaths().containsKey("/r2/"));
        assertTrue(openAPI.getPaths().containsKey("/r3/r1/"));
        assertTrue(openAPI.getPaths().containsKey("/r3/r2/"));
        System.err.println(openAPI);
    }

    private API mockAPI(final String api) {

        return new API() {
            @Override
            public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

                return CompletableFuture.completedFuture(APIResponse.success(request, ImmutableMap.of("api", api)));
            }

            @Override
            public CompletableFuture<OpenAPI> openApi() {

                final OpenAPI api = new OpenAPI();
                final PathItem path = new PathItem();
                final Operation getOp = new Operation();
                getOp.setOperationId("get");
                path.get(getOp);
                api.path("/", path);
                return CompletableFuture.completedFuture(api);
            }
        };
    }

    private String checkAPI(final API api, final String path) throws Exception {

        final APIResponse response = api.handle(APIRequest.get(Caller.SUPER, path)).get();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            response.writeTo(baos);
            try (final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray())) {
                return (String) (APIFormat.JSON.getMapper().readValue(bais, Map.class).get("api"));
            }
        }
    }
}
