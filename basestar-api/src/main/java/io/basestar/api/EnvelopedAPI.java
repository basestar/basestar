package io.basestar.api;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.auth.Caller;
import io.basestar.util.Nullsafe;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 *
 */

@Slf4j
public class EnvelopedAPI implements API {

    private final API api;

    public EnvelopedAPI(final API api) {

        this.api = api;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

        final RequestBody body = request.readBody(RequestBody.class);
        return api.handle(new APIRequest() {
            @Override
            public Caller getCaller() {

                return request.getCaller();
            }

            @Override
            public Method getMethod() {

                return Nullsafe.option(body.getMethod(), request.getMethod());
            }

            @Override
            public String getPath() {

                return Nullsafe.option(body.getPath(), request.getPath());
            }

            @Override
            public Multimap<String, String> getQuery() {

                final Multimap<String, String> result = HashMultimap.create();
                if (body.getQuery() != null) {
                    body.getQuery().forEach(result::put);
                }
                request.getQuery().asMap().forEach((k, vs) -> {
                    if(!result.containsKey(k)) {
                        result.putAll(k, vs);
                    }
                });
                return result;
            }

            @Override
            public Multimap<String, String> getHeaders() {

                final Multimap<String, String> result = HashMultimap.create();
                if (body.getHeaders() != null) {
                    body.getHeaders().forEach(result::put);
                }
                request.getHeaders().asMap().forEach((k, vs) -> {
                    if(!result.containsKey(k)) {
                        result.putAll(k, vs);
                    }
                });
                return result;
            }

            @Override
            public InputStream readBody() throws IOException {

                final byte[] bytes = request.getContentType().getMapper().writeValueAsBytes(body.getBody());
                return new ByteArrayInputStream(bytes);
            }
        });
    }

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        return CompletableFuture.completedFuture(new OpenAPI());
    }

    @Data
    public static class RequestBody {

        private APIRequest.Method method;

        private Map<String, String> query;

        private Map<String, String> headers;

        private String path;

        private Object body;
    }
}
