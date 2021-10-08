package io.basestar.api;


import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.basestar.auth.Authenticator;
import io.basestar.auth.BasicAuthenticator;
import io.basestar.auth.Caller;
import io.basestar.auth.exception.AuthenticationFailedException;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestAuthenticatingAPI {

    final Authenticator authenticator = new BasicAuthenticator() {
        @Override
        protected CompletableFuture<Caller> verify(final String username, final String password) {

            return CompletableFuture.completedFuture(Caller.builder().setId(username).build());
        }

        @Override
        public Map<String, SecurityScheme> openApi() {

            return null;
        }
    };

    final AuthenticatingAPI api = new AuthenticatingAPI(authenticator, new API() {
        @Override
        public CompletableFuture<APIResponse> handle(final APIRequest request) throws IOException {

            return CompletableFuture.completedFuture(APIResponse.success(request, request.getCaller()));
        }

        @Override
        public CompletableFuture<OpenAPI> openApi() {

            return null;
        }
    });

    private APIRequest request(final Map<String, String> headers) {

        return new APIRequest.Delegating(null) {

            @Override
            public ListMultimap<String, String> getHeaders() {

                final ListMultimap<String, String> map = ArrayListMultimap.create();
                headers.forEach(map::put);
                return map;
            }

            @Override
            public Caller getCaller() {

                return Caller.ANON;
            }

            @Override
            public ListMultimap<String, String> getQuery() {

                return ArrayListMultimap.create();
            }

            @Override
            public String getRequestId() {

                return "null";
            }

        };
    }

    @Test
    void testNoAuth() throws IOException, ExecutionException, InterruptedException {

        final APIResponse response = api.handle(request(ImmutableMap.of())).get();

        assertEquals(200, response.getStatusCode());
        assertEquals("true", response.getFirstHeader("X-Caller-Anonymous"));
    }

    @Test
    void testAuth() throws IOException, ExecutionException, InterruptedException {

        final APIResponse response = api.handle(request(ImmutableMap.of(
                "authorization", "Basic " + Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8))
        ))).get();

        assertEquals(200, response.getStatusCode());
        assertEquals("false", response.getFirstHeader("X-Caller-Anonymous"));
    }

    @Test
    void testAlternateAuth() throws IOException, ExecutionException, InterruptedException {

        final APIResponse response = api.handle(request(ImmutableMap.of(
                "X-Basic-Authorization", Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8))
        ))).get();

        assertEquals(200, response.getStatusCode());
        assertEquals("false", response.getFirstHeader("X-Caller-Anonymous"));
    }

    @Test
    void testDuplicateAuth() {

        assertThrows(AuthenticationFailedException.class, () -> {
            api.handle(request(ImmutableMap.of(
                    "X-Basic-Authorization", Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8)),
                    "x-basic-authorization", Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8))
            ))).get();
        });

        assertThrows(AuthenticationFailedException.class, () -> {
            api.handle(request(ImmutableMap.of(
                    "Authorization", "Basic " + Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8)),
                    "x-basic-authorization", Base64.getEncoder().encodeToString("matt:password".getBytes(Charsets.UTF_8))
            ))).get();
        });
    }
}
