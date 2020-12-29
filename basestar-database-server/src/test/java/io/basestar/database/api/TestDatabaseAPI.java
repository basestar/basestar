package io.basestar.database.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Namespace;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDatabaseAPI {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    @Test
    void testQuery() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.GET, "Test1", ImmutableMap.of("query", "field1=='somevalue'", "sort", "x,y"))).join();
        Mockito.verify(db).query(Caller.ANON, QueryOptions.builder()
                .schema(Name.of("Test1")).expression(Expression.parse("field1 == 'somevalue'")).sort(Sort.parseList("x,y")).build());
    }

    @Test
    void testQueryLink() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.GET, "Test1/1/link", ImmutableMap.of("count", "10"))).join();
        Mockito.verify(db).queryLink(Caller.ANON, QueryLinkOptions.builder()
                .schema(Name.of("Test1")).id("1").link("link").count(10).build());
    }

    @Test
    void testRead() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.GET, "Test1/1", ImmutableMap.of("expand", "a,b", "version", "2"))).join();
        Mockito.verify(db).read(Caller.ANON, ReadOptions.builder()
                .schema(Name.of("Test1")).id("1").expand(Name.parseSet("a,b")).version(2L).build());
    }

    @Test
    void testRead404() {

        final Database db = Mockito.mock(Database.class);
        Mockito.when(db.read(Mockito.any(Caller.class), Mockito.any(ReadOptions.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        final DatabaseAPI api = new DatabaseAPI(db);
        final APIResponse response = api.handle(request(APIRequest.Method.GET, "Test1/1")).join();
        assertEquals(404, response.getStatusCode());
    }

    @Test
    void testDelete() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.DELETE, "Test1/1")).join();
        Mockito.verify(db).delete(Caller.ANON, DeleteOptions.builder()
                .schema(Name.of("Test1")).id("1").build());
    }

    @Test
    void testCreate() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.POST, "Test1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).create(Caller.ANON, CreateOptions.builder()
                .schema(Name.of("Test1")).data(ImmutableMap.of("f1", "a", "f2", "b")).build());
    }

    @Test
    void testUpdateCreate() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.PUT, "Test1/1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).update(Caller.ANON, UpdateOptions.builder()
                .schema(Name.of("Test1")).id("1").data(ImmutableMap.of("f1", "a", "f2", "b")).mode(UpdateOptions.Mode.CREATE).build());
    }

    @Test
    void testUpdateMergeDeep() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.PATCH, "Test1/1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).update(Caller.ANON, UpdateOptions.builder()
                .schema(Name.of("Test1")).id("1").data(ImmutableMap.of("f1", "a", "f2", "b")).mode(UpdateOptions.Mode.MERGE_DEEP).build());
    }

    @Test
    void test404() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        assertEquals(404, api.handle(request(APIRequest.Method.GET, "favicon.ico")).join().getStatusCode());
        assertEquals(404, api.handle(request(APIRequest.Method.GET, "/some/long/missing/path")).join().getStatusCode());
    }

    @Test
    void test405() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        assertEquals(405, api.handle(request(APIRequest.Method.POST, "/")).join().getStatusCode());
        assertEquals(405, api.handle(request(APIRequest.Method.PUT, "/Test1")).join().getStatusCode());
        assertEquals(405, api.handle(request(APIRequest.Method.POST, "/Test1/1")).join().getStatusCode());
        assertEquals(405, api.handle(request(APIRequest.Method.POST, "/Test1/1/link")).join().getStatusCode());
    }

    @Test
    void testHead() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        assertEquals(204, api.handle(request(APIRequest.Method.HEAD, "/")).join().getStatusCode());
        assertEquals(204, api.handle(request(APIRequest.Method.HEAD, "/Test1")).join().getStatusCode());
        assertEquals(204, api.handle(request(APIRequest.Method.HEAD, "/Test1/x")).join().getStatusCode());
        assertEquals(204, api.handle(request(APIRequest.Method.HEAD, "/Test1/x/link1")).join().getStatusCode());
    }

    @Test
    void testHealth() throws IOException {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            final APIResponse response = api.handle(request(APIRequest.Method.GET, "/")).join();
            assertEquals(200, response.getStatusCode());
            response.writeTo(baos);
            final Map<?, ?> health = objectMapper.readValue(baos.toString("UTF-8"), Map.class);
            assertTrue(health.containsKey("basestar"));
        }
    }

    @Test
    void testOpenAPI() throws IOException {

        final Namespace namespace = Namespace.load(TestDatabaseAPI.class.getResource("/schema/Petstore.yml"));
        final Database db = Mockito.mock(Database.class);
        Mockito.when(db.namespace()).thenReturn(namespace);
        final DatabaseAPI api = new DatabaseAPI(db);
        final OpenAPI openAPI = api.openApi().join();
        // Support future additions to the (shared) petstore schema
        assertTrue(openAPI.getPaths().size() >= 19);
        assertTrue(openAPI.getComponents().getSchemas().size() >= 11);
        assertTrue(openAPI.getComponents().getResponses().size() >= 12);
        assertTrue(openAPI.getComponents().getRequestBodies().size() >= 6);
    }

    @Data
    private static class Properties {

        private final String f1;

        private final String f2;
    }

    private APIRequest request(final APIRequest.Method method, final String path) {

        return request(method, path, ImmutableMap.of());
    }

    private APIRequest request(final APIRequest.Method method, final String path, final Map<String, String> query) {

        return request(method, path, query, null);
    }

    private APIRequest request(final APIRequest.Method method, final String path, final Map<String, String> query, final Object body) {

        return new APIRequest() {
            @Override
            public Caller getCaller() {

                return Caller.ANON;
            }

            @Override
            public Method getMethod() {

                return method;
            }

            @Override
            public String getPath() {

                return path;
            }

            @Override
            public Multimap<String, String> getQuery() {

                final Multimap<String, String> result = HashMultimap.create();
                query.forEach(result::put);
                return result;
            }

            @Override
            public Multimap<String, String> getHeaders() {

                return HashMultimap.create();
            }

            @Override
            public InputStream readBody() throws IOException {

                if(body != null) {
                    final String str = objectMapper.writeValueAsString(body);
                    return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
                } else {
                    return null;
                }
            }

            @Override
            public String getRequestId() {

                return UUID.randomUUID().toString();
            }
        };
    }
}
