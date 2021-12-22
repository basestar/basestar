package io.basestar.database.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import io.swagger.v3.oas.models.OpenAPI;
import lombok.Data;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.skyscreamer.jsonassert.JSONAssert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestDatabaseAPI {

    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    @Test
    void testQuery() throws Exception {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);

        final QueryOptions queryOpts = QueryOptions.builder()
                .setSchema(Name.of("Test1"))
                .setExpression(Expression.parse("field1 == 'somevalue'"))
                .setSort(asList(Sort.asc(Name.of("x")), Sort.asc(Name.of("y"))))
                .setStats(EnumSet.of(Page.Stat.TOTAL))
                .build();

        final Instance instance = new Instance(ImmutableMap.<String, Object>builder()
                .put("testKey", "testValue")
                .build());

        // mock returned page to test response values
        final Page<Instance> page = new Page<>(
                singletonList(instance),
                new Page.Token("AAAAAAAAAAA"),
                Page.Stats.fromTotal(123)
        );
        Mockito.when(db.query(Caller.ANON, queryOpts)).thenReturn(CompletableFuture.completedFuture(page));

        final APIResponse result = api.handle(request(
                APIRequest.Method.GET,
                "Test1",
                ImmutableMap.of(
                        "query", "field1=='somevalue'",
                        "sort", "x,y",
                        "stats", "total")
        )).join();
        Mockito.verify(db).query(Caller.ANON, queryOpts);

        assertEquals(
                singleton("<Test1?paging=AAAAAAAAAAA&sort=x%2Cy&stats=total&query=field1%3D%3D%27somevalue%27>; rel=\"next\""),
                result.getHeaders().get("Link"));

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            result.writeTo(baos);
            final String outputEntity = baos.toString("UTF-8");

            final String expected = loadExpectedFile("query.expected.json");
            JSONAssert.assertEquals(expected, outputEntity, true);
        }
    }

    @Test
    void testQueryLink() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(
                APIRequest.Method.GET,
                "Test1/1/link",
                ImmutableMap.of("count", "10", "stats", "total")
        )).join();
        Mockito.verify(db).queryLink(Caller.ANON, QueryLinkOptions.builder()
                .setSchema(Name.of("Test1"))
                .setId("1")
                .setLink("link")
                .setCount(10)
                .setStats(EnumSet.of(Page.Stat.TOTAL))
                .build());
    }

    @Test
    void testRead() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.GET, "Test1/1", ImmutableMap.of("expand", "a,b", "version", "2"))).join();
        Mockito.verify(db).read(Caller.ANON, ReadOptions.builder()
                .setSchema(Name.of("Test1")).setId("1").setExpand(Name.parseSet("a,b")).setVersion(2L).build());
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
                .setSchema(Name.of("Test1")).setId("1").build());
    }

    @Test
    void testCreate() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.POST, "Test1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).create(Caller.ANON, CreateOptions.builder()
                .setSchema(Name.of("Test1")).setData(ImmutableMap.of("f1", "a", "f2", "b")).build());
    }

    @Test
    void testUpdateCreate() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.PUT, "Test1/1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).update(Caller.ANON, UpdateOptions.builder()
                .setSchema(Name.of("Test1")).setId("1").setData(ImmutableMap.of("f1", "a", "f2", "b"))
                .setMode(UpdateOptions.Mode.REPLACE).setCreate(true).build());
    }

    @Test
    void testUpdateMergeDeep() {

        final Database db = Mockito.mock(Database.class);
        final DatabaseAPI api = new DatabaseAPI(db);
        api.handle(request(APIRequest.Method.PATCH, "Test1/1", ImmutableMap.of(), new Properties("a", "b"))).join();
        Mockito.verify(db).update(Caller.ANON, UpdateOptions.builder()
                .setSchema(Name.of("Test1")).setId("1").setData(ImmutableMap.of("f1", "a", "f2", "b"))
                .setMode(UpdateOptions.Mode.MERGE_DEEP).setCreate(false).build());
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
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
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
            public ListMultimap<String, String> getQuery() {

                final ListMultimap<String, String> result = ArrayListMultimap.create();
                query.forEach(result::put);
                return result;
            }

            @Override
            public ListMultimap<String, String> getHeaders() {

                return ArrayListMultimap.create();
            }

            @Override
            public InputStream readBody() throws IOException {

                if (body != null) {
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

    private String loadExpectedFile(final String expectedFile) throws IOException {
        final URL expectedUrl = Objects.requireNonNull(this.getClass().getResource(expectedFile));
        return IOUtils.toString(expectedUrl, StandardCharsets.UTF_8);
    }
}
