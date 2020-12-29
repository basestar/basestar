package io.basestar.database.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.api.APIRequest;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.jackson.BasestarModule;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Data;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

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
