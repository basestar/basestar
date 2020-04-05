package io.basestar.database.api;

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.BuildMetadata;
import io.basestar.api.API;
import io.basestar.api.APIFormat;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.exception.ExceptionMetadata;
import io.basestar.exception.HasExceptionMetadata;
import io.basestar.expression.Expression;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class DatabaseAPI implements API {

    private static final Splitter PATH_SPLITTER = Splitter.on("/").omitEmptyStrings();

    private final Authenticator authenticator;

    private final Database database;

    public DatabaseAPI(final Authenticator authenticator, final Database database) {

        this.authenticator = authenticator;
        this.database = database;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        try {

            final String authorization = request.getFirstHeader("Authorization");
            final Caller caller = authenticator.authenticate(authorization);

            final List<String> path;
            if(request.getPath().equals("/")) {
                path = Collections.emptyList();
            } else {
                path = PATH_SPLITTER.splitToList(request.getPath());
            }

            switch (request.getMethod()) {
                case HEAD:
                case OPTIONS:
                    return head(request);
                case GET:
                    switch (path.size()) {
                        case 0:
                            return health(request);
                        case 1:
                            if (path.get(0).equals("favicon.ico")) {
                                return CompletableFuture.completedFuture(response(request,404, null));
                            } else {
                                return query(caller, path.get(0), request);
                                //return type(caller, request);
                            }
                        case 2:
                            return read(caller, path.get(0), path.get(1), request);
                        case 3:
                            return page(caller, path.get(0), path.get(1), path.get(2), request);
                    }
                    break;
                case PUT:
                    switch (path.size()) {
                        case 2:
                            return create(caller, path.get(0), path.get(1), request);
                    }
                    break;
                case PATCH:
                    switch (path.size()) {
                        case 2:
                            return update(caller, path.get(0), path.get(1), request);
                    }
                    break;
                case DELETE:
                    switch (path.size()) {
                        case 2:
                            return delete(caller, path.get(0), path.get(1), request);
                    }
                    break;
                case POST:
                    switch (path.size()) {
                        case 1:
                            return create(caller, path.get(0), request);
                    }
                    break;
            }
            throw new IllegalStateException();

        } catch (final Exception e) {

            return CompletableFuture.completedFuture(error(request, e));
        }
    }

    private CompletableFuture<APIResponse> health(final APIRequest request) {

        return CompletableFuture.completedFuture(success(request, ImmutableMap.of(
                "basestar", ImmutableMap.of(
                        "version", BuildMetadata.VERSION,
                        "buildTimestamp", BuildMetadata.TIMESTAMP
                )
        )));
    }

    private CompletableFuture<APIResponse> head(final APIRequest request) {

        return CompletableFuture.completedFuture(success(request,null));
    }

    private CompletableFuture<APIResponse> create(final Caller caller, final String schema, final APIRequest request) throws IOException {

        final Map<String, Object> data = parseData(request);

        final CreateOptions options = new CreateOptions()
                .setExpand(parseExpand(request));

        return respond(request, database.create(caller, schema, data, options));
    }

    private CompletableFuture<APIResponse> create(final Caller caller, final String schema, final String id, final APIRequest request) throws IOException {

        final Map<String, Object> data = parseData(request);

        final CreateOptions options = new CreateOptions()
                .setExpand(parseExpand(request));

        return respond(request, database.create(caller, schema, id, data, options));
    }

    private CompletableFuture<APIResponse> read(final Caller caller, final String schema, final String id, final APIRequest request) {

        final ReadOptions options = new ReadOptions()
                .setExpand(parseExpand(request))
                .setVersion(parseVersion(request));

        return respond(request, database.read(caller, schema, id, options));
    }

    private CompletableFuture<APIResponse> update(final Caller caller, final String schema, final String id, final APIRequest request) throws IOException {

        final Map<String, Object> data = parseData(request);

        final UpdateOptions options = new UpdateOptions()
                .setExpand(parseExpand(request))
                .setMode(parseUpdateMode(request))
                .setVersion(parseVersion(request));

        return respond(request, database.update(caller, schema, id, data, options));
    }

    private CompletableFuture<APIResponse> delete(final Caller caller, final String schema, final String id, final APIRequest request) {

        final DeleteOptions options = new DeleteOptions()
                .setVersion(parseVersion(request));

        return respond(request, database.delete(caller, schema, id, options));
    }

    private CompletableFuture<APIResponse> query(final Caller caller, final String schema, final APIRequest request) throws IOException {

        final QueryOptions options = new QueryOptions()
                .setCount(parseCount(request))
                .setExpand(parseExpand(request))
                .setSort(parseSort(request))
                .setPaging(parsePaging(request));

        final Expression query = parseQuery(request);

        return respond(request, database.query(caller, schema, query, options));
    }

    private CompletableFuture<APIResponse> page(final Caller caller, final String schema, final String id, final String rel, final APIRequest request) {

        final QueryOptions options = new QueryOptions()
                .setCount(parseCount(request))
                .setExpand(parseExpand(request))
                .setPaging(parsePaging(request));

        return respond(request, database.page(caller, schema, id, rel, options));
    }

    private Set<Path> parseExpand(final APIRequest request) {

        final String expand = request.getFirstQuery("expand");
        if(expand != null) {
            return Path.parseSet(expand);
        } else {
            return null;
        }
    }

    private UpdateOptions.Mode parseUpdateMode(final APIRequest request) {

        final String mode = request.getFirstQuery("mode");
        if(mode != null) {
            return UpdateOptions.Mode.valueOf(mode.toUpperCase());
        } else {
            return null;
        }
    }

    private Integer parseCount(final APIRequest request) {

        final String count = request.getFirstQuery("count");
        if(count != null) {
            return Integer.parseInt(count);
        } else {
            return null;
        }
    }

    private List<Sort> parseSort(final APIRequest request) {

        final String expand = request.getFirstQuery("sort");
        if(expand != null) {
            return Splitter.on(",").omitEmptyStrings().trimResults().splitToList(expand).stream()
                    .map(Sort::parse).collect(Collectors.toList());
        } else {
            return null;
        }
    }

    private Long parseVersion(final APIRequest request) {

        final String version = request.getFirstQuery("version");
        if(version != null) {
            return Long.parseLong(version);
        } else {
            return null;
        }
    }

    private PagingToken parsePaging(final APIRequest request) {

        final String paging = request.getFirstQuery("paging");
        if(paging != null) {
            return new PagingToken(paging);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseData(final APIRequest request) throws IOException {

        try(final InputStream is = request.readBody()) {
            return (Map<String, Object>) request.getContentType().getMapper().readValue(is, Map.class);
        }
    }

    private Expression parseQuery(final APIRequest request) {

        final String query = request.getFirstQuery("query");
        if(query != null) {
            return Expression.parse(query);
        } else {
            return null;
        }
    }

    private CompletableFuture<APIResponse> respond(final APIRequest request, final CompletableFuture<?> future) {

        return future.thenApply(v -> success(request, v))
                .exceptionally(v -> error(request, v));
    }

    private APIResponse success(final APIRequest request, final Object v) {

        return response(request, v == null ? 204 : 200, v);
    }

    private APIResponse error(final APIRequest request, final Throwable e) {

        e.printStackTrace(System.err);
        final ExceptionMetadata metadata = exceptionMetadata(e);
        return response(request, metadata.getStatus(), metadata);
    }

    private ExceptionMetadata exceptionMetadata(final Throwable e) {

        if(e instanceof HasExceptionMetadata) {
            return ((HasExceptionMetadata)e).getMetadata();
        } else if(e.getCause() != null) {
            return exceptionMetadata(e.getCause());
        } else {
            return new ExceptionMetadata().setStatus(500).setCode("UnknownError").setMessage(e.getMessage());
        }
    }

    private APIResponse response(final APIRequest request, final int status, final Object v) {

        final APIFormat format = request.getAccept();
        final Multimap<String, String> headers = HashMultimap.create();
        headers.put("Content-Type", format.getContentType());
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "*");
        headers.put("Access-Control-Allow-Headers", "*");
        headers.put("Access-Control-Expose-Headers", "Link");
        if(v instanceof PagedList<?>) {
            final PagedList<?> paged = (PagedList<?>)v;
            if(paged.hasPaging()) {
                final String path = request.getPath();
                final HashMultimap<String, String> query = HashMultimap.create(request.getQuery());
                query.removeAll("paging");
                query.put("paging", paged.getPaging().toString());
                final String url = path + "?" + query.entries().stream()
                        .map(e -> {
                            try {
                                return e.getKey() + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8.toString());
                            } catch (final IOException x) {
                                throw new IllegalStateException(x);
                            }
                        }).collect(Collectors.joining("&"));
                headers.put("Link", "<" + url + ">; rel=\"next\"");
            }
        }

        return new APIResponse() {
            @Override
            public int getStatusCode() {

                return status;
            }

            @Override
            public Multimap<String, String> getHeaders() {

                return headers;
            }

            @Override
            public void writeTo(final OutputStream out) throws IOException {

                if(v != null) {
                    format.getMapper().writerWithDefaultPrettyPrinter().writeValue(out, v);
                }
            }
        };
    }
}
