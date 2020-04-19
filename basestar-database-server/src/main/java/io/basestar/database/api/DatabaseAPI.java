package io.basestar.database.api;

/*-
 * #%L
 * basestar-database-server
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.basestar.BuildMetadata;
import io.basestar.api.API;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.auth.Authenticator;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

// FIXME: move to new module

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
                                return CompletableFuture.completedFuture(APIResponse.response(request,404, null));
                            } else {
                                return query(caller, path.get(0), request);
                                //return type(caller, request);
                            }
                        case 2:
                            return read(caller, path.get(0), path.get(1), request);
                        case 3:
                            return queryLink(caller, path.get(0), path.get(1), path.get(2), request);
                    }
                    break;
                case PUT:
                    switch (path.size()) {
                        case 1:
                            return create(caller, path.get(0), request);
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

            return CompletableFuture.completedFuture(APIResponse.error(request, e));
        }
    }

    private CompletableFuture<APIResponse> health(final APIRequest request) {

        return CompletableFuture.completedFuture(APIResponse.success(request, ImmutableMap.of(
                "basestar", ImmutableMap.of(
                        "version", BuildMetadata.VERSION,
                        "buildTimestamp", BuildMetadata.TIMESTAMP
                )
        )));
    }

    private CompletableFuture<APIResponse> head(final APIRequest request) {

        return CompletableFuture.completedFuture(APIResponse.success(request,null));
    }

    private CompletableFuture<APIResponse> create(final Caller caller, final String schema, final APIRequest request) throws IOException {

        return create(caller, schema, null, request);
    }

    private CompletableFuture<APIResponse> create(final Caller caller, final String schema, final String id, final APIRequest request) throws IOException {

        final Map<String, Object> data = parseData(request);

        final CreateOptions options = CreateOptions.builder()
                .schema(schema).id(id).data(data)
                .expand(parseExpand(request))
                .build();

        return respond(request, database.create(caller, options));
    }

    private CompletableFuture<APIResponse> read(final Caller caller, final String schema, final String id, final APIRequest request) {

        final ReadOptions options = ReadOptions.builder()
                .schema(schema).id(id)
                .expand(parseExpand(request))
                .version(parseVersion(request))
                .build();

        return respond(request, database.read(caller, options));
    }

    private CompletableFuture<APIResponse> update(final Caller caller, final String schema, final String id, final APIRequest request) throws IOException {

        final Map<String, Object> data = parseData(request);

        final UpdateOptions options = UpdateOptions.builder()
                .schema(schema).id(id).data(data)
                .expand(parseExpand(request))
                .mode(parseUpdateMode(request))
                .version(parseVersion(request))
                .build();

        return respond(request, database.update(caller, options));
    }

    private CompletableFuture<APIResponse> delete(final Caller caller, final String schema, final String id, final APIRequest request) {

        final DeleteOptions options = DeleteOptions.builder()
                .schema(schema).id(id)
                .version(parseVersion(request))
                .build();

        return respond(request, database.delete(caller, options));
    }

    private CompletableFuture<APIResponse> query(final Caller caller, final String schema, final APIRequest request) throws IOException {

        final Expression query = parseQuery(request);

        final QueryOptions options = QueryOptions.builder()
                .schema(schema).expression(query)
                .count(parseCount(request))
                .expand(parseExpand(request))
                .sort(parseSort(request))
                .paging(parsePaging(request))
                .build();

        return respondPaged(request, database.query(caller, options));
    }

    private CompletableFuture<APIResponse> queryLink(final Caller caller, final String schema, final String id, final String link, final APIRequest request) {

        final QueryLinkOptions options = QueryLinkOptions.builder()
                .schema(schema).id(id).link(link)
                .count(parseCount(request))
                .expand(parseExpand(request))
                .paging(parsePaging(request))
                .build();

        return respondPaged(request, database.queryLink(caller, options));
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

        return future.thenApply(v -> APIResponse.success(request, v))
                .exceptionally(v -> APIResponse.error(request, v));
    }


    private CompletableFuture<APIResponse> respondPaged(final APIRequest request, final CompletableFuture<? extends PagedList<?>> future) {

        return future.thenApply(v -> APIResponse.success(request, linkHeaders(request, v), v))
                .exceptionally(v -> APIResponse.error(request, v));
    }

    private Multimap<String, String> linkHeaders(final APIRequest request, final PagedList<?> paged) {

        if(paged.hasPaging()) {
            final Multimap<String, String> headers = HashMultimap.create();
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
            return headers;
        } else {
            return null;
        }
    }
}
