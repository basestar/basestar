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

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.basestar.BuildMetadata;
import io.basestar.api.API;
import io.basestar.api.APIFormat;
import io.basestar.api.APIRequest;
import io.basestar.api.APIResponse;
import io.basestar.api.exception.InvalidBodyException;
import io.basestar.api.exception.InvalidQueryException;
import io.basestar.api.exception.NotFoundException;
import io.basestar.api.exception.UnsupportedMethodException;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Link;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.exception.ObjectMissingException;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import io.swagger.v3.oas.models.*;
import io.swagger.v3.oas.models.media.*;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

// FIXME: move to new module

public class DatabaseAPI implements API {

    private static final Splitter PATH_SPLITTER = Splitter.on("/").omitEmptyStrings();

    private static final String PARAM_QUERY = "query";

    private static final String PARAM_EXPAND = "expand";

    private static final String PARAM_MODE = "mode";

    private static final String PARAM_COUNT = "count";

    private static final String PARAM_SORT = "sort";

    private static final String PARAM_VERSION = "version";

    private static final String PARAM_PAGING = "paging";

    private static final String PARAM_ID = "id";

    private static final String IN_QUERY = "query";

    private static final String IN_PATH = "path";

    // FIXME: make configurable
    private static final UpdateOptions.Mode DEFAULT_PUT_MODE = UpdateOptions.Mode.CREATE;

    // FIXME: make configurable
    private static final UpdateOptions.Mode DEFAULT_PATCH_MODE = UpdateOptions.Mode.MERGE_DEEP;

    private final Database database;

    public DatabaseAPI(final Database database) {

        this.database = database;
    }

    @Override
    public CompletableFuture<APIResponse> handle(final APIRequest request) {

        try {

            final Caller caller = request.getCaller();

            final List<String> path;
            if(request.getPath().equals("/")) {
                path = Collections.emptyList();
            } else {
                path = PATH_SPLITTER.splitToList(request.getPath());
            }

            final APIRequest.Method method = request.getMethod();
            switch (path.size()) {
                case 0:
                    switch(method) {
                        case HEAD:
                        case OPTIONS:
                            return head(request);
                        case GET:
                            return health(request);
                        default:
                            throw new UnsupportedMethodException(ImmutableSet.of(APIRequest.Method.GET));
                    }
                case 1:
                    switch (method) {
                        case HEAD:
                        case OPTIONS:
                            return head(request);
                        case GET:
                            if (path.get(0).equals("favicon.ico")) {
                                return CompletableFuture.completedFuture(APIResponse.response(request,404, null));
                            } else {
                                return query(caller, path.get(0), request);
                            }
                        case POST:
                            return create(caller, path.get(0), request);
                        default:
                            throw new UnsupportedMethodException(ImmutableSet.of(APIRequest.Method.GET, APIRequest.Method.POST));
                    }
                case 2:
                    switch (method) {
                        case HEAD:
                        case OPTIONS:
                            return head(request);
                        case GET:
                            return read(caller, path.get(0), path.get(1), request);
                        case PUT:
                            return update(caller, path.get(0), path.get(1), DEFAULT_PUT_MODE, request);
                        case PATCH:
                            return update(caller, path.get(0), path.get(1), DEFAULT_PATCH_MODE, request);
                        case DELETE:
                            return delete(caller, path.get(0), path.get(1), request);
                        default:
                            throw new UnsupportedMethodException(ImmutableSet.of(APIRequest.Method.GET, APIRequest.Method.PUT, APIRequest.Method.POST, APIRequest.Method.DELETE));
                    }
                case 3:
                    switch (method) {
                        case HEAD:
                        case OPTIONS:
                            return head(request);
                        case GET:
                            return queryLink(caller, path.get(0), path.get(1), path.get(2), request);
                        default:
                            throw new UnsupportedMethodException(ImmutableSet.of(APIRequest.Method.GET));
                    }
                default:
                    throw new NotFoundException();
            }

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

    private CompletableFuture<APIResponse> create(final Caller caller, final String schema, final APIRequest request) {

        final Map<String, Object> data = parseData(request);
        final String id = Instance.getId(data);

        final CreateOptions options = CreateOptions.builder()
                .schema(schema).id(id).data(data)
                .expand(parseExpand(request))
                .build();

        return respond(request, database.create(caller, options), ignored -> 201);
    }

    private CompletableFuture<APIResponse> read(final Caller caller, final String schema, final String id, final APIRequest request) {

        final ReadOptions options = ReadOptions.builder()
                .schema(schema).id(id)
                .expand(parseExpand(request))
                .version(parseVersion(request))
                .build();

        return respond(request, database.read(caller, options).thenApply(result -> {
            if(result != null) {
                return result;
            } else {
                throw new ObjectMissingException(schema, id);
            }
        }));
    }

    private CompletableFuture<APIResponse> update(final Caller caller, final String schema, final String id, final UpdateOptions.Mode mode, final APIRequest request) {

        final Map<String, Object> data = parseData(request);

        final UpdateOptions options = UpdateOptions.builder()
                .schema(schema).id(id).data(data)
                .expand(parseExpand(request))
                .mode(MoreObjects.firstNonNull(parseUpdateMode(request), mode))
                .version(parseVersion(request))
                .build();

        return respond(request, database.update(caller, options), v -> Long.valueOf(1).equals(Instance.getVersion(v)) ? 201 : 200);
    }

    private CompletableFuture<APIResponse> delete(final Caller caller, final String schema, final String id, final APIRequest request) {

        final DeleteOptions options = DeleteOptions.builder()
                .schema(schema).id(id)
                .version(parseVersion(request))
                .build();

        return respond(request, database.delete(caller, options), ignored -> 204);
    }

    private CompletableFuture<APIResponse> query(final Caller caller, final String schema, final APIRequest request) {

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

        try {
            final String expand = request.getFirstQuery(PARAM_EXPAND);
            if(expand != null) {
                return Path.parseSet(expand);
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_EXPAND, e.getMessage());
        }
    }

    private UpdateOptions.Mode parseUpdateMode(final APIRequest request) {

        try {
            final String mode = request.getFirstQuery(PARAM_MODE);
            if(mode != null) {
                return UpdateOptions.Mode.valueOf(mode.toUpperCase());
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_MODE, e.getMessage());
        }
    }

    private Integer parseCount(final APIRequest request) {

        try {
            final String count = request.getFirstQuery(PARAM_COUNT);
            if(count != null) {
                return Integer.parseInt(count);
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_COUNT, e.getMessage());
        }
    }

    private List<Sort> parseSort(final APIRequest request) {

        try {
            final String expand = request.getFirstQuery(PARAM_SORT);
            if(expand != null) {
                return Splitter.on(",").omitEmptyStrings().trimResults().splitToList(expand).stream()
                        .map(Sort::parse).collect(Collectors.toList());
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_SORT, e.getMessage());
        }
    }

    private Long parseVersion(final APIRequest request) {

        try {
            final String version = request.getFirstQuery(PARAM_VERSION);
            if(version != null) {
                return Long.parseLong(version);
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_VERSION, e.getMessage());
        }
    }

    private PagingToken parsePaging(final APIRequest request) {

        try {
            final String paging = request.getFirstQuery(PARAM_PAGING);
            if(paging != null) {
                return new PagingToken(paging);
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_PAGING, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseData(final APIRequest request) {

        try(final InputStream is = request.readBody()) {
            return (Map<String, Object>) request.getContentType().getMapper().readValue(is, Map.class);
        } catch (final IOException e) {
            throw new InvalidBodyException(e.getMessage());
        }
    }

    private Expression parseQuery(final APIRequest request) {

        try {
            final String query = request.getFirstQuery(PARAM_QUERY);
            if (query != null) {
                return Expression.parse(query);
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new InvalidQueryException(PARAM_QUERY, e.getMessage());
        }
    }

    private CompletableFuture<APIResponse> respond(final APIRequest request, final CompletableFuture<?> future) {

        return respond(request, future, ignored -> 200);
    }

    private <T> CompletableFuture<APIResponse> respond(final APIRequest request, final CompletableFuture<T> future, final Function<T, Integer> status) {

        return future.thenApply(v -> APIResponse.response(request, status.apply(v), v))
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
            query.removeAll(PARAM_PAGING);
            query.put(PARAM_PAGING, paged.getPaging().toString());
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

    @Override
    public CompletableFuture<OpenAPI> openApi() {

        final Namespace namespace = database.namespace();

        final Paths paths = new Paths();
        final Components components = new Components();
        namespace.getSchemas().forEach((name, schema) -> {
            components.addSchemas(name, schema.openApi());
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                components.addRequestBodies(name, openApiRequestBody(openApiRef(objectSchema)));
                components.addResponses(name, openApiResponse(openApiRef(objectSchema)));
                components.addResponses(name + "Page", openApiResponse(new ArraySchema().items(openApiRef(objectSchema))));
                paths.putAll(openApiPaths(objectSchema));
            }
        });

        return CompletableFuture.completedFuture(new OpenAPI()
                .paths(paths)
                .components(components));
    }

    private Map<String, PathItem> openApiPaths(final ObjectSchema schema) {

        final Map<String, PathItem> paths = new HashMap<>();
        paths.put("/" + schema.getName(), new PathItem()
                .get(openApiQuery(schema))
                .post(openApiCreate(schema)));
        paths.put("/" + schema.getName() + "/{" + PARAM_ID + "}", new PathItem()
                .get(openApiGet(schema))
                .put(openApiUpdate(schema))
                .patch(openApiPatch(schema))
                .delete(openApiDelete(schema)));
        schema.getLinks().forEach((name, link) -> {
            paths.put("/" + schema.getName() + "/{" + PARAM_ID + "}/" + name, new PathItem()
                    .get(openApiLinkQuery(schema, link)));
        });
        return paths;
    }

    private Operation openApiGet(final ObjectSchema schema) {

        return new Operation()
                .operationId("get" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_ID).in(IN_PATH).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName())))
                .addTagsItem(schema.getName());
    }

    private Operation openApiCreate(final ObjectSchema schema) {

        return new Operation()
                .operationId("create" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_VERSION).in(IN_QUERY).schema(new StringSchema()))
                .requestBody(new RequestBody().$ref(schema.getName()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName())))
                .addTagsItem(schema.getName());
    }

    private Operation openApiUpdate(final ObjectSchema schema) {

        return new Operation()
                .operationId("update" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_ID).in(IN_PATH).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_VERSION).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_MODE).in(IN_QUERY).schema(new StringSchema()))
                .requestBody(new RequestBody().$ref(schema.getName()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName())))
                .addTagsItem(schema.getName());
    }

    private Operation openApiPatch(final ObjectSchema schema) {

        return new Operation()
                .operationId("patch" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_ID).in(IN_PATH).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_VERSION).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_MODE).in(IN_QUERY).schema(new StringSchema()))
                .requestBody(new RequestBody().$ref(schema.getName()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName())))
                .addTagsItem(schema.getName());
    }

    private Operation openApiDelete(final ObjectSchema schema) {

        return new Operation()
                .operationId("delete" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_ID).in(IN_PATH).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_VERSION).in(IN_QUERY).schema(new StringSchema()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName())))
                .addTagsItem(schema.getName());
    }

    private Operation openApiQuery(final ObjectSchema schema) {

        return new Operation()
                .operationId("query" + schema.getName())
                .addParametersItem(new Parameter().name(PARAM_QUERY).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_COUNT).in(IN_QUERY).schema(new IntegerSchema()))
                .addParametersItem(new Parameter().name(PARAM_PAGING).in(IN_QUERY).schema(new StringSchema()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName() + "Page")))
                .addTagsItem(schema.getName());
    }

    private Operation openApiLinkQuery(final ObjectSchema schema, final Link link) {

        return new Operation()
                .operationId("queryLink" + schema.getName() + link.getName())
                .addParametersItem(new Parameter().name(PARAM_ID).in(IN_PATH).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_QUERY).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_EXPAND).in(IN_QUERY).schema(new StringSchema()))
                .addParametersItem(new Parameter().name(PARAM_COUNT).in(IN_QUERY).schema(new IntegerSchema()))
                .addParametersItem(new Parameter().name(PARAM_PAGING).in(IN_QUERY).schema(new StringSchema()))
                .responses(openApiResponses(new ApiResponse().$ref(schema.getName() + "Page")))
                .addTagsItem(schema.getName());
    }

    private Schema<?> openApiRef(final ObjectSchema schema) {

        return new io.swagger.v3.oas.models.media.ObjectSchema().$ref(schema.getName());
    }

    private Content openApiContent(final io.swagger.v3.oas.models.media.Schema<?> schema) {

        final Content content = new Content();
        for(final APIFormat format : APIFormat.values()) {
            content.addMediaType(format.getContentType(), new MediaType().schema(schema));
        }
        return content;
    }

    private RequestBody openApiRequestBody(final io.swagger.v3.oas.models.media.Schema<?> schema) {

        return new RequestBody().content(openApiContent(schema));
    }

    private ApiResponse openApiResponse(final io.swagger.v3.oas.models.media.Schema<?> schema) {

        return new ApiResponse().content(openApiContent(schema));
    }

    private ApiResponses openApiResponses(final ApiResponse response) {

        return new ApiResponses()
                .addApiResponse("200", response);
    }
}
