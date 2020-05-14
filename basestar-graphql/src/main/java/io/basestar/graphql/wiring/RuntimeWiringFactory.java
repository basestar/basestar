package io.basestar.graphql.wiring;

/*-
 * #%L
 * basestar-graphql
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

import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.graphql.GraphQLNamingStrategy;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.*;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import io.basestar.util.Sort;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


public class RuntimeWiringFactory {

    private final Database database;

    private final Namespace namespace;

    private final GraphQLNamingStrategy namingStrategy;

    public RuntimeWiringFactory(final Database database, final Namespace namespace, final GraphQLNamingStrategy namingStrategy) {

        this.database = database;
        this.namespace = namespace;
        this.namingStrategy = namingStrategy;
    }

    public RuntimeWiring runtimeWiring() {

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
        builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLUtils.QUERY_TYPE)
                .dataFetchers(queryFetchers()));
        builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLUtils.MUTATION_TYPE)
                .dataFetchers(mutationFetchers()));
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof InstanceSchema) {
                if(!((InstanceSchema) schema).isConcrete()) {
                    builder.type(TypeRuntimeWiring.newTypeWiring(schema.getName())
                            .typeResolver(InterfaceResolver.INSTANCE));
                }
            }
        });
        return builder.build();
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> queryFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.forEachObjectSchema((schemaName, schema) -> {
            results.put(namingStrategy.readMethodName(schema), getFetcher(schema));
            results.put(namingStrategy.queryMethodName(schema), queryFetcher(schema));
            schema.getLinks().forEach((linkName, link) -> {
                results.put(namingStrategy.queryLinkMethodName(schema, link), queryLinkFetcher(schema, link));
            });
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> getFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final Long version = env.getArgumentOrDefault(Reserved.VERSION, null);
            final ReadOptions options = ReadOptions.builder()
                    .schema(schema.getName()).id(id)
                    .version(version).expand(expand)
                    .build();
            return database.read(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> queryFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Path> paths = Path.children(paths(env), namingStrategy.pageItemsFieldName());
            final Set<Path> expand = schema.requiredExpand(paths);
            final String query = env.getArgument(namingStrategy.queryArgumentName());
            final Expression expression = query == null ? Constant.TRUE : Expression.parse(query);
            final String paging = env.getArgument(namingStrategy.pagingArgumentName());
            final Number count = env.getArgument(namingStrategy.countArgumentName());
            final List<?> sort = env.getArgument(namingStrategy.sortArgumentName());
            final QueryOptions options = QueryOptions.builder()
                    .schema(schema.getName())
                    .expression(expression)
                    .paging(paging(paging))
                    .count(count(count))
                    .sort(sort(sort))
                    .expand(expand)
                    .build();
            return database.query(caller, options)
                    .thenApply(objects -> objects.map(object -> GraphQLUtils.toResponse(schema, object)))
                    .thenApply(this::toPage);
        };
    }

    private DataFetcher<CompletableFuture<?>> queryLinkFetcher(final ObjectSchema schema, final Link link) {

        return (env) -> {

            final Caller caller = GraphQLUtils.caller(env.getContext());
            final ObjectSchema linkSchema = link.getSchema();
            final Set<Path> paths = Path.children(paths(env), namingStrategy.pageItemsFieldName());
            final Set<Path> expand = linkSchema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final String paging = env.getArgument(namingStrategy.pagingArgumentName());
            final Number count = env.getArgument(namingStrategy.countArgumentName());
            final QueryLinkOptions options = QueryLinkOptions.builder()
                    .schema(schema.getName())
                    .link(link.getName())
                    .id(id)
                    .expand(expand)
                    .paging(paging(paging))
                    .count(count(count))
                    .build();
            return database.queryLink(caller, options)
                    .thenApply(objects -> objects.map(object -> GraphQLUtils.toResponse(linkSchema, object)))
                    .thenApply(this::toPage);
        };
    }

    private Map<String, Object> toPage(final PagedList<?> page) {

        final Map<String, Object> result = new HashMap<>();
        result.put(namingStrategy.pageItemsFieldName(), page.getPage());
        if(page.hasPaging()) {
            result.put(namingStrategy.pagePagingFieldName(), page.getPaging().toString());
        }
        return result;
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> mutationFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                results.put(namingStrategy.createMethodName(objectSchema), createFetcher(objectSchema));
                results.put(namingStrategy.updateMethodName(objectSchema), updateFetcher(objectSchema));
                results.put(namingStrategy.deleteMethodName(objectSchema), deleteFetcher(objectSchema));

            }
        });
        return results;
    }

    //    private Fetcher<ActionOptions> create(final ObjectSchema schema) {
//
//        return (context, field) -> {
//
//            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
//            final Map<String, Object> data = GraphQLUtils.argInput(context, schema, field, namingStrategy.dataArgumentName());
//            final Map<String, Expression> expressions = GraphQLUtils.argInputExpr(context, schema, field, namingStrategy.expressionsArgumentName());
//            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());
//
//            final CreateOptions.Builder builder = CreateOptions.builder();
//            builder.schema(schema.getName());
//            builder.id(id);
//            builder.data(data);
//            builder.expressions(expressions);
//            builder.expand(schema.requiredExpand(paths));
//            return builder.build();
//        };
//    }
//
//    private Fetcher<ActionOptions> update(final ObjectSchema schema) {
//
//        return (context, field) -> {
//
//            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
//            final Long version = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, Reserved.VERSION);
//            final Map<String, Object> data = GraphQLUtils.argInput(context, schema, field, namingStrategy.dataArgumentName());
//            final Map<String, Expression> expressions = GraphQLUtils.argInputExpr(context, schema, field, namingStrategy.expressionsArgumentName());
//            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());
//
//            assert id != null;
//
//            final UpdateOptions.Builder builder = UpdateOptions.builder();
//            builder.schema(schema.getName());
//            builder.id(id);
//            builder.version(version);
//            builder.data(data);
//            builder.expressions(expressions);
//            builder.expand(schema.requiredExpand(paths));
//            return builder.build();
//        };
//    }
//
//    private Fetcher<ActionOptions> delete(final ObjectSchema schema) {
//
//        return (context, field) -> {
//
//            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
//            final Long version = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, Reserved.VERSION);
//            assert id != null;
//
//            final DeleteOptions.Builder builder = DeleteOptions.builder();
//            builder.schema(schema.getName());
//            builder.id(id);
//            builder.version(version);
//            return builder.build();
//        };
//    }

    private DataFetcher<CompletableFuture<?>> createFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgumentOrDefault(Reserved.ID, null);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument(namingStrategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(namingStrategy.expressionsArgumentName()));
            final CreateOptions options = CreateOptions.builder()
                    .schema(schema.getName()).id(id)
                    .data(data).expand(expand)
                    .expressions(expressions)
                    .build();
            return database.create(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> updateFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final Long version = env.getArgumentOrDefault(Reserved.VERSION, null);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument(namingStrategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(namingStrategy.expressionsArgumentName()));
            final UpdateOptions options = UpdateOptions.builder()
                    .schema(schema.getName()).id(id)
                    .data(data).version(version)
                    .expressions(expressions)
                    .expand(expand)
                    .build();
            return database.update(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> deleteFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final String id = env.getArgument(Reserved.ID);
            final Long version = env.getArgumentOrDefault(Reserved.VERSION, null);
            final DeleteOptions options = DeleteOptions.builder()
                    .schema(schema.getName()).id(id)
                    .version(version)
                    .build();
            return database.delete(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private static Map<String, Expression> parseExpressions(final Map<String, String> exprs) {

        if(exprs == null) {
            return null;
        } else {
            return exprs.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> Expression.parse(e.getValue())
            ));
        }
    }

    private static Set<Path> paths(final DataFetchingEnvironment env) {

        return env.getSelectionSet().getFields().stream()
                .map(v -> path(v.getQualifiedName()))
                .collect(Collectors.toSet());
    }

    private static Path path(final String name) {

        // FIXME: if we do this properly then we don't need to use the reserved prefix in map key/value
        return Path.of(Arrays.stream(name.split("/"))
                .map(v -> {
                    if(v.equals(GraphQLUtils.MAP_VALUE)) {
                        return "*";
                    } else {
                        return v;
                    }
                }).filter(v -> !v.startsWith(Reserved.PREFIX))
                .toArray(String[]::new));
    }

    private static Integer count(final Number value) {

        if(value == null) {
            return null;
        } else {
            return value.intValue();
        }
    }

    private static PagingToken paging(final String value) {

        if(value == null) {
            return null;
        } else {
            return new PagingToken(value);
        }
    }

    private static List<Sort> sort(final List<?> value) {

        if(value == null) {
            return null;
        } else {
            return value.stream()
                    .map(v -> Sort.parse(v.toString()))
                    .collect(Collectors.toList());
        }
    }
}
