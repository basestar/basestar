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

import graphql.TypeResolutionEnvironment;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLObjectType;
import graphql.schema.TypeResolver;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.TypeRuntimeWiring;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.graphql.GraphQLUtils;
import io.basestar.schema.*;
import io.basestar.util.Path;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;


@Deprecated
public class RuntimeWiringFactory {

    private final Database database;

    private final Namespace namespace;

    public RuntimeWiringFactory(final Database database, final Namespace namespace) {

        this.database = database;
        this.namespace = namespace;
    }

    public RuntimeWiring runtimeWiring() {

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
        builder.type(TypeRuntimeWiring.newTypeWiring("Query")
                .dataFetchers(queryFetchers()));
        builder.type(TypeRuntimeWiring.newTypeWiring("Mutation")
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
        namespace.getSchemas().forEach((schemaName, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                results.put("read" + objectSchema.getName(), getFetcher(objectSchema));
                results.put("query" + objectSchema.getName(), queryFetcher(objectSchema));
                objectSchema.getAllLinks().forEach((linkName, link) -> {
                    final String name = "query" + schemaName + GraphQLUtils.ucFirst(linkName);
                    results.put(name, queryLinkFetcher(objectSchema, link));
                });
            }
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
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String query = env.getArgument("query");
            final QueryOptions options = QueryOptions.builder()
                    .schema(schema.getName())
                    .expression(Expression.parse(query))
                    .expand(expand)
                    .build();
            return database.query(caller, options)
                    .thenApply(objects -> objects.map(object -> GraphQLUtils.toResponse(schema, object)));
        };
    }

    private DataFetcher<CompletableFuture<?>> queryLinkFetcher(final ObjectSchema schema, final Link link) {

        return (env) -> {

            final Caller caller = GraphQLUtils.caller(env.getContext());
            final ObjectSchema linkSchema = link.getSchema();
            final Set<Path> paths = paths(env);
            final Set<Path> expand = linkSchema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final QueryLinkOptions options = QueryLinkOptions.builder()
                    .schema(schema.getName())
                    .link(link.getName())
                    .id(id)
                    .expand(expand)
                    .build();
            return database.queryLink(caller, options)
                    .thenApply(objects -> objects.map(object -> GraphQLUtils.toResponse(linkSchema, object)));
        };
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> mutationFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                results.put("create" + objectSchema.getName(), createFetcher(objectSchema));
                results.put("update" + objectSchema.getName(), updateFetcher(objectSchema));
            }
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> createFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgumentOrDefault(Reserved.ID, null);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument("data"));
            final CreateOptions options = CreateOptions.builder()
                    .schema(schema.getName()).id(id)
                    .data(data).expand(expand)
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
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument("data"));
            final UpdateOptions options = UpdateOptions.builder()
                    .schema(schema.getName()).id(id)
                    .data(data).version(version)
                    .expand(expand)
                    .build();
            return database.update(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
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

    private static class InterfaceResolver implements TypeResolver {

        public static final InterfaceResolver INSTANCE = new InterfaceResolver();

        @Override
        public GraphQLObjectType getType(final TypeResolutionEnvironment env) {

            final Map<String, Object> object = env.getObject();
            return env.getSchema().getObjectType(Instance.getSchema(object));
        }
    }
}
