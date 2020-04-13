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
import io.basestar.database.options.CreateOptions;
import io.basestar.database.options.QueryOptions;
import io.basestar.database.options.ReadOptions;
import io.basestar.database.options.UpdateOptions;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.util.Path;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class RuntimeWiringFactory {

    private final Database database;

    private final Namespace namespace;

    private final DataConverter converter;

    public RuntimeWiringFactory(final Database database, final Namespace namespace) {

        this(database, namespace, new DataConverter());
    }

    public RuntimeWiringFactory(final Database database, final Namespace namespace, final DataConverter converter) {

        this.database = database;
        this.namespace = namespace;
        this.converter = converter;
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

    private Map<String, DataFetcher> queryFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                results.put("get" + objectSchema.getName(), getFetcher(objectSchema));
                results.put("query" + objectSchema.getName(), queryFetcher(objectSchema));
            }
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> getFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = Caller.SUPER;
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final Long version = env.getArgumentOrDefault(Reserved.VERSION, null);
            final ReadOptions options = new ReadOptions().setExpand(expand).setVersion(version);
            return database.read(caller, schema.getName(), id, options)
                    .thenApply(object -> converter.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> queryFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = Caller.SUPER;
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String query = env.getArgument("query");
            final QueryOptions options = new QueryOptions().setExpand(expand);
            return database.query(caller, schema.getName(), Expression.parse(query), options)
                    .thenApply(objects -> objects.map(object -> converter.toResponse(schema, object)));
        };
    }

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
            final Caller caller = Caller.SUPER;
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgumentOrDefault(Reserved.ID, null);
            final Map<String, Object> data = converter.fromRequest(schema, env.getArgument("data"));
            final CreateOptions options = new CreateOptions().setExpand(expand);
            if(id != null) {
                return database.create(caller, schema.getName(), id, data, options)
                        .thenApply(object -> converter.toResponse(schema, object));
            } else {
                return database.create(caller, schema.getName(), data, options)
                        .thenApply(object -> converter.toResponse(schema, object));
            }
        };
    }

    private DataFetcher<CompletableFuture<?>> updateFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = Caller.SUPER;
            final Set<Path> paths = paths(env);
            final Set<Path> expand = schema.requiredExpand(paths);
            final String id = env.getArgument(Reserved.ID);
            final Long version = env.getArgumentOrDefault(Reserved.VERSION, null);
            final Map<String, Object> data = converter.fromRequest(schema, env.getArgument("data"));
            final UpdateOptions options = new UpdateOptions().setExpand(expand).setVersion(version);
            return database.update(caller, schema.getName(), id, data, options)
                    .thenApply(object -> converter.toResponse(schema, object));
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
                    if(v.equals(TypeDefinitionRegistryFactory.MAP_VALUE)) {
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
