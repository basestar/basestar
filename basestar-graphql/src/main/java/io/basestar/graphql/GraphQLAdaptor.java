package io.basestar.graphql;

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

import graphql.ExecutionResult;
import graphql.ExecutionResultImpl;
import graphql.GraphQL;
import graphql.TypeResolutionEnvironment;
import graphql.execution.*;
import graphql.language.Field;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.TypeResolver;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.graphql.wiring.RuntimeWiringFactory;
import io.basestar.schema.*;
import io.basestar.schema.use.UseInteger;
import io.basestar.schema.use.UseString;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class GraphQLAdaptor {

    private final Database database;

    private final Namespace namespace;

    private final GraphQLNamingStrategy namingStrategy;

    public GraphQLAdaptor(final Database database, final Namespace namespace) {

        this(database, namespace, GraphQLNamingStrategy.DEFAULT);
    }

    public GraphQLAdaptor(final Database database, final Namespace namespace, final GraphQLNamingStrategy namingStrategy) {

        this.database = database;
        this.namespace = namespace;
        this.namingStrategy = namingStrategy;
    }

    public GraphQL graphQL() {

        final TypeDefinitionRegistry registry = new GraphQLSchemaAdaptor(namespace, namingStrategy).typeDefinitionRegistry();
        final RuntimeWiring wiring = new RuntimeWiringFactory(database, namespace, namingStrategy).runtimeWiring();
//        final RuntimeWiring wiring = runtimeWiring();

        final Map<String, Fetcher<ReadOptions>> reads = new HashMap<>();
        final Map<String, Fetcher<QueryOptions>> queries = new HashMap<>();
        final Map<String, Fetcher<QueryLinkOptions>> queryLinks = new HashMap<>();
        final Map<String, Fetcher<ActionOptions>> mutations = new HashMap<>();
        namespace.forEachObjectSchema((schemaName, schema) -> {
            reads.put(namingStrategy.readMethodName(schema), read(schema));
            queries.put(namingStrategy.queryMethodName(schema), query(schema));
            mutations.put(namingStrategy.createMethodName(schema), create(schema));
            mutations.put(namingStrategy.updateMethodName(schema), update(schema));
            mutations.put(namingStrategy.deleteMethodName(schema), delete(schema));
            schema.getLinks().forEach((linkName, link) -> {
                queryLinks.put(namingStrategy.queryLinkMethodName(schema, link), queryLink(schema, link));
            });
        });

        final SchemaGenerator generator = new SchemaGenerator();
        final GraphQLSchema schema = generator.makeExecutableSchema(registry, wiring);

        return GraphQL.newGraphQL(schema)
//                .queryExecutionStrategy(new AsyncExecutionStrategy() {
//                    @Override
//                    public CompletableFuture<ExecutionResult> execute(final ExecutionContext context, final ExecutionStrategyParameters params) throws NonNullableFieldWasNullException {
//
//                        final Caller caller = caller(context);
//                        final Map<String, CompletableFuture<?>> futures = new HashMap<>();
//                        for(final String key : params.getFields().keySet()) {
//                            final MergedField subfield = params.getFields().getSubField(key);
//                            final String alias = subfield.getResultKey();
//                            final Field field = subfield.getSingleField();
//                            final String name = field.getName();
//                            if("__schema".equals(name)) {
//                                futures.put(alias, CompletableFuture.completedFuture(schema));
//                            } else {
//                                final Fetcher<QueryOptions> query = queries.get(name);
//                                if (query != null) {
//                                    final QueryOptions options = query.apply(context, field);
//                                    final ObjectSchema schema = namespace.requireObjectSchema(options.getSchema());
//                                    futures.put(alias, database.query(caller, options)
//                                            .thenApply(vs -> vs.map(v -> GraphQLUtils.toResponse(schema, field.getSelectionSet(), v))));
//                                } else {
//                                    final Fetcher<ReadOptions> read = reads.get(name);
//                                    if (read != null) {
//                                        final ReadOptions options = read.apply(context, field);
//                                        final ObjectSchema schema = namespace.requireObjectSchema(options.getSchema());
//                                        futures.put(alias, database.read(caller, options)
//                                                .thenApply(v -> GraphQLUtils.toResponse(schema, field.getSelectionSet(), v)));
//                                    }
//                                }
//                            }
//                        }
//                        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture<?>[0])).thenApply(ignored -> {
//
//                            final Map<String, Object> responses = new HashMap<>();
//
//                            for(final String key : params.getFields().keySet()) {
//                                final MergedField subfield = params.getFields().getSubField(key);
//                                final String alias = subfield.getResultKey();
//                                final CompletableFuture<?> future = futures.get(alias);
//                                responses.put(alias, future.getNow(null));
//                            }
//
//                            return ExecutionResultImpl.newExecutionResult()
//                                    .data(responses)
//                                    .build();
//                        });
//                    }
//                })
                .mutationExecutionStrategy(new ExecutionStrategy() {
                    @Override
                    public CompletableFuture<ExecutionResult> execute(final ExecutionContext context, final ExecutionStrategyParameters params) throws NonNullableFieldWasNullException {

                        final Caller caller = GraphQLUtils.caller(context.getContext());
                        final Map<String, ActionOptions> actions = new HashMap<>();
                        for(final String key : params.getFields().keySet()) {
                            final MergedField subfield = params.getFields().getSubField(key);
                            final String alias = subfield.getResultKey();
                            final Field field = subfield.getSingleField();
                            final Fetcher<ActionOptions> fetcher = mutations.get(field.getName());
                            actions.put(alias, fetcher.apply(context, field));
                        }
                        return database.transaction(caller, TransactionOptions.builder()
                                .actions(actions)
                                .build()).thenApply(results -> {

                                    final Map<String, Object> responses = new HashMap<>();

                                    for(final String key : params.getFields().keySet()) {
                                        final MergedField subfield = params.getFields().getSubField(key);
                                        final String alias = subfield.getResultKey();
                                        final Field field = subfield.getSingleField();
                                        final ActionOptions action = actions.get(alias);
                                        final ObjectSchema schema = namespace.requireObjectSchema(action.getSchema());
                                        final Object value = GraphQLUtils.toResponse(schema, field.getSelectionSet(), results.get(key));
                                        responses.put(key, value);
                                    }

                                    return ExecutionResultImpl.newExecutionResult()
                                            .data(responses)
                                            .build();
                        });
                    }
                })
                .subscriptionExecutionStrategy(new SubscriptionExecutionStrategy())
                .build();
    }

    public interface Fetcher<T> {

        T apply(ExecutionContext context, Field field);
    }

    private Fetcher<ReadOptions> read(final ObjectSchema schema) {

        return (context, field) -> {

            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
            final Long version = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, Reserved.VERSION);
            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());

            assert id != null;

            final ReadOptions.Builder builder = ReadOptions.builder();
            builder.schema(schema.getName());
            builder.id(id);
            builder.version(version);
            builder.expand(schema.requiredExpand(paths));
            return builder.build();
        };
    }

    private Fetcher<QueryOptions> query(final ObjectSchema schema) {

        return (context, field) -> {

            final String expression = GraphQLUtils.argValue(context, UseString.DEFAULT, field, namingStrategy.queryArgumentName());
            final Long count = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, namingStrategy.countArgumentName());
            final String paging = GraphQLUtils.argValue(context, UseString.DEFAULT, field, namingStrategy.pagingArgumentName());
            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());

            assert expression != null;

            final QueryOptions.Builder builder = QueryOptions.builder();
            builder.schema(schema.getName());
            builder.expression(Expression.parse(expression));
            if(count != null) {
                builder.count(count.intValue());
            }
            if(paging != null) {
                builder.paging(new PagingToken(paging));
            }
            builder.expand(schema.requiredExpand(paths));
            return builder.build();
        };
    }

    private Fetcher<ActionOptions> create(final ObjectSchema schema) {

        return (context, field) -> {

            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
            final Map<String, Object> data = GraphQLUtils.argInput(context, schema, field, namingStrategy.dataArgumentName());
            final Map<String, Expression> expressions = GraphQLUtils.argInputExpr(context, schema, field, namingStrategy.expressionsArgumentName());
            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());

            final CreateOptions.Builder builder = CreateOptions.builder();
            builder.schema(schema.getName());
            builder.id(id);
            builder.data(data);
            builder.expressions(expressions);
            builder.expand(schema.requiredExpand(paths));
            return builder.build();
        };
    }

    private Fetcher<ActionOptions> update(final ObjectSchema schema) {

        return (context, field) -> {

            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
            final Long version = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, Reserved.VERSION);
            final Map<String, Object> data = GraphQLUtils.argInput(context, schema, field, namingStrategy.dataArgumentName());
            final Map<String, Expression> expressions = GraphQLUtils.argInputExpr(context, schema, field, namingStrategy.expressionsArgumentName());
            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());

            assert id != null;

            final UpdateOptions.Builder builder = UpdateOptions.builder();
            builder.schema(schema.getName());
            builder.id(id);
            builder.version(version);
            builder.data(data);
            builder.expressions(expressions);
            builder.expand(schema.requiredExpand(paths));
            return builder.build();
        };
    }

    private Fetcher<ActionOptions> delete(final ObjectSchema schema) {

        return (context, field) -> {

            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
            final Long version = GraphQLUtils.argValue(context, UseInteger.DEFAULT, field, Reserved.VERSION);
            assert id != null;

            final DeleteOptions.Builder builder = DeleteOptions.builder();
            builder.schema(schema.getName());
            builder.id(id);
            builder.version(version);
            return builder.build();
        };
    }

    private Fetcher<QueryLinkOptions> queryLink(final ObjectSchema schema, final Link link) {

        return (context, field) -> {

            final String id = GraphQLUtils.argValue(context, UseString.DEFAULT, field, Reserved.ID);
            final Set<Path> paths = GraphQLUtils.paths(schema, field.getSelectionSet());

            final QueryLinkOptions.Builder builder = QueryLinkOptions.builder();
            builder.schema(schema.getName());
            builder.id(id);
            builder.link(link.getName());
            builder.expand(schema.requiredExpand(paths));
            return builder.build();
        };
    }

    public RuntimeWiring runtimeWiring() {

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
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

    private static class InterfaceResolver implements TypeResolver {

        public static final InterfaceResolver INSTANCE = new InterfaceResolver();

        @Override
        public GraphQLObjectType getType(final TypeResolutionEnvironment env) {

            final Map<String, Object> object = env.getObject();
            return env.getSchema().getObjectType(Instance.getSchema(object));
        }
    }
}
