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

import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLSchema;
import graphql.schema.SelectedField;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.TypeDefinitionRegistry;
import graphql.schema.idl.TypeRuntimeWiring;
import io.basestar.auth.Caller;
import io.basestar.database.Database;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.Constant;
import io.basestar.graphql.schema.SchemaAdaptor;
import io.basestar.graphql.subscription.SubscriberContext;
import io.basestar.graphql.wiring.InterfaceResolver;
import io.basestar.schema.*;
import io.basestar.util.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GraphQLAdaptor {

    private final Database database;

    private final Namespace namespace;

    private final GraphQLStrategy strategy;

    @lombok.Builder(builderClassName = "Builder")
    GraphQLAdaptor(final Database database, final Namespace namespace, final GraphQLStrategy strategy) {

        this.database = Nullsafe.require(database);
        this.namespace = Nullsafe.require(namespace);
        this.strategy = Nullsafe.option(strategy, GraphQLStrategy.DEFAULT);
    }

    public GraphQL graphQL() {

        final TypeDefinitionRegistry registry = new SchemaAdaptor(namespace, strategy).typeDefinitionRegistry();
        final RuntimeWiring wiring = runtimeWiring();

        final SchemaGenerator generator = new SchemaGenerator();
        final GraphQLSchema schema = generator.makeExecutableSchema(registry, wiring);

        return GraphQL.newGraphQL(schema)
                .subscriptionExecutionStrategy(new AsyncExecutionStrategy())
                .build();
    }

    public RuntimeWiring runtimeWiring() {

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
        builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLUtils.QUERY_TYPE)
                .dataFetchers(queryFetchers()));
        builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLUtils.MUTATION_TYPE)
                .dataFetchers(mutationFetchers()));
        builder.type(TypeRuntimeWiring.newTypeWiring(GraphQLUtils.SUBSCRIPTION_TYPE)
                .dataFetchers(subscriptionFetchers()));
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof InstanceSchema) {
                if(!((InstanceSchema) schema).isConcrete()) {
                    builder.type(TypeRuntimeWiring.newTypeWiring(strategy.typeName(schema))
                            .typeResolver(new InterfaceResolver(strategy)));
                }
            }
        });
        return builder.build();
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> queryFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.forEachObjectSchema((schemaName, schema) -> {
            results.put(strategy.readMethodName(schema), readFetcher(schema));
            results.put(strategy.queryMethodName(schema), queryFetcher(schema));
            schema.getLinks().forEach((linkName, link) -> {
                results.put(strategy.queryLinkMethodName(schema, link), queryLinkFetcher(schema, link));
            });
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> readFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> names = paths(env);
            final Set<Name> expand = schema.requiredExpand(names);
            final String id = env.getArgument(strategy.idArgumentName());
            final Long version = version(env);
            return read(caller, schema, id, version, expand);
        };
    }

    private CompletableFuture<?> read(final Caller caller, final ObjectSchema schema, final String id, final Long version, final Set<Name> expand) {

        final ReadOptions options = ReadOptions.builder()
                .schema(schema.getQualifiedName()).id(id)
                .version(version).expand(expand)
                .build();
        return database.read(caller, options)
                .thenApply(object -> GraphQLUtils.toResponse(schema, object));
    }

    private DataFetcher<CompletableFuture<?>> queryFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> names = Name.children(paths(env), strategy.pageItemsFieldName());
            final Set<Name> expand = schema.requiredExpand(names);
            final String query = env.getArgument(strategy.queryArgumentName());
            final Expression expression = query == null ? Constant.TRUE : Expression.parse(query);
            final PagingToken paging = paging(env);
            final Integer count = count(env);
            final List<Sort> sort = sort(env);
            final QueryOptions options = QueryOptions.builder()
                    .schema(schema.getQualifiedName())
                    .expression(expression)
                    .paging(paging)
                    .count(count)
                    .sort(sort)
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
            final InstanceSchema linkSchema = link.getSchema();
            final Set<Name> names = Name.children(paths(env), strategy.pageItemsFieldName());
            final Set<Name> expand = linkSchema.requiredExpand(names);
            final String id = env.getArgument(strategy.idArgumentName());
            final PagingToken paging = paging(env);
            final Integer count = count(env);
            final QueryLinkOptions options = QueryLinkOptions.builder()
                    .schema(schema.getQualifiedName())
                    .link(link.getName())
                    .id(id)
                    .expand(expand)
                    .paging(paging)
                    .count(count)
                    .build();
            return database.queryLink(caller, options)
                    .thenApply(objects -> objects.map(object -> GraphQLUtils.toResponse(linkSchema, object)))
                    .thenApply(this::toPage);
        };
    }

    private Map<String, Object> toPage(final PagedList<?> page) {

        final Map<String, Object> result = new HashMap<>();
        result.put(strategy.pageItemsFieldName(), page.getPage());
        if(page.hasPaging()) {
            result.put(strategy.pagePagingFieldName(), page.getPaging().toString());
        }
        return result;
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> mutationFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                results.put(strategy.createMethodName(objectSchema), createFetcher(objectSchema));
                results.put(strategy.updateMethodName(objectSchema), updateFetcher(objectSchema));
                results.put(strategy.patchMethodName(objectSchema), patchFetcher(objectSchema));
                results.put(strategy.deleteMethodName(objectSchema), deleteFetcher(objectSchema));
                results.put(strategy.batchMethodName(), batchFetcher());
            }
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> createFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> names = paths(env);
            final Set<Name> expand = schema.requiredExpand(names);
            final String id = env.getArgumentOrDefault(strategy.idArgumentName(), null);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument(strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(strategy.expressionsArgumentName()));
            final CreateOptions options = CreateOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .data(data).expand(expand)
                    .expressions(expressions)
                    .build();
            return database.create(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> updateFetcher(final ObjectSchema schema, final UpdateOptions.Mode mode) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> names = paths(env);
            final Set<Name> expand = schema.requiredExpand(names);
            final String id = env.getArgument(strategy.idArgumentName());
            final Long version = version(env);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, env.getArgument(strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(strategy.expressionsArgumentName()));

            final UpdateOptions options = UpdateOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .mode(mode)
                    .data(data).version(version)
                    .expressions(expressions)
                    .expand(expand)
                    .build();
            return database.update(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> updateFetcher(final ObjectSchema schema) {

        return updateFetcher(schema, strategy.updateMode());
    }

    private DataFetcher<CompletableFuture<?>> patchFetcher(final ObjectSchema schema) {

        return updateFetcher(schema, strategy.patchMode());
    }

    private DataFetcher<CompletableFuture<?>> deleteFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final String id = env.getArgument(strategy.idArgumentName());
            final Long version = version(env);
            final DeleteOptions options = DeleteOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .version(version)
                    .build();
            return database.delete(caller, options)
                    .thenApply(object -> GraphQLUtils.toResponse(schema, object));
        };
    }

    private interface BatchHandler {

        ActionOptions actionOptions(DataFetchingEnvironment env, SelectedField field);
    }

    @SuppressWarnings("unchecked")
    private <T> T argument(final SelectedField field, final String name) {

        return field.getArguments() == null ? null : (T)field.getArguments().get(name);
    }

    // FIXME: merge with createFetcher
    private BatchHandler createBatchHandler(final ObjectSchema schema) {

        return (env, field) -> {

            final Set<Name> names = paths(field);
            final Set<Name> expand = schema.requiredExpand(names);
            final String id = argument(field, strategy.idArgumentName());
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, argument(field, strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(argument(field, strategy.expressionsArgumentName()));
            return CreateOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .data(data).expand(expand)
                    .expressions(expressions)
                    .build();
        };
    }

    // FIXME: merge with updateFetcher
    private BatchHandler updateBatchHandler(final ObjectSchema schema, final UpdateOptions.Mode mode) {

        return (env, field) -> {
            final Set<Name> names = paths(field);
            final Set<Name> expand = schema.requiredExpand(names);
            final String id = argument(field, strategy.idArgumentName());
            final Long version = version(field);
            final Map<String, Object> data = GraphQLUtils.fromRequest(schema, argument(field, strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(argument(field, strategy.expressionsArgumentName()));
            return UpdateOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .mode(mode)
                    .data(data).version(version)
                    .expressions(expressions)
                    .expand(expand)
                    .build();
        };
    }

    private BatchHandler updateBatchHandler(final ObjectSchema schema) {

        return updateBatchHandler(schema, strategy.updateMode());
    }

    private BatchHandler patchBatchHandler(final ObjectSchema schema) {

        return updateBatchHandler(schema, strategy.patchMode());
    }

    private BatchHandler deleteBatchHandler(final ObjectSchema schema) {

        return (env, field) -> {
            final String id = argument(field, strategy.idArgumentName());
            final Long version = version(field);
            return DeleteOptions.builder()
                    .schema(schema.getQualifiedName()).id(id)
                    .version(version)
                    .build();
        };
    }

    private DataFetcher<CompletableFuture<?>> batchFetcher() {

        final Map<String, BatchHandler> handlers = new HashMap<>();
        namespace.getSchemas().forEach((k, schema) -> {
            if(schema instanceof ObjectSchema) {
                final ObjectSchema objectSchema = (ObjectSchema)schema;
                handlers.put(strategy.createMethodName(objectSchema), createBatchHandler(objectSchema));
                handlers.put(strategy.updateMethodName(objectSchema), updateBatchHandler(objectSchema));
                handlers.put(strategy.patchMethodName(objectSchema), patchBatchHandler(objectSchema));
                handlers.put(strategy.deleteMethodName(objectSchema), deleteBatchHandler(objectSchema));
            }
        });
        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final String consistencyStr = env.getArgument(strategy.consistencyArgumentName());
            final Consistency consistency = consistencyStr == null ? Consistency.EVENTUAL : Consistency.valueOf(consistencyStr);

            final BatchOptions.Builder builder = BatchOptions.builder()
                    .consistency(consistency);

            env.getSelectionSet().getFields().forEach(field -> {
                if(!field.getQualifiedName().contains("/")) {
                    final BatchHandler handler = handlers.get(field.getName());
                    builder.action(field.getQualifiedName(), handler.actionOptions(env, field));
                }
            });

            return database.batch(caller, builder.build())
                    .thenApply(results -> {
                        final Map<String, Object> response = new HashMap<>();
                        results.forEach((k, v) -> {
                            final ObjectSchema schema = namespace.requireObjectSchema(Instance.getSchema(v));
                            response.put(k, GraphQLUtils.toResponse(schema, v));
                        });
                        return response;
                    });
        };
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> subscriptionFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.forEachObjectSchema((schemaName, schema) -> {
            results.put(strategy.subscribeMethodName(schema), subscribeFetcher(schema));
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> subscribeFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final SubscriberContext subscriberContext = GraphQLUtils.subscriber(env.getContext());
            final Set<Name> names = paths(env);
            final Set<Name> expand = schema.requiredExpand(names);
            final String alias = env.getField().getAlias();
            final String id = env.getArgument(strategy.idArgumentName());
            return subscriberContext.subscribe(schema, id, alias, names)
                    .thenCompose(ignored -> read(caller, schema, id, null, expand));
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

    private static Set<Name> paths(final DataFetchingEnvironment env) {

        return paths(env.getSelectionSet().getFields());
    }

    private static Set<Name> paths(final SelectedField field) {

        return paths(field.getSelectionSet().getFields());
    }

    private static Set<Name> paths(final List<SelectedField> fields) {

        return fields.stream()
                .map(v -> path(v.getQualifiedName()))
                .collect(Collectors.toSet());
    }

    private static Name path(final String name) {

        // FIXME: if we do this properly then we don't need to use the reserved prefix in map key/value
        return Name.of(Arrays.stream(name.split("/"))
                .map(v -> {
                    if(v.equals(GraphQLUtils.MAP_VALUE)) {
                        return "*";
                    } else {
                        return v;
                    }
                }).filter(v -> !v.startsWith(Reserved.PREFIX))
                .toArray(String[]::new));
    }

    private Long version(final SelectedField field) {

        final Number value = argument(field, strategy.versionArgumentName());
        if(value == null) {
            return null;
        } else {
            return value.longValue();
        }
    }

    private Long version(final DataFetchingEnvironment env) {

        final Number value = env.getArgument(strategy.versionArgumentName());
        if(value == null) {
            return null;
        } else {
            return value.longValue();
        }
    }

    private Integer count(final DataFetchingEnvironment env) {

        final Number value = env.getArgument(strategy.countArgumentName());
        if(value == null) {
            return null;
        } else {
            return value.intValue();
        }
    }

    private PagingToken paging(final DataFetchingEnvironment env) {

        final String value = env.getArgument(strategy.pagingArgumentName());
        if(value == null) {
            return null;
        } else {
            return new PagingToken(value);
        }
    }

    private List<Sort> sort(final DataFetchingEnvironment env) {

        final List<?> value = env.getArgument(strategy.sortArgumentName());
        if(value == null) {
            return null;
        } else {
            return value.stream()
                    .map(v -> Sort.parse(v.toString()))
                    .collect(Collectors.toList());
        }
    }
}
