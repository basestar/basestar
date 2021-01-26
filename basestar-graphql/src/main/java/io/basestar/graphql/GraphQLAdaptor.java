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

import com.google.common.collect.ImmutableMap;
import graphql.GraphQL;
import graphql.execution.AsyncExecutionStrategy;
import graphql.language.*;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
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
import io.basestar.graphql.transform.GraphQLRequestTransform;
import io.basestar.graphql.transform.GraphQLResponseTransform;
import io.basestar.graphql.wiring.*;
import io.basestar.schema.*;
import io.basestar.secret.SecretContext;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
public class GraphQLAdaptor {

    private final Database database;

    private final Namespace namespace;

    private final GraphQLStrategy strategy;

    private final GraphQLRequestTransform requestTransform;

    private final GraphQLResponseTransform responseTransform;

    private final SecretContext secretContext;

    @lombok.Builder(builderClassName = "Builder")
    GraphQLAdaptor(final Database database, final Namespace namespace, final GraphQLStrategy strategy,
                   final SecretContext secretContext) {

        this.database = Nullsafe.require(database);
        this.namespace = Nullsafe.require(namespace);
        this.strategy = Nullsafe.orDefault(strategy, GraphQLStrategy.DEFAULT);
        this.requestTransform = this.strategy.requestTransform();
        this.responseTransform = this.strategy.responseTransform();
        this.secretContext = Nullsafe.orDefault(secretContext, SecretContext::none);
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

    public static RuntimeWiring.Builder runtimeWiringBuilder(final GraphQLStrategy strategy, final SecretContext secretContext) {

        final RuntimeWiring.Builder builder = RuntimeWiring.newRuntimeWiring();
        builder.scalar(GraphQLScalarType.newScalar()
                .name(strategy.anyTypeName())
                .coercing(AnyCoercing.INSTANCE)
                .build());
        builder.scalar(GraphQLScalarType.newScalar()
                .name(strategy.dateTypeName())
                .coercing(DateCoercing.INSTANCE)
                .build());
        builder.scalar(GraphQLScalarType.newScalar()
                .name(strategy.dateTimeTypeName())
                .coercing(DateTimeCoercing.INSTANCE)
                .build());
        builder.scalar(GraphQLScalarType.newScalar()
                .name(strategy.binaryTypeName())
                .coercing(BinaryCoercing.INSTANCE)
                .build());
        builder.scalar(GraphQLScalarType.newScalar()
                .name(strategy.secretTypeName())
                .coercing(new SecretCoercing(secretContext))
                .build());
        return builder;
    }

    public RuntimeWiring runtimeWiring() {

        final RuntimeWiring.Builder builder = runtimeWiringBuilder(strategy, secretContext);
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
                            .typeResolver(new InterfaceResolver((InstanceSchema)schema, strategy)));
                }
            }
        });
        return builder.build();
    }

    @SuppressWarnings("rawtypes")
    private Map<String, DataFetcher> queryFetchers() {

        final Map<String, DataFetcher> results = new HashMap<>();
        namespace.forEachLinkableSchema((schemaName, schema) -> {
            if(schema instanceof ReferableSchema) {
                final ReferableSchema objectSchema = (ReferableSchema)schema;
                results.put(strategy.readMethodName(objectSchema), readFetcher(objectSchema));
            }
            results.put(strategy.queryMethodName(schema), queryFetcher(schema));
            schema.getLinks().forEach((linkName, link) -> {
                results.put(strategy.queryLinkMethodName(schema, link), queryLinkFetcher(schema, link));
            });
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> readFetcher(final ReferableSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> expand = expand(schema, env);
            final String id = env.getArgument(strategy.idArgumentName());
            final Long version = version(env);
            return read(caller, schema, id, version, expand);
        };
    }

    private CompletableFuture<?> read(final Caller caller, final ReferableSchema schema, final String id, final Long version, final Set<Name> expand) {

        final ReadOptions options = ReadOptions.builder()
                .setSchema(schema.getQualifiedName())
                .setId(id).setVersion(version).setExpand(expand)
                .build();
        return database.read(caller, options)
                .thenApply(object -> responseTransform.toResponse(schema, object));
    }

    private DataFetcher<CompletableFuture<?>> queryFetcher(final LinkableSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> expand = expand(schema, env, strategy.pageItemsFieldName());
            final String query = env.getArgument(strategy.queryArgumentName());
            final Expression expression = query == null ? Constant.TRUE : Expression.parse(query);
            final Page.Token paging = paging(env);
            final Integer count = count(env);
            final List<Sort> sort = sort(env);
            final Set<Page.Stat> stats = stats(env);
            final QueryOptions options = QueryOptions.builder()
                    .setSchema(schema.getQualifiedName())
                    .setExpression(expression)
                    .setPaging(paging)
                    .setCount(count)
                    .setStats(stats)
                    .setSort(sort)
                    .setExpand(expand)
                    .build();
            return database.query(caller, options)
                    .thenApply(result -> responseTransform.toResponsePage(schema, result));
        };
    }

    private DataFetcher<CompletableFuture<?>> queryLinkFetcher(final LinkableSchema schema, final Link link) {

        return (env) -> {

            final Caller caller = GraphQLUtils.caller(env.getContext());
            final InstanceSchema linkSchema = link.getSchema();
            final Set<Name> expand = expand(linkSchema, env, strategy.pageItemsFieldName());
            final String id = env.getArgument(strategy.idArgumentName());
            final Page.Token paging = paging(env);
            final Integer count = count(env);
            final List<Sort> sort = sort(env);
            final Set<Page.Stat> stats = stats(env);
            final QueryLinkOptions options = QueryLinkOptions.builder()
                    .setSchema(schema.getQualifiedName())
                    .setLink(link.getName())
                    .setId(id)
                    .setExpand(expand)
                    .setPaging(paging)
                    .setCount(count)
                    .setStats(stats)
                    .setSort(sort)
                    .build();
            return database.queryLink(caller, options)
                    .thenApply(result -> responseTransform.toResponsePage(linkSchema, result));
        };
    }

    private Set<Page.Stat> stats(final DataFetchingEnvironment env) {

        final Set<Page.Stat> stats = new HashSet<>();
        final SelectionSet selections = env.getMergedField().getSingleField().getSelectionSet();
        if(GraphQLUtils.findField(selections, strategy.pageTotalFieldName()) != null) {
            stats.add(Page.Stat.TOTAL);
        }
        if(GraphQLUtils.findField(selections, strategy.pageApproxTotalFieldName()) != null) {
            stats.add(Page.Stat.APPROX_TOTAL);
        }
        return stats;
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
            final Set<Name> expand = expand(schema, env);
            final String id = env.getArgumentOrDefault(strategy.idArgumentName(), null);
            final Map<String, Object> data = requestTransform.fromRequest(schema, env.getArgument(strategy.dataArgumentName()));
            final Consistency consistency = consistency(env);
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(strategy.expressionsArgumentName()));
            final CreateOptions options = CreateOptions.builder()
                    .setSchema(schema.getQualifiedName()).setId(id)
                    .setData(data).setConsistency(consistency).setExpand(expand)
                    .setExpressions(expressions)
                    .build();
            return database.create(caller, options)
                    .thenApply(object -> responseTransform.toResponse(schema, object));
        };
    }

    private DataFetcher<CompletableFuture<?>> updateFetcher(final ObjectSchema schema, final UpdateOptions.Mode mode) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final Set<Name> expand = expand(schema, env);
            final String id = env.getArgument(strategy.idArgumentName());
            final Long version = version(env);
            final Map<String, Object> data = requestTransform.fromRequest(schema, env.getArgument(strategy.dataArgumentName()));
            final Consistency consistency = consistency(env);
            final Map<String, Expression> expressions = parseExpressions(env.getArgument(strategy.expressionsArgumentName()));

            final UpdateOptions options = UpdateOptions.builder()
                    .setSchema(schema.getQualifiedName()).setId(id)
                    .setMode(mode).setConsistency(consistency)
                    .setData(data).setVersion(version)
                    .setExpressions(expressions)
                    .setExpand(expand)
                    .build();
            return database.update(caller, options)
                    .thenApply(object -> responseTransform.toResponse(schema, object));
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
            final Consistency consistency = consistency(env);
            final DeleteOptions options = DeleteOptions.builder()
                    .setSchema(schema.getQualifiedName()).setId(id)
                    .setVersion(version).setConsistency(consistency)
                    .build();
            return database.delete(caller, options)
                    .thenApply(object -> responseTransform.toResponse(schema, object));
        };
    }

    private interface BatchHandler {

        ActionOptions actionOptions(DataFetchingEnvironment env, Field field);
    }

    @SuppressWarnings("unchecked")
    private <T> T argument(final Field field, final String name) {

        if(field != null && field.getArguments() != null) {
            for (final Argument argument : field.getArguments()) {
                if (argument.getName().equals(name)) {
                    final Value<?> value = argument.getValue();
                    return (T) GraphQLUtils.fromValue(value, ImmutableMap.of());
                }
            }
        }
        return null;
    }

    // FIXME: merge with createFetcher
    private BatchHandler createBatchHandler(final ObjectSchema schema) {

        return (env, field) -> {

            final Set<Name> expand = expand(schema, field);
            final String id = argument(field, strategy.idArgumentName());
            final Map<String, Object> data = requestTransform.fromRequest(schema, argument(field, strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(argument(field, strategy.expressionsArgumentName()));
            return CreateOptions.builder()
                    .setSchema(schema.getQualifiedName()).setId(id)
                    .setData(data).setExpand(expand)
                    .setExpressions(expressions)
                    .build();
        };
    }

    // FIXME: merge with updateFetcher
    private BatchHandler updateBatchHandler(final ObjectSchema schema, final UpdateOptions.Mode mode) {

        return (env, field) -> {

            final Set<Name> expand = expand(schema, field);
            final String id = argument(field, strategy.idArgumentName());
            final Long version = version(field);
            final Map<String, Object> data = requestTransform.fromRequest(schema, argument(field, strategy.dataArgumentName()));
            final Map<String, Expression> expressions = parseExpressions(argument(field, strategy.expressionsArgumentName()));
            return UpdateOptions.builder()
                    .setSchema(schema.getQualifiedName()).setId(id)
                    .setMode(mode)
                    .setData(data).setVersion(version)
                    .setExpressions(expressions)
                    .setExpand(expand)
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
                    .setSchema(schema.getQualifiedName())
                    .setId(id).setVersion(version)
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
                    .setConsistency(consistency);

            env.getMergedField().getSingleField().getSelectionSet().getSelections().forEach(selection -> {
                if(selection instanceof Field) {
                    final Field field = (Field)selection;
                    final BatchHandler handler = handlers.get(field.getName());
                    builder.putAction(field.getName(), handler.actionOptions(env, field));
                }
            });

            return database.batch(caller, builder.build())
                    .thenApply(results -> {
                        final Map<String, Object> response = new HashMap<>();
                        results.forEach((k, v) -> {
                            final ObjectSchema schema = namespace.requireObjectSchema(Instance.getSchema(v));
                            response.put(k, responseTransform.toResponse(schema, v));
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
            results.put(strategy.subscribeQueryMethodName(schema), subscribeQueryFetcher(schema));
        });
        return results;
    }

    private DataFetcher<CompletableFuture<?>> subscribeFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final SubscriberContext subscriberContext = GraphQLUtils.subscriber(env.getContext());
            final Set<Name> names = paths(env);
            final Set<Name> expand = expand(schema, env);
            final String alias = Nullsafe.orDefault(env.getField().getAlias(), () -> strategy.subscribeMethodName(schema));
            final String id = env.getArgument(strategy.idArgumentName());
            return subscriberContext.subscribe(schema, id, alias, names)
                    .thenCompose(ignored -> read(caller, schema, id, null, expand));
        };
    }

    private DataFetcher<CompletableFuture<?>> subscribeQueryFetcher(final ObjectSchema schema) {

        return (env) -> {
            final Caller caller = GraphQLUtils.caller(env.getContext());
            final SubscriberContext subscriberContext = GraphQLUtils.subscriber(env.getContext());
            final Set<Name> expand = expand(schema, env, strategy.pageItemsFieldName());
            final Set<Name> names = paths(env);
            final String alias = Nullsafe.orDefault(env.getField().getAlias(), () -> strategy.subscribeMethodName(schema));
            final String query = env.getArgument(strategy.queryArgumentName());
            final Expression expression = Expression.parse(query);
            final Integer count = count(env);
            final List<Sort> sort = sort(env);
            final QueryOptions options = QueryOptions.builder()
                    .setSchema(schema.getQualifiedName())
                    .setExpression(expression)
                    .setCount(count)
                    .setSort(sort)
                    .setExpand(expand)
                    .build();
            return subscriberContext.subscribe(schema, expression, alias, names)
                    .thenCompose(ignored -> database.query(caller, options)
                        .thenApply(result -> responseTransform.toResponsePage(schema, result)));
        };
    }

    private static Map<String, Expression> parseExpressions(final Map<String, String> exprs) {

        if (exprs == null) {
            return null;
        } else {
            return exprs.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> Expression.parse(e.getValue())
            ));
        }
    }

    private Set<Name> expand(final InstanceSchema schema, final DataFetchingEnvironment env, final String fieldName) {

        final SelectionSet selectionSet = env.getMergedField().getSingleField().getSelectionSet();
        for(final Selection<?> selection : selectionSet.getSelections()) {
            if(selection instanceof Field) {
                final Field field = (Field)selection;
                if(fieldName.equals(field.getName())) {
                    return GraphQLUtils.expand(strategy, namespace, schema, field.getSelectionSet());
                }
            }
        }
        return Collections.emptySet();
    }

    private Set<Name> expand(final InstanceSchema schema, final DataFetchingEnvironment env) {

        return GraphQLUtils.expand(strategy, namespace, schema, env.getMergedField().getSingleField().getSelectionSet());
    }

    private Set<Name> expand(final InstanceSchema schema, final Field field) {

        return GraphQLUtils.expand(strategy, namespace, schema, field.getSelectionSet());
    }

    @Deprecated
    private static Set<Name> paths(final DataFetchingEnvironment env) {

        return paths(env.getMergedField().getSingleField().getSelectionSet());
    }

    @Deprecated
    private static Set<Name> paths(final SelectionSet selections) {

        if(selections == null || selections.getSelections() == null) {
            return Collections.emptySet();
        }
        return selections.getSelections().stream()
                .flatMap(selection -> {
                    final Set<Name> paths = new HashSet<>();
                    if(selection instanceof Field) {
                        final Field field = (Field)selection;
                        final Name path = path(field.getName());
                        paths.add(path);
                        paths(field.getSelectionSet()).forEach(v -> paths.add(path.with(v)));
                    } else if(selection instanceof InlineFragment) {
                        final InlineFragment fragment = (InlineFragment)selection;
                        paths.addAll(paths(fragment.getSelectionSet()));
                    } else {
                        log.warn("Skipping selection {}", selection);
                    }
                    return paths.stream();
                })
                .filter(v -> !v.isEmpty())
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

    private Long version(final Field field) {

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

    private Consistency consistency(final DataFetchingEnvironment env) {

        final String value = env.getArgument(strategy.consistencyArgumentName());
        if(value == null) {
            return null;
        } else {
            return Consistency.valueOf(value.toUpperCase());
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

    private Page.Token paging(final DataFetchingEnvironment env) {

        final String value = env.getArgument(strategy.pagingArgumentName());
        if(value == null) {
            return null;
        } else {
            return new Page.Token(value);
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
