package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseRef;
import io.basestar.schema.use.UseStruct;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DefaultLayerStorage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.util.Name;
import io.basestar.util.*;
import lombok.Data;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.SQLStateClass;
import org.jooq.impl.DSL;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * FIXME: currently we use literals/inline everywhere because Athena doesn't like ? params, need to make this
 * strategy-driven rather than forced on everyone.
 */

@Slf4j
public class SQLStorage implements DefaultLayerStorage {

    private static final String COUNT_AS = "__count";

    private final DataSource dataSource;

    private final SQLDialect dialect;

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
        this.dialect = builder.dialect;
        this.strategy = builder.strategy;
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private DataSource dataSource;

        private SQLDialect dialect;

        private SQLStrategy strategy;

        private EventStrategy eventStrategy;

        public SQLStorage build() {

            return new SQLStorage(this);
        }
    }

    private List<SelectFieldOrAsterisk> selectFields(final ObjectSchema schema) {

        final List<SelectFieldOrAsterisk> fields = new ArrayList<>();
        schema.metadataSchema().forEach((name, type) -> {
            fields.add(SQLUtils.selectField(DSL.field(DSL.name(name)), type).as(DSL.name(name)));
        });
        schema.getProperties().forEach((name, prop) -> {
            fields.add(SQLUtils.selectField(DSL.field(DSL.name(name)), prop.typeOf()).as(DSL.name(name)));
        });
        return fields;
    }

//    @Override
//    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {
//
//        return withContext(context -> context.select(selectFields(schema))
//                .from(DSL.table(objectTableName(schema)))
//                .where(idField(schema).eq(DSL.inline(id)))
//                .limit(DSL.inline(1)).fetchAsync().thenApply(result -> first(schema, result)));
//    }
//
//    @Override
//    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {
//
//        return withContext(context -> context.select(selectFields(schema))
//                .from(DSL.table(historyTableName(schema)))
//                .where(idField(schema).eq(DSL.inline(id))
//                        .and(versionField(schema).eq(DSL.inline(version))))
//                .limit(DSL.inline(1)).fetchAsync().thenApply(result -> first(schema, result)));
//    }

    @Override
    public Pager<Map<String, Object>> queryObject(final ObjectSchema schema, final Expression query,
                                                  final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());

        final Set<Expression> disjunction = new DisjunctionVisitor().visit(bound);

        final Map<String, Pager<Map<String, Object>>> sources = new HashMap<>();

        // FIXME: use a union instead of doing this manually
        for(final Expression conjunction : disjunction) {
            final Map<Name, Range<Object>> ranges = conjunction.visit(new RangeVisitor());

            Index best = null;
            // Only multi-value indexes need to be matched separately
            for(final Index index : schema.getIndexes().values()) {
                if(index.isMultiValue()) {
                    final Set<Name> names = index.getMultiValuePaths();
                    if (ranges.keySet().containsAll(names)) {
                        best = index;
                    }
                }
            }

            final Index index = best;
            sources.put(conjunction.digest(), (stats, token, count) -> queryImpl(schema, index, conjunction, sort, expand, count, token, stats));
        }

        return Pager.merge(Instance.comparator(sort), sources);
    }

    private Condition condition(final DSLContext context, final ObjectSchema schema, final Index index, final Expression expression) {

        final Function<Name, QueryPart> columnResolver;
        if(index == null) {
            columnResolver = objectColumnResolver(context, schema);
        } else {
            columnResolver = indexColumnResolver(context, schema, index);
        }
        return new SQLExpressionVisitor(columnResolver).condition(expression);
    }

    private List<OrderField<?>> orderFields(final List<Sort> sort) {

        return sort.stream()
                .map(v -> {
                    final Field<?> field = DSL.field(SQLUtils.columnName(v.getName()));
                    return v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc();
                })
                .collect(Collectors.toList());
    }

    private CompletableFuture<Page<Map<String, Object>>> queryImpl(final ObjectSchema schema, final Index index,
                                                                   final Expression expression, final List<Sort> sort,
                                                                   final Set<Name> expand, final int count,
                                                                   final Page.Token token, final Set<Page.Stat> stats) {

        final List<OrderField<?>> orderFields = orderFields(sort);

        final Table<Record> table;
        if(index == null) {
            table = DSL.table(objectTableName(schema));
        } else {
            table = DSL.table(indexTableName(schema, index));
        }

        final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {

            final Condition condition = condition(context, schema, index, expression);

            log.debug("SQL condition {}", condition);

            final SelectSeekStepN<Record> select = context.select(selectFields(schema))
                    .from(table).where(condition).orderBy(orderFields);

            final SelectForUpdateStep<Record> seek;
            if(token == null) {
                seek = select.limit(DSL.inline(count));
            } else {
                final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
                seek = select.seek(values.toArray(new Object[0])).limit(DSL.inline(count));
            }

            return seek.fetchAsync().thenApply(results -> {

                final List<Map<String, Object>> objects = all(schema, results);

                final Page.Token nextToken;
                if(objects.size() < count) {
                    nextToken = null;
                } else {
                    final Map<String, Object> last = objects.get(objects.size() - 1);
                    nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, last);
                }

                return new Page<>(objects, nextToken);
            });

        });

        if(stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {

            // Runs in parallel with query
            final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {

                final Condition condition = condition(context, schema, index, expression);

                return context.select(DSL.count().as(COUNT_AS)).from(table).where(condition).fetchAsync().thenApply(results -> {

                    if(results.isEmpty()) {
                        return Page.Stats.ZERO;
                    } else {
                        final Record1<Integer> record = results.iterator().next();
                        return Page.Stats.fromTotal(record.value1());
                    }
                });
            });

            return CompletableFuture.allOf(pageFuture, statsFuture).thenApply(ignored -> {

                final Page<Map<String, Object>> page = pageFuture.getNow(Page.empty());
                return page.withStats(statsFuture.getNow(Page.Stats.ZERO));
            });

        } else {
            return pageFuture;
        }
    }

    private Function<Name, QueryPart> objectColumnResolver(final DSLContext context, final ObjectSchema schema) {

        return name -> {

            if(schema.metadataSchema().containsKey(name.first())) {
                final Name rest = name.withoutFirst();
                if (rest.isEmpty()) {
                    return DSL.field(DSL.name(name.first()));
                } else {
                    throw new UnsupportedOperationException("Query of this type is not supported");
                }
            } else {
                final Property prop = schema.requireProperty(name.first(), true);
                final Name rest = name.withoutFirst();
                if (rest.isEmpty()) {
                    return DSL.field(DSL.name(name.first()));
                } else {
                    return prop.typeOf().visit(new Use.Visitor.Defaulting<QueryPart>() {
                        @Override
                        public <T> QueryPart visitDefault(final Use<T> type) {

                            throw new UnsupportedOperationException("Query of this type is not supported");
                        }

                        @Override
                        public QueryPart visitStruct(final UseStruct type) {

                            // FIXME
                            return DSL.field(SQLUtils.columnName(name));
                        }

                        @Override
                        public QueryPart visitRef(final UseRef type) {

                            final Field<String> sourceId = DSL.field(DSL.name(name.first()), String.class);
                            if (rest.equals(ObjectSchema.ID_NAME)) {
                                return sourceId;
                            } else {
                                throw new UnsupportedOperationException("Query of this type is not supported");
                            }
                        }
                    });
                }
            }
        };
    }

    private Function<Name, QueryPart> indexColumnResolver(final DSLContext context, final ObjectSchema schema, final Index index) {

        // FIXME
        return name -> DSL.field(SQLUtils.columnName(name));
    }
//
//    @Override
//    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {
//
//        final Table<Record> table = DSL.table(objectTableName(schema));
//
//        return ImmutableList.of((count, token, stats) -> {
//
//            final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {
//
//                final Function<Name, QueryPart> columnResolver = objectColumnResolver(context, schema);
//                final SQLExpressionVisitor expressionVisitor = new SQLExpressionVisitor(columnResolver);
//
//                final List<GroupField> groupFields = group.entrySet().stream()
//                        .map(e -> expressionVisitor.field(e.getValue()).as(DSL.name(e.getKey())))
//                        .collect(Collectors.toList());
//
//                final List<Sort> sort = group.keySet().stream().map(Sort::asc).collect(Collectors.toList());
//
//                final List<OrderField<?>> orderFields = orderFields(sort);
//
//                final Condition condition = condition(context, schema, /* FIXME */ null, query);
//
//                log.debug("SQL aggregate condition {}", condition);
//
//                final List<SelectFieldOrAsterisk> selectFields = new ArrayList<>();
//                group.keySet().forEach(k -> {
//                    selectFields.add(DSL.field(DSL.name(k)));
//                });
//                aggregates.forEach((k, v) -> {
//                    selectFields.add(expressionVisitor.field(v).as(DSL.name(k)));
//                });
//
//                final SelectSeekStepN<Record> select = context.select(selectFields)
//                        .from(table).where(condition).groupBy(groupFields).orderBy(orderFields);
//
//                final SelectForUpdateStep<Record> seek;
//                if (token == null) {
//                    seek = select.limit(DSL.inline(count));
//                } else {
//                    final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
//                    seek = select.seek(values.toArray(new Object[0])).limit(DSL.inline(count));
//                }
//
//                return seek.fetchAsync().thenApply(results -> {
//
//                    final List<Map<String, Object>> objects = results.stream()
//                            .map(v -> {
//                                final Map<String, Object> value = new HashMap<>();
//                                group.keySet().forEach(k -> value.put(k, v.get(k)));
//                                aggregates.keySet().forEach(k -> value.put(k, v.get(k)));
//                                return value;
//                            })
//                            .collect(Collectors.toList());
//
//                    final Page.Token nextToken;
//                    if (objects.size() < count) {
//                        nextToken = null;
//                    } else {
//                        final Map<String, Object> last = objects.get(objects.size() - 1);
//                        nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, last);
//                    }
//
//                    return new Page<>(objects, nextToken);
//                });
//
//            });
//
//            return pageFuture;
//        });
//    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final Map<ObjectSchema, Set<String>> byId = new IdentityHashMap<>();

            private final Map<ObjectSchema, Set<IdVersion>> byIdVersion = new IdentityHashMap<>();

            @Override
            public ReadTransaction getObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                final Set<String> target = byId.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(id);
                return this;
            }

            @Override
            public ReadTransaction getObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                final Set<IdVersion> target = byIdVersion.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(new IdVersion(id, version));
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return withContext(initialContext -> initialContext.transactionResultAsync(config -> {

                    final DSLContext context = DSL.using(config);

                    final SortedMap<BatchResponse.RefKey, Map<String, Object>> results = new TreeMap<>();

                    for (final Map.Entry<ObjectSchema, Set<String>> entry : byId.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Set<String> ids = entry.getValue();
                        all(schema, context.select(selectFields(schema))
                                .from(objectTableName(schema))
                                .where(idField(schema).in(literals(ids)))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), v), v));
                    }

                    for (final Map.Entry<ObjectSchema, Set<IdVersion>> entry : byIdVersion.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Set<Row2<String, Long>> idVersions = entry.getValue().stream()
                                .map(v -> DSL.row(v.getId(), v.getVersion()))
                                .collect(Collectors.toSet());
                        all(schema, context.select(selectFields(schema))
                                .from(objectTableName(schema))
                                .where(DSL.row(idField(schema), versionField(schema))
                                        .in(idVersions))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), v), v));
                    }

                    return new BatchResponse(results);

                }));
            }
        };
    }

    private List<Field<?>> literals(final Collection<?> values) {

        return values.stream().map(DSL::inline).collect(Collectors.toList());
    }

    @Data
    private static class IdVersion {

        private final String id;

        private final long version;
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction();
    }

    protected class WriteTransaction implements DefaultLayerStorage.WriteTransaction {

        private final List<Function<DSLContext, BatchResponse>> steps = new ArrayList<>();

        @Override
        public StorageTraits storageTraits(final ReferableSchema schema) {

            return SQLStorage.this.storageTraits(schema);
        }

        @Override
        public void createObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(objectTableName(schema)))
                            .set(toRecord(schema, after))
                            .execute();

//                    final History history = schema.getHistory();
//                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                        context.insertInto(DSL.table(historyTableName(schema)))
//                                .set(toRecord(schema, after))
//                                .execute();
//                    }

                    return BatchResponse.fromRef(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if(SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        throw e;
                    }
                }
            });
        }

        @Override
        public void updateObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                Condition condition = idField(schema).eq(id);
                if (version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if (context.update(DSL.table(objectTableName(schema))).set(toRecord(schema, after))
                        .where(condition).limit(DSL.inline(1)).execute() != 1) {

                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

//                final History history = schema.getHistory();
//                if (history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                    context.insertInto(DSL.table(historyTableName(schema)))
//                            .set(toRecord(schema, after))
//                            .execute();
//                }

                return BatchResponse.fromRef(schema.getQualifiedName(), after);
            });
        }

        @Override
        public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                Condition condition = idField(schema).eq(id);
                if(version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if(context.deleteFrom(DSL.table(objectTableName(schema)))
                        .where(condition).limit(DSL.inline(1)).execute() != 1) {

                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

                return BatchResponse.empty();
            });
        }

//        public void createIndex(final ObjectSchema schema, final Index index, final String id, final Index.Key key, final Map<String, Object> projection) {
//
//            if (index.isMultiValue()) {
//
//                steps.add(context -> {
//
//                    context.insertInto(DSL.table(indexTableName(schema, index)))
//                            .set(toRecord(schema, index, key, projection))
//                            .execute();
//
//                    return BatchResponse.empty();
//                });
//
//            } else {
//                throw new UnsupportedOperationException();
//            }
//        }

//        @Override
//        public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {
//
//            // FIXME
//            throw new UnsupportedOperationException();
//        }
//
//        @Override
//        public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {
//
//            // FIXME
//            throw new UnsupportedOperationException();
//        }

        @Override
        public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(historyTableName(schema)))
                            .set(toRecord(schema, after))
                            .execute();

                    return BatchResponse.fromRef(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if(SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        throw e;
                    }
                }
            });
        }

        @Override
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            final SortedMap<BatchResponse.RefKey, Map<String, Object>> changes = new TreeMap<>();
            return withContext(initialContext -> initialContext.transactionAsync(config -> {

                final DSLContext context = DSL.using(config);

                steps.forEach(step -> changes.putAll(step.apply(context).getRefs()));

            }).thenApply(v -> new BatchResponse(changes)));
        }
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return SQLStorageTraits.INSTANCE;
    }

    @Override
    public Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return Collections.emptySet();
    }

    private <T> CompletableFuture<T> withContext(final Function<DSLContext, CompletionStage<T>> with) {

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            final DSLContext context = DSL.using(conn, dialect);
            final Connection conn2 = conn;
            return with.apply(context)
                    .toCompletableFuture()
                    .whenComplete((a, b) -> closeQuietly(conn2));
        } catch (final Exception e) {
            closeQuietly(conn);
            if(e instanceof RuntimeException) {
                throw (RuntimeException)e;
            } else {
                throw new IllegalStateException(e);
            }
        }
    }

    private static void closeQuietly(final Connection conn) {

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (final Exception e2) {
            throw new IllegalStateException(e2);
        }
    }

    private Field<String> idField(final ReferableSchema schema) {

        return DSL.field(DSL.name(ObjectSchema.ID), String.class);
    }

    private Field<Long> versionField(final ReferableSchema schema) {

        return DSL.field(DSL.name(ObjectSchema.VERSION), Long.class);
    }

    private org.jooq.Name objectTableName(final ReferableSchema schema) {

        return strategy.objectTableName(schema);
    }

    private org.jooq.Name historyTableName(final ReferableSchema schema) {

        return strategy.historyTableName(schema);
    }

    private org.jooq.Name indexTableName(final ReferableSchema schema, final Index index) {

        return strategy.indexTableName(schema, index);
    }

    private Map<String, Object> first(final ReferableSchema schema, final Result<Record> result) {

        if(result.isEmpty()) {
            return null;
        } else {
            return fromRecord(schema, result.iterator().next());
        }
    }

    private List<Map<String, Object>> all(final ReferableSchema schema, final Result<Record> result) {

        return result.stream()
                .map(v -> fromRecord(schema, v))
                .collect(Collectors.toList());
    }

    private Map<String, Object> fromRecord(final ReferableSchema schema, final Record record) {

        final Map<String, Object> data = record.intoMap();
        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(k, SQLUtils.fromSQLValue(v, data.get(k))));
        schema.getProperties().forEach((k, v) ->
                result.put(k, SQLUtils.fromSQLValue(v.typeOf(), data.get(k))));
        return result;
    }

    private Map<Field<?>, Object> toRecord(final ReferableSchema schema, final Map<String, Object> object) {

        final Map<Field<?>, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));
        schema.getProperties().forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v.typeOf(), object.get(k))));
        return result;
    }

    private Map<Field<?>, Object> toRecord(final ReferableSchema schema, final Index index, final Index.Key key, final Map<String, Object> object) {

        final Map<Field<?>, Object> result = new HashMap<>();
        index.projectionSchema(schema).forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));

        final List<Name> partitionNames = index.resolvePartitionNames();
        final List<Object> partition = key.getPartition();
        assert partitionNames.size() == partition.size();
        for(int i = 0; i != partition.size(); ++i) {
            final Name name = partitionNames.get(i);
            final Object value = partition.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(SQLUtils.columnName(name)), SQLUtils.toSQLValue(type, value));
        }
        final List<Sort> sortPaths = index.getSort();
        final List<Object> sort = key.getSort();
        assert sortPaths.size() == sort.size();
        for(int i = 0; i != sort.size(); ++i) {
            final Name name = sortPaths.get(i).getName();
            final Object value = sort.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(SQLUtils.columnName(name)), SQLUtils.toSQLValue(type, value));
        }
        return result;
    }
}
