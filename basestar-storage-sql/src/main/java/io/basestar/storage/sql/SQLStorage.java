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
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseObject;
import io.basestar.schema.use.UseStruct;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.DisjunctionVisitor;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.storage.util.Pager;
import io.basestar.util.Nullsafe;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Sort;
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
import java.util.stream.Stream;

@Slf4j
public class SQLStorage implements Storage.WithWriteIndex, Storage.WithWriteHistory {

    private final DataSource dataSource;

    private final SQLDialect dialect;

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
        this.dialect = builder.dialect;
        this.strategy = builder.strategy;
        this.eventStrategy = Nullsafe.option(builder.eventStrategy, EventStrategy.EMIT);
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

        return Stream.concat(schema.metadataSchema().keySet().stream(), schema.getProperties().keySet().stream())
                .map(name -> DSL.field(DSL.name(name)).as(DSL.name(name)))
                .collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return withContext(context -> context.select(selectFields(schema))
                .from(DSL.table(objectTableName(schema)))
                .where(idField(schema).eq(id))
                .limit(1).fetchAsync().thenApply(result -> first(schema, result)));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return withContext(context -> context.select(selectFields(schema))
                    .from(DSL.table(historyTableName(schema)))
                    .where(idField(schema).eq(id)
                            .and(versionField(schema).eq(version)))
                    .limit(1).fetchAsync().thenApply(result -> first(schema, result)));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        final Expression bound = query.bind(Context.init());

        final Set<Expression> disjunction = new DisjunctionVisitor().visit(bound);

        final List<Pager.Source<Map<String, Object>>> sources = new ArrayList<>();

        for(final Expression conjunction : disjunction) {
            final Map<io.basestar.util.Name, Range<Object>> ranges = conjunction.visit(new RangeVisitor());

            Index best = null;
            // Only multi-value indexes need to be matched
            for(final Index index : schema.getIndexes().values()) {
                if(index.isMultiValue()) {
                    final Set<io.basestar.util.Name> names = index.getMultiValuePaths();
                    if (ranges.keySet().containsAll(names)) {
                        best = index;
                    }
                }
            }

            final List<OrderField<?>> orderFields = sort.stream()
                    .map(v -> {
                        final Field<?> field = DSL.field(SQLUtils.columnName(v.getName()));
                        return v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc();
                    })
                    .collect(Collectors.toList());

            final Index index = best;
            sources.add((count, token, stats) ->
                    withContext(context -> {

                        final Table<Record> table;
                        final Function<io.basestar.util.Name, QueryPart> columnResolver;
                        if(index == null) {
                            table = DSL.table(objectTableName(schema));
                            columnResolver = objectColumnResolver(context, schema);
                        } else {
                            table = DSL.table(indexTableName(schema, index));
                            columnResolver = indexColumnResolver(context, schema, index);
                        }
                        final Condition condition = new SQLExpressionVisitor(columnResolver).condition(conjunction);

                        log.info("SQL condition {}", condition);

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

                                    final PagingToken nextToken;
                                    if(objects.size() < count) {
                                        nextToken = null;
                                    } else {
                                        final Map<String, Object> last = objects.get(objects.size() - 1);
                                        nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, last);
                                    }

                                    return new PagedList<>(objects, nextToken);
                                });

                    }));
        }

        return sources;
    }

    private Function<io.basestar.util.Name, QueryPart> objectColumnResolver(final DSLContext context, final ObjectSchema schema) {

        return name -> {

            final Property prop = schema.requireProperty(name.first(), true);
            final io.basestar.util.Name rest = name.withoutFirst();
            if(rest.isEmpty()) {
                return DSL.field(DSL.name(name.first()));
            } else {
                return prop.getType().visit(new Use.Visitor.Defaulting<QueryPart>() {
                    @Override
                    public QueryPart visitDefault(final Use<?> type) {

                        throw new UnsupportedOperationException("Query of this type is not supported");
                    }

                    @Override
                    public QueryPart visitStruct(final UseStruct type) {

                        // FIXME
                        return DSL.field(SQLUtils.columnName(name));
                    }

                    @Override
                    public QueryPart visitObject(final UseObject type) {

                        final Field<String> sourceId = DSL.field(DSL.name(name.first()), String.class);
                        if(rest.equals(Reserved.ID_NAME)) {
                            return sourceId;
                        } else {
                            throw new UnsupportedOperationException("Query of this type is not supported");
                        }
                    }
                });
            }
        };
    }

    private Function<io.basestar.util.Name, QueryPart> indexColumnResolver(final DSLContext context, final ObjectSchema schema, final Index index) {

        // FIXME
        return name -> DSL.field(SQLUtils.columnName(name));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            private final Map<ObjectSchema, Set<String>> byId = new IdentityHashMap<>();

            private final Map<ObjectSchema, Set<IdVersion>> byIdVersion = new IdentityHashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final Set<String> target = byId.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(id);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                final Set<IdVersion> target = byIdVersion.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(new IdVersion(id, version));
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return withContext(initialContext -> initialContext.transactionResultAsync(config -> {

                    final DSLContext context = DSL.using(config);

                    final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();

                    for (final Map.Entry<ObjectSchema, Set<String>> entry : byId.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Set<String> ids = entry.getValue();
                        all(schema, context.select(selectFields(schema))
                                .from(objectTableName(schema))
                                .where(idField(schema).in(ids))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.Key.from(schema.getQualifiedName(), v), v));
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
                                .forEach(v -> results.put(BatchResponse.Key.from(schema.getQualifiedName(), v), v));
                    }

                    return new BatchResponse.Basic(results);

                }));
            }
        };
    }

    @Data
    private static class IdVersion {

        private final String id;

        private final long version;
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return new WriteTransaction();
    }

    protected class WriteTransaction implements WithWriteIndex.WriteTransaction, WithWriteHistory.WriteTransaction {

        private final List<Function<DSLContext, BatchResponse>> steps = new ArrayList<>();

        @Override
        public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(objectTableName(schema)))
                            .set(toRecord(schema, after))
                            .execute();

                    final History history = schema.getHistory();
                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                        context.insertInto(DSL.table(historyTableName(schema)))
                                .set(toRecord(schema, after))
                                .execute();
                    }

                    return BatchResponse.single(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if(SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        throw e;
                    }
                }
            });

            final StorageTraits traits = storageTraits(schema);
            for(final Index index : schema.getIndexes().values()) {
                final Consistency best = traits.getMultiValueIndexConsistency();
                if (index.isMultiValue() && !index.getConsistency(best).isAsync()) {
                    for(final Map.Entry<Index.Key, Map<String, Object>> entry : index.readValues(after).entrySet()) {
                        createIndex(schema, index, id, 1L, entry.getKey(), entry.getValue());
                    }
                }
            }

            return this;
        }

        @Override
        public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                Condition condition = idField(schema).eq(id);
                if(version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if(context.update(DSL.table(objectTableName(schema))).set(toRecord(schema, after))
                        .where(condition).limit(1).execute() != 1) {

                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

                final History history = schema.getHistory();
                if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                    context.insertInto(DSL.table(historyTableName(schema)))
                            .set(toRecord(schema, after))
                            .execute();
                }

                return BatchResponse.single(schema.getQualifiedName(), after);
            });

            // FIXME: not writing non-async indexes

            return this;
        }

        @Override
        public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                Condition condition = idField(schema).eq(id);
                if(version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if(context.deleteFrom(DSL.table(objectTableName(schema)))
                        .where(condition).limit(1).execute() != 1) {

                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

                return BatchResponse.empty();
            });

            // FIXME: not writing non-async indexes

            return this;
        }

        @Override
        public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            if (index.isMultiValue()) {

                steps.add(context -> {

                    context.insertInto(DSL.table(indexTableName(schema, index)))
                            .set(toRecord(schema, index, key, projection))
                            .execute();

                    return BatchResponse.empty();
                });

                return this;

            } else {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

            // FIXME
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

            // FIXME
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(historyTableName(schema)))
                            .set(toRecord(schema, after))
                            .execute();

                    return BatchResponse.single(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if(SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        throw e;
                    }
                }
            });

            return this;
        }

        @Override
        public CompletableFuture<BatchResponse> write() {

            final SortedMap<BatchResponse.Key, Map<String, Object>> changes = new TreeMap<>();
            return withContext(initialContext -> initialContext.transactionAsync(config -> {

                final DSLContext context = DSL.using(config);

                steps.forEach(step -> changes.putAll(step.apply(context)));

            }).thenApply(v -> new BatchResponse.Basic(changes)));
        }
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return SQLStorageTraits.INSTANCE;
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

    private Field<String> idField(final ObjectSchema schema) {

        return DSL.field(DSL.name(Reserved.ID), String.class);
    }

    private Field<Long> versionField(final ObjectSchema schema) {

        return DSL.field(DSL.name(Reserved.VERSION), Long.class);
    }

    private org.jooq.Name objectTableName(final ObjectSchema schema) {

        return strategy.objectTableName(schema);
    }

    private org.jooq.Name historyTableName(final ObjectSchema schema) {

        return strategy.historyTableName(schema);
    }

    private org.jooq.Name indexTableName(final ObjectSchema schema, final Index index) {

        return strategy.indexTableName(schema, index);
    }

    private Map<String, Object> first(final ObjectSchema schema, final Result<Record> result) {

        if(result.isEmpty()) {
            return null;
        } else {
            return fromRecord(schema, result.iterator().next());
        }
    }

    private List<Map<String, Object>> all(final ObjectSchema schema, final Result<Record> result) {

        return result.stream()
                .map(v -> fromRecord(schema, v))
                .collect(Collectors.toList());
    }

    private Map<String, Object> fromRecord(final ObjectSchema schema, final Record record) {

        final Map<String, Object> data = record.intoMap();
        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(k, SQLUtils.fromSQLValue(v, data.get(k))));
        schema.getProperties().forEach((k, v) ->
                result.put(k, SQLUtils.fromSQLValue(v.getType(), data.get(k))));
        return result;
    }

    private Map<Field<?>, Object> toRecord(final ObjectSchema schema, final Map<String, Object> object) {

        final Map<Field<?>, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));
        schema.getProperties().forEach((k, v) ->
            result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v.getType(), object.get(k))));
        return result;
    }

    private Map<Field<?>, Object> toRecord(final ObjectSchema schema, final Index index, final Index.Key key, final Map<String, Object> object) {

        final Map<Field<?>, Object> result = new HashMap<>();
        index.projectionSchema(schema).forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));

        final List<io.basestar.util.Name> partitionNames = index.resolvePartitionPaths();
        final List<Object> partition = key.getPartition();
        assert partitionNames.size() == partition.size();
        for(int i = 0; i != partition.size(); ++i) {
            final io.basestar.util.Name name = partitionNames.get(i);
            final Object value = partition.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(SQLUtils.columnName(name)), SQLUtils.toSQLValue(type, value));
        }
        final List<Sort> sortPaths = index.getSort();
        final List<Object> sort = key.getSort();
        assert sortPaths.size() == sort.size();
        for(int i = 0; i != sort.size(); ++i) {
            final io.basestar.util.Name name = sortPaths.get(i).getName();
            final Object value = sort.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(SQLUtils.columnName(name)), SQLUtils.toSQLValue(type, value));
        }
        return result;
    }
}
