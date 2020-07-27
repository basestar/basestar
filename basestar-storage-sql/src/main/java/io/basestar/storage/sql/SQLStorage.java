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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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
import io.basestar.storage.sql.mapper.RowMapper;
import io.basestar.storage.util.KeysetPagingUtils;
import io.basestar.storage.util.Pager;
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

    private Table<Record> objectTable(final ObjectSchema schema) {

        return DSL.table(objectTableName(schema));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, expand);
        final Table<Record> table = rowMapper.joined(DSL.table(objectTableName(schema)), this::objectTable);
        return withContext(context -> context.select(rowMapper.select())
                .from(table)
                .where(idField(schema).eq(id))
                .limit(1).fetchAsync().thenApply(rowMapper::first));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, expand);
        final Table<Record> table = rowMapper.joined(DSL.table(historyTableName(schema)), this::objectTable);
        return withContext(context -> context.select(rowMapper.select())
                    .from(table)
                    .where(idField(schema).eq(id)
                            .and(versionField(schema).eq(version)))
                    .limit(1).fetchAsync().thenApply(rowMapper::first));
    }

    private Index bestIndex(final ObjectSchema schema, final Expression expression) {

        final Map<Name, Range<Object>> ranges = expression.visit(new RangeVisitor());

        Index best = null;
        // Only multi-value indexes need to be matched
        for(final Index index : schema.getIndexes().values()) {
            if(index.isMultiValue()) {
                final Set<Name> names = index.getMultiValuePaths();
                if (ranges.keySet().containsAll(names)) {
                    best = index;
                }
            }
        }
        return best;
    }

    // Shouldn't need to do this - convert disjoint multi-value index queries into a union instead
    private boolean requiresDisjunction(final ObjectSchema schema, final Set<Expression> expressions) {

        return expressions.stream().map(v -> bestIndex(schema, v)).anyMatch(Objects::nonNull);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());

        final Set<Expression> disjunction = new DisjunctionVisitor().visit(bound);
        final Set<Expression> resolvedDisjunction;
        if(requiresDisjunction(schema, disjunction)) {
            resolvedDisjunction = disjunction;
        } else {
            resolvedDisjunction = ImmutableSet.of(bound);
        }

        final List<Pager.Source<Map<String, Object>>> sources = new ArrayList<>();
        for(final Expression conjunction : resolvedDisjunction) {

            final Set<Name> effectiveExpand = Sets.union(expand, schema.requiredExpand(conjunction.names()));

            final Index index = bestIndex(schema, conjunction);

            final List<OrderField<?>> orderFields = sort.stream()
                    .map(v -> {
                        final Field<?> field = DSL.field(SQLUtils.columnName(v.getName()));
                        return v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc();
                    })
                    .collect(Collectors.toList());

            sources.add((count, token, stats) ->
                    withContext(context -> {

                        final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, effectiveExpand);

                        final Table<Record> rootTable;
                        final Function<Name, QueryPart> columnResolver;
                        if(index == null) {
                            rootTable = DSL.table(objectTableName(schema));
                            columnResolver = rowMapper::resolve;

                        } else {
                            rootTable = DSL.table(indexTableName(schema, index));
                            columnResolver = indexColumnResolver(context, schema, index);
                        }

                        final Set<Name> names = conjunction.names();
                        final Table<Record> table = rowMapper.joined(rootTable, s -> DSL.table(objectTableName(s)));

                        final Condition condition = new SQLExpressionVisitor(columnResolver).condition(conjunction);

                        final SelectSeekStepN<Record> select = context.select(rowMapper.select())
                                .from(table).where(condition).orderBy(orderFields);

                        log.info("SQL: {}", select);

                        final SelectForUpdateStep<Record> seek;
                        if(token == null) {
                            seek = select.limit(DSL.inline(count));
                        } else {
                            final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
                            seek = select.seek(values.toArray(new Object[0])).limit(DSL.inline(count));
                        }

                        return seek.fetchAsync().thenApply(results -> {

                                    final List<Map<String, Object>> objects = rowMapper.all(results);

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

//    private Table<Record> objectTable(final ObjectSchema schema, final Set<Name> expand) {
//
//        return objectTable(schema, Name.of(), expand);
//    }

//    private Table<Record> objectTable(final ObjectSchema schema, final Name name, final Set<Name> expand) {
//
//        final Table<Record> rootTable = DSL.table(objectTableName(schema)).as(tableName(name));
//        return join(schema, rootTable, expand);
//    }

//    private Table<Record> historyTable(final ObjectSchema schema, final Set<Name> expand) {
//
//        return historyTable(schema, Name.of(), expand);
//    }
//
//    private Table<Record> historyTable(final ObjectSchema schema, final Name name, final Set<Name> expand) {
//
//        final Table<Record> rootTable = DSL.table(historyTableName(schema)).as(tableName(name));
//        return join(schema, rootTable, expand);
//    }

//    private Table<Record> join(final InstanceSchema schema, final Table<Record> sourceTable, final Set<Name> expand) {
//
//        return join(schema, sourceTable, expand, Name.of());
//    }
//
//    private Table<Record> join(final InstanceSchema schema, final Table<Record> sourceTable, final Set<Name> expand, final Name name) {
//
//        Table<Record> table = sourceTable;
//        final Map<String, Set<Name>> branches = Name.branch(expand);
//        for(final Map.Entry<String, Property> entry : schema.getProperties().entrySet()) {
//            final String propertyName = entry.getKey();
//            final Property property = entry.getValue();
//            final Set<Name> branch = branches.get(propertyName);
//            if(branch != null) {
//                table = join(table, property.getType(), branch, name.with(propertyName));
//            }
//        }
//        return table;
//    }
//
//    private Table<Record> join(final Table<Record> sourceTable, final Use<?> type, final Set<Name> expand, final Name name) {
//
//        return type.visit(new Use.Visitor.Defaulting<Table<Record>>() {
//
//            @Override
//            public Table<Record> visitDefault(final Use<?> type) {
//
//                return sourceTable;
//            }
//
//            @Override
//            public <T> Table<Record> visitCollection(final UseCollection<T, ? extends Collection<T>> type) {
//
//                throw new UnsupportedOperationException("Joins through collections are not supported yet");
//            }
//
//            @Override
//            public <T> Table<Record> visitMap(final UseMap<T> type) {
//
//                throw new UnsupportedOperationException("Joins through maps are not supported yet");
//            }
//
//            @Override
//            public Table<Record> visitStruct(final UseStruct type) {
//
//                return join(type.getSchema(), sourceTable, expand, name);
//            }
//
//            @Override
//            public Table<Record> visitObject(final UseObject type) {
//
//                final Table<Record> targetTable = objectTable(type.getSchema(), name, expand);
//                final Field<String> targetId = DSL.field(DSL.name(targetTable.getUnqualifiedName(), DSL.name(Reserved.ID)), String.class);
//                final Field<String> sourceId = DSL.field(DSL.name(sourceTable.getUnqualifiedName(), columnName(name)), String.class);
//                return sourceTable.leftJoin(targetTable).on(targetId.eq(sourceId));
//            }
//        });
//    }

    private org.jooq.Name tableName(final Name name) {

        return DSL.name(Reserved.PREFIX + name.toString(Reserved.PREFIX));
    }

    private org.jooq.Name columnName(final Name name) {

        return DSL.name(name.toArray());
    }

    private Name instanceFieldName(final InstanceSchema schema, final Name name) {

        final Name first = Name.of(name.first());
        final Name rest = name.withoutFirst();
        if(rest.isEmpty() && schema.metadataSchema().containsKey(name.first())) {
            return first;
        }
        final Property prop = schema.requireProperty(name.first(), true);
        if(rest.isEmpty()) {
            return first;
        } else {
            return prop.getType().visit(new Use.Visitor.Defaulting<Name>() {
                @Override
                public Name visitDefault(final Use<?> type) {

                    throw new UnsupportedOperationException("Query of this type is not supported");
                }

                @Override
                public Name visitStruct(final UseStruct type) {

                    return first.with(instanceFieldName(type.getSchema(), rest));
                }

                @Override
                public Name visitObject(final UseObject type) {

                    if (rest.equals(Reserved.ID_NAME)) {
                        return first;
                    } else {
                        return Name.of(Reserved.PREFIX).with(first.with(instanceFieldName(type.getSchema(), rest)));
                    }
                }
            });
        }
    }

    private Function<Name, QueryPart> indexColumnResolver(final DSLContext context, final ObjectSchema schema, final Index index) {

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

            private final Map<ObjectSchema, Set<Name>> expandId = new IdentityHashMap<>();

            private final Map<ObjectSchema, Set<IdVersion>> byIdVersion = new IdentityHashMap<>();

            private final Map<ObjectSchema, Set<Name>> expandIdVersion = new IdentityHashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

                final Set<String> target = byId.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(id);
                expandId.computeIfAbsent(schema, ignored -> new HashSet<>()).addAll(expand);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

                final Set<IdVersion> target = byIdVersion.computeIfAbsent(schema, ignored -> new HashSet<>());
                target.add(new IdVersion(id, version));
                expandIdVersion.computeIfAbsent(schema, ignored -> new HashSet<>()).addAll(expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return withContext(initialContext -> initialContext.transactionResultAsync(config -> {

                    final DSLContext context = DSL.using(config);

                    final SortedMap<BatchResponse.Key, Map<String, Object>> results = new TreeMap<>();

                    for (final Map.Entry<ObjectSchema, Set<String>> entry : byId.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Set<Name> expand = expandId.get(schema);
                        final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, expand);
                        final Set<String> ids = entry.getValue();
                        rowMapper.all(context.select(rowMapper.select())
                                .from(rowMapper.joined(objectTable(schema), SQLStorage.this::objectTable))
                                .where(idField(schema).in(ids))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.Key.from(schema.getQualifiedName(), v), v));
                    }

                    for (final Map.Entry<ObjectSchema, Set<IdVersion>> entry : byIdVersion.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Set<Name> expand = expandIdVersion.get(schema);
                        final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, expand);
                        final Set<Row2<String, Long>> idVersions = entry.getValue().stream()
                                .map(v -> DSL.row(v.getId(), v.getVersion()))
                                .collect(Collectors.toSet());
                        rowMapper.all(context.select(rowMapper.select())
                                .from(rowMapper.joined(objectTable(schema), SQLStorage.this::objectTable))
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

            final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, schema.getExpand());
            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(objectTableName(schema)))
                            .set(rowMapper.toRecord(after))
                            .execute();

                    final History history = schema.getHistory();
                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                        context.insertInto(DSL.table(historyTableName(schema)))
                                .set(rowMapper.toRecord(after))
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

            final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, schema.getExpand());
            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                Condition condition = idField(schema).eq(id);
                if(version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if(context.update(DSL.table(objectTableName(schema))).set(rowMapper.toRecord(after))
                        .where(condition).limit(1).execute() != 1) {

                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

                final History history = schema.getHistory();
                if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
                    context.insertInto(DSL.table(historyTableName(schema)))
                            .set(rowMapper.toRecord(after))
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

            final RowMapper<Map<String, Object>> rowMapper = rowMapper(schema, schema.getExpand());
            steps.add(context -> {

                try {

                    context.insertInto(DSL.table(historyTableName(schema)))
                            .set(rowMapper.toRecord(after))
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

    private RowMapper<Map<String, Object>> rowMapper(final ObjectSchema schema, final Set<Name> expand) {

        // FIXME: cache these
        return strategy.rowMapper(schema, expand);
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return SQLStorageTraits.INSTANCE;
    }

    @Override
    public Set<Name> supportedExpand(final ObjectSchema schema, final Set<Name> expand) {

        // Supports any expand
        return expand;
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



//    private List<Map<String, Object>> all(final ObjectSchema schema, final Result<Record> result) {
//
//        return result.stream()
//                .map(v -> fromRecord(schema, v))
//                .collect(Collectors.toList());
//    }

//    private Map<String, Object> fromRecord(final ObjectSchema schema, final Record record) {
//
//        final Map<String, Object> data = record.intoMap();
//        final Map<String, Object> result = new HashMap<>();
//        schema.metadataSchema().forEach((k, v) ->
//                result.put(k, SQLUtils.fromSQLValue(v, data.get(k))));
//        schema.getProperties().forEach((k, v) ->
//                result.put(k, SQLUtils.fromSQLValue(v.getType(), data.get(k))));
//        return result;
//    }
//
//    private Map<Field<?>, Object> toRecord(final ObjectSchema schema, final Map<String, Object> object) {
//
//        final Map<Field<?>, Object> result = new HashMap<>();
//        schema.metadataSchema().forEach((k, v) ->
//                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));
//        schema.getProperties().forEach((k, v) ->
//            result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v.getType(), object.get(k))));
//        return result;
//    }

    private Map<Field<?>, Object> toRecord(final ObjectSchema schema, final Index index, final Index.Key key, final Map<String, Object> object) {

        final Map<Field<?>, Object> result = new HashMap<>();
        index.projectionSchema(schema).forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), SQLUtils.toSQLValue(v, object.get(k))));

        final List<Name> partitionNames = index.resolvePartitionPaths();
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
