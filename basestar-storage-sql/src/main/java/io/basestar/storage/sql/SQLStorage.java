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
import io.basestar.expression.visitor.DisjunctionVisitor;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.Use;
import io.basestar.schema.use.UseRef;
import io.basestar.schema.use.UseStruct;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DefaultLayerStorage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
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
import org.jooq.conf.Settings;
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

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

    private List<Table<?>> tables;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
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

    private Optional<org.jooq.Field<?>> resolveField(final Table<?> table, final String name) {

        if (strategy.useMetadata()) {
            final String normalizedName = normalizeColumnName(name);
            final List<org.jooq.Field<?>> fields = Arrays.stream(table.fields())
                    .filter(f -> normalizeColumnName(f.getName()).equals(normalizedName))
                    .collect(Collectors.toList());
            if (fields.isEmpty()) {
                return Optional.empty();
            } else if (fields.size() > 1) {
                throw new IllegalStateException("Field " + name + " is ambiguous in " + table.getQualifiedName());
            } else {
                return Optional.of(fields.get(0));
            }
        } else {
            return Optional.of(DSL.field(DSL.name(name)));
        }
    }

    private String normalizeColumnName(final String name) {

        return name.replaceAll("[^\\w\\d]", "").toLowerCase();
    }

    private List<SelectFieldOrAsterisk> selectFields(final LinkableSchema schema, final Table<?> table) {

        final SQLDialect dialect = strategy.dialect();

        final List<SelectFieldOrAsterisk> fields = new ArrayList<>();
        schema.metadataSchema().forEach((name, type) -> {
            final Optional<Field<?>> opt = resolveField(table, name);
            if (opt.isPresent()) {
                fields.add(dialect.selectField(opt.get(), type).as(DSL.name(name)));
            } else {
                dialect.missingMetadataValue(schema, name).ifPresent(field -> {
                    fields.add(dialect.selectField(field, type).as(DSL.name(name)));
                });
            }
        });
        schema.getProperties().forEach((name, prop) -> {
            resolveField(table, name).ifPresent(field -> {
                fields.add(dialect.selectField(field, prop.typeOf()).as(DSL.name(name)));
            });
        });
        return fields;
    }

    @Override
    public Pager<Map<String, Object>> queryView(final Consistency consistency, final ViewSchema schema, final Expression query,
                                                final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());
        return (stats, token, count) -> queryImpl(schema, null, bound, sort, expand, count, token, stats);
    }

    @Override
    public Pager<Map<String, Object>> queryObject(final Consistency consistency, final ObjectSchema schema, final Expression query,
                                                  final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());

        final Set<Expression> disjunction = new DisjunctionVisitor().visit(bound);

        final Map<String, Pager<Map<String, Object>>> sources = new HashMap<>();

        // FIXME: use a union instead of doing this manually
        for (final Expression conjunction : disjunction) {
            final Map<Name, Range<Object>> ranges = conjunction.visit(new RangeVisitor());

            Index best = null;
            // Only multi-value indexes need to be matched separately
            for (final Index index : schema.getIndexes().values()) {
                if (index.isMultiValue()) {
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

    private Condition condition(final DSLContext context, final LinkableSchema schema, final Index index, final Expression expression) {

        final SQLDialect dialect = strategy.dialect();

        final Function<Name, QueryPart> columnResolver;
        if (index == null) {
            columnResolver = objectColumnResolver(context, schema);
        } else {
            columnResolver = indexColumnResolver(context, (ReferableSchema) schema, index);
        }
        final InferenceContext inferenceContext = InferenceContext.from(schema);
        return new SQLExpressionVisitor(dialect, inferenceContext, columnResolver).condition(expression);
    }

    private List<OrderField<?>> orderFields(final List<Sort> sort) {

        final SQLDialect dialect = strategy.dialect();

        return sort.stream()
                .map(v -> {
                    final Field<?> field = DSL.field(dialect.columnName(v.getName()));
                    return v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc();
                })
                .collect(Collectors.toList());
    }

    private Table<?> resolveTable(final DSLContext context, final Table<Record> table) {

        if (strategy.useMetadata()) {
            // FIXME: cache this for a short time rather than forever
            synchronized (this) {
                if (tables == null) {
                    tables = context.meta().getTables();
                }
                return tables.stream().filter(v -> nameMatch(v.getQualifiedName(), table.getQualifiedName()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("Table " + table.getQualifiedName() + " not found"));
            }
        } else {
            return table;
        }
    }

    private boolean nameMatch(final org.jooq.Name name, final org.jooq.Name matchName) {

        final org.jooq.Name[] nameParts = name.parts();
        final org.jooq.Name[] matchNameParts = matchName.parts();
        for (int i = 0; i != Math.min(nameParts.length, matchNameParts.length); ++i) {
            if (i < nameParts.length) {
                if (!nameParts[nameParts.length - (i + 1)].equalsIgnoreCase(matchNameParts[matchNameParts.length - (i + 1)])) {
                    return false;
                }
            }
        }
        return true;
    }

    private CompletableFuture<Page<Map<String, Object>>> queryImpl(final LinkableSchema schema, final Index index,
                                                                   final Expression expression, final List<Sort> sort,
                                                                   final Set<Name> expand, final int count,
                                                                   final Page.Token token, final Set<Page.Stat> stats) {

        final List<OrderField<?>> orderFields = orderFields(sort);

        final Table<Record> rawTable;
        if (index == null) {
            rawTable = DSL.table(schemaTableName(schema));
        } else {
            rawTable = DSL.table(indexTableName((ReferableSchema) schema, index));
        }

        final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {

            final Table<?> table = resolveTable(context, rawTable);

            final Condition condition = condition(context, schema, index, expression);

            log.debug("SQL condition {}", condition);

            final SelectSeekStepN<Record> select = context.select(selectFields(schema, table))
                    .from(rawTable.getQualifiedName()).where(condition).orderBy(orderFields);

            final SelectForUpdateStep<Record> seek;
            if (token == null) {
                seek = select.limit(DSL.inline(count));
            } else {
                final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
                seek = select.seek(values.stream().map(DSL::inline).toArray(Object[]::new)).limit(DSL.inline(count));
            }

            return seek.fetchAsync().thenApply(results -> {

                final List<Map<String, Object>> objects = all(schema, results);

                final Page.Token nextToken;
                if (objects.size() < count) {
                    nextToken = null;
                } else {
                    final Map<String, Object> last = objects.get(objects.size() - 1);
                    nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, last);
                }

                return new Page<>(objects, nextToken);
            });

        });

        if (stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {

            // Runs in parallel with query
            final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {

                final Condition condition = condition(context, schema, index, expression);

                return context.select(DSL.count().as(COUNT_AS)).from(rawTable.getQualifiedName()).where(condition).fetchAsync().thenApply(results -> {

                    if (results.isEmpty()) {
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

    private Function<Name, QueryPart> objectColumnResolver(final DSLContext context, final LinkableSchema schema) {

        final SQLDialect dialect = strategy.dialect();

        return name -> {

            if (schema.metadataSchema().containsKey(name.first())) {
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
                            return DSL.field(dialect.columnName(name));
                        }

                        @Override
                        public QueryPart visitRef(final UseRef type) {

                            return dialect.refIdField(type, name);
                        }
                    });
                }
            }
        };
    }

    private Function<Name, QueryPart> indexColumnResolver(final DSLContext context, final ReferableSchema schema, final Index index) {

        final SQLDialect dialect = strategy.dialect();

        // FIXME
        return name -> DSL.field(dialect.columnName(name));
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
                        final Table<?> table = resolveTable(context, DSL.table(schemaTableName(schema)));
                        final Set<String> ids = entry.getValue();
                        all(schema, context.select(selectFields(schema, table))
                                .from(table.getQualifiedName())
                                .where(idField(schema).in(literals(ids)))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), v), v));
                    }

                    for (final Map.Entry<ObjectSchema, Set<IdVersion>> entry : byIdVersion.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final Table<?> table = resolveTable(context, DSL.table(historyTableName(schema)));
                        final Set<Row2<String, Long>> idVersions = entry.getValue().stream()
                                .map(v -> DSL.row(v.getId(), v.getVersion()))
                                .collect(Collectors.toSet());
                        all(schema, context.select(selectFields(schema, table))
                                .from(table.getQualifiedName())
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
                            .set(toRecord(context, schema, after))
                            .execute();

//                    final History history = schema.getHistory();
//                    if(history.isEnabled() && history.getConsistency(Consistency.ATOMIC).isStronger(Consistency.ASYNC)) {
//                        context.insertInto(DSL.table(historyTableName(schema)))
//                                .set(toRecord(schema, after))
//                                .execute();
//                    }

                    return BatchResponse.fromRef(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if (SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
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

                if (context.update(DSL.table(objectTableName(schema))).set(toRecord(context, schema, after))
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
                if (version != null) {
                    condition = condition.and(versionField(schema).eq(version));
                }

                if (context.deleteFrom(DSL.table(objectTableName(schema)))
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
                            .set(toRecord(context, schema, after))
                            .execute();

                    return BatchResponse.fromRef(schema.getQualifiedName(), after);

                } catch (final DataAccessException e) {
                    if (SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    } else {
                        throw e;
                    }
                }
            });
        }

        @Override
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            // Fixme: should support materialized views
            if (schema instanceof ReferableSchema || schema instanceof ViewSchema && ((ViewSchema) schema).isMaterialized()) {
                steps.add(context -> {

                    try {

                        context.insertInto(DSL.table(schemaTableName(schema)))
                                .set(toRecord(context, schema, after))
                                .execute();

                        return BatchResponse.empty();

                    } catch (final DataAccessException e) {
                        if (SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION.equals(e.sqlStateClass())) {
                            throw new ObjectExistsException(schema.getQualifiedName(), Instance.getId(after));
                        } else {
                            throw e;
                        }
                    }
                });
            }

            return this;
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

        final SQLDialect dialect = strategy.dialect();

        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            final DSLContext context = DSL.using(conn, dialect.dmlDialect(), new Settings().withStatementType(strategy.statementType()));
            final Connection conn2 = conn;
            return with.apply(context)
                    .toCompletableFuture()
                    .whenComplete((a, b) -> closeQuietly(conn2));
        } catch (final Exception e) {
            closeQuietly(conn);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
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

    private org.jooq.Name schemaTableName(final LinkableSchema schema) {

        if (schema instanceof ReferableSchema) {
            return strategy.objectTableName((ReferableSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return strategy.viewName((ViewSchema) schema);
        } else {
            throw new IllegalStateException("Cannot determine name for schema " + schema);
        }
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

    private Map<String, Object> first(final LinkableSchema schema, final Result<Record> result) {

        if (result.isEmpty()) {
            return null;
        } else {
            return fromRecord(schema, result.iterator().next());
        }
    }

    private List<Map<String, Object>> all(final LinkableSchema schema, final Result<Record> result) {

        return result.stream()
                .map(v -> fromRecord(schema, v))
                .collect(Collectors.toList());
    }

    private Map<String, Object> fromRecord(final LinkableSchema schema, final Record record) {

        final SQLDialect dialect = strategy.dialect();

        final Map<String, Object> data = record.intoMap();
        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(k, dialect.fromSQLValue(v, data.get(k))));
        schema.getProperties().forEach((k, v) ->
                result.put(k, dialect.fromSQLValue(v.typeOf(), data.get(k))));

        if (Instance.getSchema(result) == null) {
            Instance.setSchema(result, schema.getQualifiedName());
        }
        // Make sure view records have a valid __key field
        if (schema instanceof ViewSchema) {
            if (result.get(ViewSchema.ID) == null) {
                result.put(ViewSchema.ID, ((ViewSchema) schema).createId(result));
            }
        }
        return result;
    }

    private Map<Field<?>, Object> toRecord(final DSLContext context, final LinkableSchema schema, final Map<String, Object> object) {

        final SQLDialect dialect = strategy.dialect();

        final Table<?> table = resolveTable(context, DSL.table(schemaTableName(schema)));

        final Map<Field<?>, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) -> {
            resolveField(table, k).ifPresent(field -> {
                result.put(field, dialect.toSQLValue(v, object.get(k)));
            });
        });
        schema.getProperties().forEach((k, v) -> {
            resolveField(table, k).ifPresent(field -> {
                result.put(field, dialect.toSQLValue(v.typeOf(), object.get(k)));
            });
        });

        return result.entrySet().stream().filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<Field<?>, Object> toRecord(final ReferableSchema schema, final Index index, final Index.Key key, final Map<String, Object> object) {

        final SQLDialect dialect = strategy.dialect();

        final Map<Field<?>, Object> result = new HashMap<>();
        index.projectionSchema(schema).forEach((k, v) ->
                result.put(DSL.field(DSL.name(k)), dialect.toSQLValue(v, object.get(k))));

        final List<Name> partitionNames = index.resolvePartitionNames();
        final List<Object> partition = key.getPartition();
        assert partitionNames.size() == partition.size();
        for (int i = 0; i != partition.size(); ++i) {
            final Name name = partitionNames.get(i);
            final Object value = partition.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(dialect.columnName(name)), dialect.toSQLValue(type, value));
        }
        final List<Sort> sortPaths = index.getSort();
        final List<Object> sort = key.getSort();
        assert sortPaths.size() == sort.size();
        for (int i = 0; i != sort.size(); ++i) {
            final Name name = sortPaths.get(i).getName();
            final Object value = sort.get(i);
            final Use<?> type = schema.typeOf(name);
            result.put(DSL.field(dialect.columnName(name)), dialect.toSQLValue(type, value));
        }
        return result;
    }
}
