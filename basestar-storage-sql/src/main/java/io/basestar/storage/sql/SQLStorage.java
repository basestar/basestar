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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.compare.Eq;
import io.basestar.expression.constant.Constant;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.logical.And;
import io.basestar.expression.visitor.DisjunctionVisitor;
import io.basestar.schema.Index;
import io.basestar.schema.Schema;
import io.basestar.schema.*;
import io.basestar.schema.from.FromSql;
import io.basestar.schema.use.Use;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DefaultLayerStorage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.sql.mapping.QueryMapping;
import io.basestar.storage.sql.resolver.*;
import io.basestar.storage.sql.strategy.SQLStrategy;
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

    private final DataSource dataSource;

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

    private final TableResolver tableResolver;

    private final ExpressionResolver expressionResolver;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
        this.strategy = builder.strategy;
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
        this.tableResolver = new QueryTableResolver(strategy.useMetadata() ? new FlexibleTableResolver(strategy) : new StrictTableResolver(strategy)) {
            @Override
            protected Optional<ResolvedTable> query(final DSLContext context, final QuerySchema schema, final Map<String, Object> arguments) {

                final io.basestar.schema.from.From from = schema.getFrom();
                if (from instanceof FromSql) {
                    final String initialSql = ((FromSql) from).getReplacedSql(s -> tableResolver.requireTable(context, s, ImmutableMap.of(), null, false).getTable().getQualifiedName().toString());
                    final SQL sql = strategy.dialect().getReplacedSqlWithBindings(initialSql, schema.getArguments(), arguments);
                    final Table<?> table = DSL.table(sql);
                    return Optional.of(new ResolvedTable() {
                        @Override
                        public Table<?> getTable() {

                            return table;
                        }

                        @Override
                        public Optional<Field<?>> column(final Name name) {

                            return Optional.of(DSL.field(strategy.getNamingStrategy().columnName(name)));
                        }
                    });
                } else {
                    throw new UnsupportedOperationException("Query only supported with SQL from definition");
                }
            }
        };
        this.expressionResolver = (context, tableResolver, columnResolver, queryMapping, expression) -> {

            final SQLExpressionVisitor visitor = strategy.dialect().expressionResolver(context, tableResolver, columnResolver, queryMapping);
            return visitor.visit(expression);
        };
    }

    public static Builder builder() {

        return new Builder();
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

    @Override
    public Pager<Map<String, Object>> queryView(final Consistency consistency, final ViewSchema schema, final Expression query,
                                                final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());
        final QueryMapping queryMapping = strategy.dialect().schemaMapping(schema, false, expand);
        return (stats, token, count) -> queryImpl(queryMapping, bound, sort, expand, count, token, stats);
    }


    @Override
    public Pager<Map<String, Object>> queryQuery(final Consistency consistency, final QuerySchema schema, final Map<String, Object> arguments,
                                                 final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());
        final QueryMapping queryMapping = strategy.dialect().schemaMapping(schema, arguments, expand);
        return (stats, token, count) -> queryImpl(queryMapping, bound, sort, expand, count, token, stats);
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

            final QueryMapping queryMapping = strategy.dialect().schemaMapping(schema, index, expand);
            sources.put(conjunction.digest(), (stats, token, count) -> queryImpl(queryMapping, conjunction, sort, expand, count, token, stats));
        }

        return Pager.merge(Instance.comparator(sort), sources);
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = new And(query, new Eq(new NameConstant(ReferableSchema.ID), new Constant(id))).bind(Context.init());
        final QueryMapping queryMapping = strategy.dialect().schemaMapping(schema, true, expand);
        return (stats, token, count) -> queryImpl(queryMapping, bound, sort, expand, count, token, stats);
    }

    // Fix for https://groups.google.com/g/jooq-user/c/kABZWPQScSU
    private boolean canUseKeysetPaging(final QueryableSchema schema, final List<Sort> sort) {

        if (schema instanceof LinkableSchema) {
            for (final Sort s : sort) {
                final Use<?> type = schema.typeOf(s.getName());
                if (type.isOptional()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private CompletableFuture<Page<Map<String, Object>>> queryImpl(final QueryMapping queryMapping,
                                                                   final Expression expression, final List<Sort> sort,
                                                                   final Set<Name> expand, final int count,
                                                                   final Page.Token token, final Set<Page.Stat> stats) {

        final SQLDialect dialect = strategy.dialect();

        final boolean useKeysetPaging = canUseKeysetPaging(queryMapping.getSchema(), sort);

        final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {

            final SelectSeekStepN<org.jooq.Record> select = queryMapping.select(context, tableResolver, expressionResolver, expression, sort, expand);
            final SelectForUpdateStep<org.jooq.Record> seek;
            final long offset;
            if (token == null) {
                seek = select.limit(DSL.inline(count));
                offset = 0;
            } else if (useKeysetPaging) {
                final List<Object> values = KeysetPagingUtils.keysetValues(queryMapping.getSchema(), sort, token);
                seek = select.seek(values.stream().map(dialect::bind).toArray(Object[]::new)).limit(DSL.inline(count));
                offset = 0;
            } else {
                offset = Nullsafe.mapOrDefault(token, Page.Token::getLongValue, 0L);
                seek = select.offset(DSL.inline(offset)).limit(DSL.inline(count));
            }

            return seek.fetchAsync().thenApply(results -> {

                final List<Map<String, Object>> objects = queryMapping.fromResult(results, expand);

                final Page.Token nextToken;
                if (objects.size() < count) {
                    nextToken = null;
                } else if (useKeysetPaging) {
                    final Map<String, Object> last = objects.get(objects.size() - 1);
                    nextToken = KeysetPagingUtils.keysetPagingToken(queryMapping.getSchema(), sort, last);
                } else {
                    nextToken = Page.Token.fromLongValue(offset + count);
                }

                return new Page<>(objects, nextToken);
            });

        });

        if (stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {

            // Runs in parallel with query
            final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {

                final SelectConditionStep<org.jooq.Record1<Integer>> select = queryMapping.selectCount(context, tableResolver, expressionResolver, expression, expand);
                return select.fetchAsync().thenApply(results -> {

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
            @SuppressWarnings("unchecked")
            public CompletableFuture<BatchResponse> read() {

                final SQLDialect dialect = strategy.dialect();
                return withContext(initialContext -> initialContext.transactionResultAsync(config -> {

                    final DSLContext context = DSL.using(config);

                    final SortedMap<BatchResponse.RefKey, Map<String, Object>> results = new TreeMap<>();

                    for (final Map.Entry<ObjectSchema, Set<String>> entry : byId.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        // FIXME: must capture expand
                        final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());
                        final Set<String> ids = entry.getValue();

                        final SelectConditionStep<org.jooq.Record> select = queryMapping.select(context, tableResolver, expressionResolver, ImmutableSet.of())
                                .where(queryMapping.selectField(Name.of(ReferableSchema.ID)).in(literals(ids)));

                        queryMapping.fromResult(select.fetch(), Immutable.set())
                                .forEach(v -> results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), v), v));
                    }

                    for (final Map.Entry<ObjectSchema, Set<IdVersion>> entry : byIdVersion.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        // FIXME: must capture expand
                        final QueryMapping queryMapping = dialect.schemaMapping(schema, true, ImmutableSet.of());

                        final Set<Row2<String, Long>> idVersions = entry.getValue().stream()
                                .map(v -> DSL.row(v.getId(), v.getVersion()))
                                .collect(Collectors.toSet());

                        final SelectConditionStep<org.jooq.Record> select = queryMapping.select(context, tableResolver, expressionResolver, ImmutableSet.of())
                                .where(DSL.row(
                                        (Field<String>) queryMapping.selectField(Name.of(ReferableSchema.ID)),
                                        (Field<Long>) queryMapping.selectField(Name.of(ReferableSchema.VERSION))
                                ).in(idVersions));

                        queryMapping.fromResult(select.fetch(), Immutable.set())
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

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        return new WriteTransaction();
    }

    @Override
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return eventStrategy;
    }

    @Override
    public StorageTraits storageTraits(final Schema schema) {

        return new SQLStorageTraits(strategy.dialect());
    }

    @Override
    public Set<Name> supportedExpand(final QueryableSchema schema, final Set<Name> expand) {

        final SQLDialect dialect = strategy.dialect();
        final QueryMapping mapping;
        if (schema instanceof LinkableSchema) {
            mapping = dialect.schemaMapping((LinkableSchema) schema, false, expand);
        } else {
            mapping = dialect.schemaMapping((QuerySchema) schema, ImmutableMap.of(), expand);
        }
        return mapping.supportedExpand(expand);
    }

    @Override
    public CompletableFuture<Long> increment(final SequenceSchema schema) {

        return withContext(context -> {

            final org.jooq.Name sequenceName = strategy.getNamingStrategy().sequenceName(schema);
            return strategy.dialect().incrementSequence(context, sequenceName).fetchAsync().thenApply(result -> {

                final Iterator<Record1<Long>> iterator = result.iterator();
                if (iterator.hasNext()) {
                    return iterator.next().value1();
                } else {
                    throw new IllegalStateException("Sequence " + schema.getQualifiedName() + " yielded no value");
                }
            });
        });
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

    @Data
    private static class IdVersion {

        private final String id;

        private final long version;
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

                    final SQLDialect dialect = strategy.dialect();
                    // FIXME: must capture expand
                    final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

                    final int result = queryMapping.createObjectLayer(context, tableResolver, after).execute();
                    if (result != 1) {
                        throw new ObjectExistsException(schema.getQualifiedName(), id);
                    }

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

                final SQLDialect dialect = strategy.dialect();
                // FIXME: must capture expand
                final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

                final int result = queryMapping.updateObjectLayer(context, tableResolver, id, version, after).execute();

                if (result != 1) {
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }
                return BatchResponse.fromRef(schema.getQualifiedName(), after);
            });
        }

        @Override
        public void deleteObjectLayer(final ReferableSchema schema, final String id, final Map<String, Object> before) {

            steps.add(context -> {

                final Long version = before == null ? null : Instance.getVersion(before);

                final SQLDialect dialect = strategy.dialect();
                // FIXME: must capture expand
                final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

                final int result = queryMapping.deleteObjectLayer(context, tableResolver, id, version).execute();

                if (result != 1) {
                    throw new VersionMismatchException(schema.getQualifiedName(), id, version);
                }

                return BatchResponse.empty();
            });
        }

        @Override
        public void writeHistoryLayer(final ReferableSchema schema, final String id, final Map<String, Object> after) {

            steps.add(context -> {

                try {

                    final SQLDialect dialect = strategy.dialect();
                    // FIXME: must capture expand
                    final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

                    final Long version = Instance.getVersion(after);
                    queryMapping.createHistoryLayer(context, tableResolver, id, version, after).ifPresent(statement -> {

                        if (statement.execute() != 1) {
                            throw new ObjectExistsException(schema.getQualifiedName(), id);
                        }
                    });

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
        public void writeObjectLayer(final ReferableSchema schema, final Map<String, Object> after) {

            // not needed, alternate implementation of write provided
            throw new UnsupportedOperationException();
        }

        @Override
        public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

            // Fixme: should support materialized views
            if (schema instanceof ReferableSchema || schema instanceof ViewSchema && ((ViewSchema) schema).isMaterialized()) {
                steps.add(context -> {

                    try {

                        final SQLDialect dialect = strategy.dialect();
                        // FIXME: must capture expand
                        final QueryMapping queryMapping = dialect.schemaMapping(schema, false, ImmutableSet.of());

                        queryMapping.createObjectLayer(context, tableResolver, after).execute();

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
}
