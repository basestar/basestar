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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import io.basestar.schema.use.UseRef;
import io.basestar.schema.use.UseStruct;
import io.basestar.schema.util.Casing;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DefaultLayerStorage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.storage.exception.ObjectExistsException;
import io.basestar.storage.exception.VersionMismatchException;
import io.basestar.storage.query.Range;
import io.basestar.storage.query.RangeVisitor;
import io.basestar.storage.sql.resolver.FieldResolver;
import io.basestar.storage.sql.resolver.ValueResolver;
import io.basestar.storage.sql.strategy.NamingStrategy;
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
import org.jooq.impl.TableImpl;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.sql.Connection;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * FIXME: currently we use literals/inline everywhere because Athena doesn't like ? params, need to make this
 * strategy-driven rather than forced on everyone.
 */

@Slf4j
public class SQLStorage implements DefaultLayerStorage {

    public static final Duration TABLE_CACHE_DURATION = Duration.ofMinutes(30);

    private static final String COUNT_AS = "__count";

    private final DataSource dataSource;

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

    private final Cache<org.jooq.Name, Table<?>> tableCache;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
        this.strategy = builder.strategy;
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
        this.tableCache = CacheBuilder.newBuilder().expireAfterAccess(TABLE_CACHE_DURATION).build();
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

    private Optional<org.jooq.Field<?>> resolveField(@Nullable final Table<?> table, final String name) {

        if (strategy.useMetadata() && table != null) {
            final String normalizedName = strategy.getNamingStrategy().columnName(name);
            final List<org.jooq.Field<?>> fields = Arrays.stream(table.fields())
                    .filter(f -> strategy.getNamingStrategy().columnName(f.getName()).equals(normalizedName))
                    .collect(Collectors.toList());
            if (fields.isEmpty()) {
                return Optional.empty();
            } else if (fields.size() > 1) {
                throw new IllegalStateException("Field " + name + " is ambiguous in " + table.getQualifiedName());
            } else {
                return Optional.of(fields.get(0));
            }
        } else {
            return Optional.of(DSL.field(DSL.name(strategy.getNamingStrategy().columnName(name))));
        }
    }

    private FieldResolver fieldResolver(@Nullable final Table<?> table, final String name) {

        final Name root = Name.of(name);
        return new FieldResolver() {
            @Override
            public Optional<Field<?>> field() {

                return field(Name.empty());
            }

            @Override
            public Optional<Field<?>> field(final Name name) {

                return resolveField(table, root.with(name).toString("_"));
            }
        };
    }

    private List<SelectFieldOrAsterisk> selectFields(final QueryableSchema schema, @Nullable final Table<?> table) {

        final SQLDialect dialect = strategy.dialect();

        final List<SelectFieldOrAsterisk> fields = new ArrayList<>();
        schema.metadataSchema().forEach((name, type) -> {
            fields.addAll(dialect.selectFields(fieldResolver(table, name), Name.of(name), type));
        });
        schema.getProperties().forEach((name, prop) -> {
            fields.addAll(dialect.selectFields(fieldResolver(table, name), Name.of(name), prop.typeOf()));
        });
        return fields;
    }

    @Override
    public Pager<Map<String, Object>> queryView(final Consistency consistency, final ViewSchema schema, final Expression query,
                                                final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());
        return (stats, token, count) -> queryImpl(schema, null, schemaTableName(schema), bound, sort, expand, count, token, stats);
    }


    @Override
    public Pager<Map<String, Object>> queryQuery(final Consistency consistency, final QuerySchema schema, final Map<String, Object> arguments,
                                                 final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = query.bind(Context.init());

        final SQLDialect dialect = strategy.dialect();
        final NamingStrategy namingStrategy = strategy.getNamingStrategy();

        final io.basestar.schema.from.From from = schema.getFrom();
        if (from instanceof FromSql) {
            final SQLExpressionVisitor visitor = strategy.expressionVisitor(schema);
            final Condition condition = visitor.condition(bound);

            final String initialSql = ((FromSql) from).getReplacedSql(s -> namingStrategy.reference(s).toString());
            final SQL sql = dialect.getReplacedSqlWithBindings(initialSql, schema.getArguments(), arguments);

            return (stats, token, count) -> {

                final long offset = Nullsafe.mapOrDefault(token, Page.Token::getLongValue, 0L);

                final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {

                    final List<SelectFieldOrAsterisk> fields = selectFields(schema, null);
                    final List<OrderField<?>> orderFields = orderFields(null, sort);

                    return context.select(fields).from(sql).where(condition).orderBy(orderFields)
                            .limit(DSL.inline(count)).offset(DSL.inline(offset)).fetchAsync().thenApply(results -> {

                                final List<Map<String, Object>> objects = all(schema, results);

                                final Page.Token nextToken;
                                if (objects.size() < count) {
                                    nextToken = null;
                                } else {
                                    nextToken = Page.Token.fromLongValue(offset + count);
                                }

                                return new Page<>(objects, nextToken);
                            });
                });

                if (stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {

                    // Runs in parallel with query
                    final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {

                        return context.select(DSL.count().as(COUNT_AS)).from(sql).where(condition).fetchAsync().thenApply(results -> {

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

            };

        } else {
            throw new UnsupportedOperationException("Query only supported with SQL from definition");
        }
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

            final org.jooq.Name tableName = Optional.ofNullable(index)
                    .flatMap(index1 -> indexTableName(schema, index1))
                    .orElseGet(() -> schemaTableName(schema));

            sources.put(conjunction.digest(), (stats, token, count) -> queryImpl(schema, index, tableName, conjunction, sort, expand, count, token, stats));
        }

        return Pager.merge(Instance.comparator(sort), sources);
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Expression bound = new And(query, new Eq(new NameConstant(ReferableSchema.ID), new Constant(id))).bind(Context.init());
        return (stats, token, count) -> {
            final org.jooq.Name tableName = historyTableName(schema).orElse(schemaTableName(schema));
            return queryImpl(schema, null, tableName, bound, sort, expand, count, token, stats);
        };
    }

    private Condition condition(final Table<?> table, final LinkableSchema schema, final Index index, final Expression expression) {

        final Function<Name, QueryPart> columnResolver;
        if (index == null) {
            columnResolver = objectColumnResolver(table, schema);
        } else {
            columnResolver = indexColumnResolver(table, (ReferableSchema) schema, index);
        }
        return strategy.expressionVisitor(schema, columnResolver).condition(expression);
    }

    private List<OrderField<?>> orderFields(@Nullable final Table<?> table, final List<Sort> sort) {

        return sort.stream()
                .flatMap(v -> {
                    final Optional<Field<?>> opt = resolveField(table, v.getName().toString());
                    if (opt.isPresent()) {
                        final Field<?> field = opt.get();
                        return Stream.of(v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc());
                    } else {
                        return Stream.empty();
                    }
                })
                .collect(Collectors.toList());
    }

    private Table<?> resolveTable(final DSLContext context, final org.jooq.Name qualifiedName, final LinkableSchema schema) {

        return resolveTable(context, qualifiedName, schema, null);
    }


    private Table<?> resolveTable(final DSLContext context, final org.jooq.Name qualifiedName, final LinkableSchema schema, @Nullable final Index index) {

        if (strategy.useMetadata()) {
            return describeTable(context, qualifiedName);
        } else {
            final Casing columnCasing = strategy.getNamingStrategy().getColumnCasing();
            final List<Field<?>> fields = index == null
                    ? strategy.dialect().fields(columnCasing, schema)
                    : strategy.dialect().fields(columnCasing, (ReferableSchema) schema, index);

            return new TableImpl<org.jooq.Record>(qualifiedName) {
                {
                    fields.forEach(field -> createField(DSL.name(field.getName()), field.getDataType()));
                }
            };
        }
    }

    // Fix for https://groups.google.com/g/jooq-user/c/kABZWPQScSU
    private boolean canUseKeysetPaging(final LinkableSchema schema, final List<Sort> sort) {

        for (final Sort s : sort) {
            final Use<?> type = schema.typeOf(s.getName());
            if (type.isOptional()) {
                return false;
            }
        }
        return true;
    }

    private CompletableFuture<Page<Map<String, Object>>> queryImpl(final LinkableSchema schema, final Index index,
                                                                   final org.jooq.Name tableName,
                                                                   final Expression expression, final List<Sort> sort,
                                                                   final Set<Name> expand, final int count,
                                                                   final Page.Token token, final Set<Page.Stat> stats) {


        final SQLDialect dialect = strategy.dialect();

        final boolean useKeysetPaging = canUseKeysetPaging(schema, sort);

        final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {

            final Table<?> table = resolveTable(context, tableName, schema, index);

            final List<OrderField<?>> orderFields = orderFields(table, sort);

            final Condition condition = condition(table, schema, index, expression);

            log.debug("SQL condition {}", condition);

            final SelectSeekStepN<org.jooq.Record> select = context.select(selectFields(schema, table))
                    .from(table.getQualifiedName()).where(condition).orderBy(orderFields);

            final SelectForUpdateStep<org.jooq.Record> seek;
            final long offset;
            if (token == null) {
                seek = select.limit(DSL.inline(count));
                offset = 0;
            } else if (useKeysetPaging) {
                final List<Object> values = KeysetPagingUtils.keysetValues(schema, sort, token);
                seek = select.seek(values.stream().map(dialect::bind).toArray(Object[]::new)).limit(DSL.inline(count));
                offset = 0;
            } else {
                offset = Nullsafe.mapOrDefault(token, Page.Token::getLongValue, 0L);
                seek = select.offset(DSL.inline(offset)).limit(DSL.inline(count));
            }

            return seek.fetchAsync().thenApply(results -> {

                final List<Map<String, Object>> objects = all(schema, results);

                final Page.Token nextToken;
                if (objects.size() < count) {
                    nextToken = null;
                } else if (useKeysetPaging) {
                    final Map<String, Object> last = objects.get(objects.size() - 1);
                    nextToken = KeysetPagingUtils.keysetPagingToken(schema, sort, last);
                } else {
                    nextToken = Page.Token.fromLongValue(offset + count);
                }

                return new Page<>(objects, nextToken);
            });

        });

        if (stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {

            // Runs in parallel with query
            final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {

                final Table<?> table = resolveTable(context, tableName, schema, index);

                final Condition condition = condition(table, schema, index, expression);

                return context.select(DSL.count().as(COUNT_AS)).from(table.getQualifiedName()).where(condition).fetchAsync().thenApply(results -> {

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

    private Function<Name, QueryPart> objectColumnResolver(final Table<?> table, final LinkableSchema schema) {

        final SQLDialect dialect = strategy.dialect();

        return name -> {

            if (schema.metadataSchema().containsKey(name.first())) {
                final Name rest = name.withoutFirst();
                if (rest.isEmpty()) {
                    return resolveField(table, name.first()).map(DSL::field)
                            .orElseThrow(() -> new UnsupportedOperationException("Field " + name + " not found"));
                } else {
                    throw new UnsupportedOperationException("Query of this type is not supported");
                }
            } else {
                final Property prop = schema.requireProperty(name.first(), true);
                final Name rest = name.withoutFirst();
                if (rest.isEmpty()) {
                    return resolveField(table, name.first()).map(DSL::field)
                            .orElseThrow(() -> new UnsupportedOperationException("Field " + name + " not found"));
                } else {
                    return prop.typeOf().visit(new Use.Visitor.Defaulting<QueryPart>() {
                        @Override
                        public <T> QueryPart visitDefault(final Use<T> type) {

                            throw new UnsupportedOperationException("Query of this type is not supported");
                        }

                        @Override
                        public QueryPart visitStruct(final UseStruct type) {

                            // FIXME
                            return DSL.field(strategy.getNamingStrategy().columnName(name));
                        }

                        @Override
                        public QueryPart visitRef(final UseRef type) {

                            return dialect.refIdField(strategy.getNamingStrategy(), type, name);
                        }
                    });
                }
            }
        };
    }

    private Function<Name, QueryPart> indexColumnResolver(final Table<?> table, final ReferableSchema schema, final Index index) {

        final SQLDialect dialect = strategy.dialect();

        // FIXME
        return name -> DSL.field(strategy.getNamingStrategy().columnName(name));
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
                        final Table<?> table = resolveTable(context, schemaTableName(schema), schema);
                        final Set<String> ids = entry.getValue();
                        all(schema, context.select(selectFields(schema, table))
                                .from(table.getQualifiedName())
                                .where(idField(schema).in(literals(ids)))
                                .fetch())
                                .forEach(v -> results.put(BatchResponse.RefKey.from(schema.getQualifiedName(), v), v));
                    }

                    for (final Map.Entry<ObjectSchema, Set<IdVersion>> entry : byIdVersion.entrySet()) {
                        final ObjectSchema schema = entry.getKey();
                        final org.jooq.Name historyTableName = historyTableName(schema)
                                .orElseThrow(() -> new IllegalStateException("History table is not available, therefore querying by id_version is not allowed"));
                        final Table<?> table = resolveTable(context, historyTableName, schema);
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
    public Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return Collections.emptySet();
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

    private Field<String> idField(final ReferableSchema schema) {

        return DSL.field(DSL.name(strategy.getNamingStrategy().columnName(ObjectSchema.ID)), String.class);
    }

    private Field<Long> versionField(final ReferableSchema schema) {

        return DSL.field(DSL.name(strategy.getNamingStrategy().columnName(ObjectSchema.VERSION)), Long.class);
    }

    private org.jooq.Name schemaTableName(final LinkableSchema schema) {

        if (schema instanceof ReferableSchema) {
            return strategy.getNamingStrategy().objectTableName((ReferableSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return strategy.getNamingStrategy().viewName((ViewSchema) schema);
        } else {
            throw new IllegalStateException("Cannot determine name for schema " + schema);
        }
    }

    private org.jooq.Name objectTableName(final ReferableSchema schema) {

        return strategy.getNamingStrategy().objectTableName(schema);
    }

    private Optional<org.jooq.Name> historyTableName(final ReferableSchema schema) {

        return strategy.getNamingStrategy().historyTableName(schema);
    }

    private Optional<org.jooq.Name> indexTableName(final ReferableSchema schema, final Index index) {

        return strategy.getNamingStrategy().indexTableName(schema, index);
    }

    private Map<String, Object> first(final LinkableSchema schema, final Result<org.jooq.Record> result) {

        if (result.isEmpty()) {
            return null;
        } else {
            return fromRecord(schema, result.iterator().next());
        }
    }

    private List<Map<String, Object>> all(final QueryableSchema schema, final Result<org.jooq.Record> result) {

        return result.stream()
                .map(v -> fromRecord(schema, v))
                .collect(Collectors.toList());
    }

    private Map<String, Object> fromRecord(final QueryableSchema schema, final org.jooq.Record record) {

        final SQLDialect dialect = strategy.dialect();

        final Map<String, Object> data = record.intoMap();
        final Map<String, Object> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) ->
                result.put(k, dialect.fromSQLValue(v, ValueResolver.of(data.get(k)))));
        schema.getProperties().forEach((k, v) ->
                result.put(k, dialect.fromSQLValue(v.typeOf(), new ValueResolver() {
                    @Override
                    public Object value() {

                        return data.get(k);
                    }

                    @Override
                    public Object value(final Name name) {

                        return data.get(Name.of(k).with(name).toString("_"));
                    }
                })));

        if (Instance.getSchema(result) == null) {
            Instance.setSchema(result, schema.getQualifiedName());
        }
        // Make sure view records have a valid __key field
        if (schema instanceof ViewSchema) {
            final ViewSchema viewSchema = (ViewSchema) schema;
            if (!viewSchema.getFrom().isExternal()) {
                if (result.get(ViewSchema.ID) == null) {
                    result.put(ViewSchema.ID, viewSchema.createId(result));
                }
            }
        }
        return result;
    }

    private Map<Field<?>, SelectField<?>> toRecord(final DSLContext context, final LinkableSchema schema, final Map<String, Object> object) {

        final SQLDialect dialect = strategy.dialect();

        final Table<?> table = resolveTable(context, schemaTableName(schema), schema);

        final Map<Field<?>, SelectField<?>> result = new HashMap<>();
        schema.metadataSchema().forEach((k, v) -> {
            result.putAll(dialect.toSQLValues(v, fieldResolver(table, k), object.get(k)));
        });
        schema.getProperties().forEach((k, v) -> {
            result.putAll(dialect.toSQLValues(v.typeOf(), fieldResolver(table, k), object.get(k)));
        });

        return result.entrySet().stream().filter(e -> e.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Table<?> describeTable(final DSLContext context, final org.jooq.Name name) {

        try {
            return tableCache.get(name, () -> strategy.describeTable(context, name));
        } catch (final ExecutionException e) {
            throw new IllegalStateException(e);
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

//    private Map<Field<?>, Object> toRecord(final ReferableSchema schema, final Index index, final Index.Key key, final Map<String, Object> object) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        final Map<Field<?>, Object> result = new HashMap<>();
//        index.projectionSchema(schema).forEach((k, v) ->
//                result.put(DSL.field(DSL.name(k)), dialect.toSQLValue(v, object.get(k))));
//
//        final List<Name> partitionNames = index.resolvePartitionNames();
//        final List<Object> partition = key.getPartition();
//        assert partitionNames.size() == partition.size();
//        for (int i = 0; i != partition.size(); ++i) {
//            final Name name = partitionNames.get(i);
//            final Object value = partition.get(i);
//            final Use<?> type = schema.typeOf(name);
//            result.put(DSL.field(strategy.columnName(name)), dialect.toSQLValue(type, value));
//        }
//        final List<Sort> sortPaths = index.getSort();
//        final List<Object> sort = key.getSort();
//        assert sortPaths.size() == sort.size();
//        for (int i = 0; i != sort.size(); ++i) {
//            final Name name = sortPaths.get(i).getName();
//            final Object value = sort.get(i);
//            final Use<?> type = schema.typeOf(name);
//            result.put(DSL.field(strategy.columnName(name)), dialect.toSQLValue(type, value));
//        }
//        return result;
//    }

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
                    final org.jooq.Table<?> table = DSL.table(objectTableName(schema));
                    final Map<Field<?>, SelectField<?>> record = toRecord(context, schema, after);

                    final int result = dialect.createObjectLayer(context, table, idField(schema), id, record);
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
                final org.jooq.Table<?> table = DSL.table(objectTableName(schema));
                final Map<Field<?>, SelectField<?>> record = toRecord(context, schema, after);

                final int result = dialect.updateObjectLayer(context, table, idField(schema), versionField(schema), id, version, record);

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
                final org.jooq.Table<?> table = DSL.table(objectTableName(schema));

                final int result = dialect.deleteObjectLayer(context, table, idField(schema), versionField(schema), id, version);

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
                    final Optional<org.jooq.Name> historyTableName = historyTableName(schema);
                    if (historyTableName.isPresent()) {

                        final org.jooq.Table<?> table = DSL.table(historyTableName.get());
                        final Map<Field<?>, SelectField<?>> record = toRecord(context, schema, after);
                        final Long version = Instance.getVersion(after);

                        final int result = dialect.createHistoryLayer(context, table, idField(schema), versionField(schema), id, version, record);
                        if (result != 1) {
                            throw new ObjectExistsException(schema.getQualifiedName(), id);
                        }
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

                        final Map<Field<?>, SelectField<?>> record = toRecord(context, schema, after);

                        context.insertInto(DSL.table(schemaTableName(schema)))
                                .columns(record.keySet())
                                .select(DSL.select(record.values().toArray(new SelectFieldOrAsterisk[0])))
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
}
