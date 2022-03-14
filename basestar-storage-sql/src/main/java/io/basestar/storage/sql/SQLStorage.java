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

//   public static final Duration TABLE_CACHE_DURATION = Duration.ofMinutes(30);

    private static final String COUNT_AS = "__count";

    private final DataSource dataSource;

    private final SQLStrategy strategy;

    private final EventStrategy eventStrategy;

//    private final Cache<org.jooq.Name, Table<?>> tableCache;

    private final TableResolver tableResolver;

    private final ExpressionResolver expressionResolver;

    private SQLStorage(final Builder builder) {

        this.dataSource = builder.dataSource;
        this.strategy = builder.strategy;
        this.eventStrategy = Nullsafe.orDefault(builder.eventStrategy, EventStrategy.EMIT);
//        this.tableCache = CacheBuilder.newBuilder().expireAfterAccess(TABLE_CACHE_DURATION).build();
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
        this.expressionResolver = (columnResolver, inferenceContext, expression) -> {

            final SQLExpressionVisitor visitor = strategy.dialect().expressionResolver(inferenceContext, columnResolver);
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

//    private Optional<org.jooq.Field<?>> resolveField(@Nullable final Table<?> table, final String name) {
//
//        return resolveField(table, null, name);
//    }

//    private Optional<org.jooq.Field<?>> resolveField(@Nullable final Table<?> table, @Nullable final Name tableAlias, final String name) {
//
//        if (strategy.useMetadata() && table != null) {
//            final org.jooq.Name normalizedName;
//            if (tableAlias != null) {
//                normalizedName = joinAlias(tableAlias).append(DSL.name(strategy.getNamingStrategy().columnName(name)));
//            } else {
//                normalizedName = DSL.name(strategy.getNamingStrategy().columnName(name));
//            }
//            final List<org.jooq.Name> fields = Arrays.stream(table.fields())
//                    .map(Named::getQualifiedName)
//                    .map(f -> {
//                        if (f.parts().length > normalizedName.parts().length) {
//                            return DSL.name(Arrays.stream(f.parts()).skip(f.parts().length - normalizedName.parts().length).toArray(org.jooq.Name[]::new));
//                        } else {
//                            return f;
//                        }
//                    })
//                    .filter(f -> f.equals(normalizedName))
//                    .collect(Collectors.toList());
//            if (fields.isEmpty()) {
//                return Optional.empty();
//            } else if (fields.size() > 1) {
//                throw new IllegalStateException("Field " + name + " is ambiguous in " + table.getQualifiedName());
//            } else {
//                return Optional.of(DSL.field(fields.get(0)));
//            }
//        } else {
//            final org.jooq.Name fieldName;
//            if (tableAlias != null) {
//                fieldName = joinAlias(tableAlias).append(DSL.name(strategy.getNamingStrategy().columnName(name)));
//            } else {
//                fieldName = DSL.name(strategy.getNamingStrategy().columnName(name));
//            }
//            return Optional.of(DSL.field(fieldName));
//        }
//    }

//    private Optional<org.jooq.Field<?>> resolveField(@Nullable final Table<?> table, @Nullable final Name name) {
//
//        if (strategy.useMetadata() && table != null) {
//            final org.jooq.Name normalizedName = DSL.name(strategy.getNamingStrategy().columnName(name));
//            final List<org.jooq.Name> fields = Arrays.stream(table.fields())
//                    .map(Named::getQualifiedName)
//                    .map(f -> {
//                        if (f.parts().length > normalizedName.parts().length) {
//                            return DSL.name(Arrays.stream(f.parts()).skip(f.parts().length - normalizedName.parts().length).toArray(org.jooq.Name[]::new));
//                        } else {
//                            return f;
//                        }
//                    })
//                    .filter(f -> f.equals(normalizedName))
//                    .collect(Collectors.toList());
//            if (fields.isEmpty()) {
//                return Optional.empty();
//            } else if (fields.size() > 1) {
//                throw new IllegalStateException("Field " + name + " is ambiguous in " + table.getQualifiedName());
//            } else {
//                return Optional.of(DSL.field(fields.get(0)));
//            }
//        } else {
//            final org.jooq.Name fieldName = DSL.name(strategy.getNamingStrategy().columnName(name));
//            return Optional.of(DSL.field(fieldName));
//        }
//    }

//    private FieldResolver fieldResolver(@Nullable final Table<?> table, final String name) {
//
//        final Name root = Name.of(name);
//        return new FieldResolver() {
//            @Override
//            public Optional<Field<?>> field() {
//
//                return field(Name.empty());
//            }
//
//            @Override
//            public Optional<Field<?>> field(final Name name) {
//
//                return resolveField(table, null, root.with(name).toString("_"));
//            }
//        };
//    }

//    private FieldResolver fieldResolver(@Nullable final Table<?> table, @Nullable final Name tableAlias, final String name) {
//
//        final Name root = Name.of(name);
//        return new FieldResolver() {
//            @Override
//            public Optional<Field<?>> field() {
//
//                return field(Name.empty());
//            }
//
//            @Override
//            public Optional<Field<?>> field(final Name name) {
//
//                return resolveField(table, tableAlias, root.with(name).toString("_"));
//            }
//        };
//    }

//    private List<SelectFieldOrAsterisk> selectFields(final DSLContext context, final QueryableSchema schema, @Nullable final Table<?> table) {
//
//        return selectFields(context, schema, table, null, Immutable.set());
//    }

//    private List<SelectFieldOrAsterisk> selectFields(final DSLContext context, final QueryableSchema schema, @Nullable final Table<?> table, @Nullable final Name tableAlias, final Set<Name> expand) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        final List<SelectFieldOrAsterisk> fields = new ArrayList<>();
//        schema.metadataSchema().forEach((name, type) -> {
//            fields.addAll(dialect.selectFields(fieldResolver(table, tableAlias, name), tableAlias == null ? Name.of(name) : tableAlias.with(name), type));
//        });
//        final Map<String, Set<Name>> branches = Name.branch(expand);
//        schema.getProperties().forEach((name, prop) -> {
//            final Set<Name> branch = branches.get(name);
//            if (branch != null) {
//                prop.getType().visit(new Use.Visitor.Defaulting<Void>() {
//
//                    @Override
//                    public <T> Void visitDefault(final Use<T> type) {
//
//                        fields.addAll(dialect.selectFields(fieldResolver(table, tableAlias, name), tableAlias == null ? Name.of(name) : tableAlias.with(name), prop.typeOf()));
//                        return null;
//                    }
//
//                    @Override
//                    public Void visitRef(final UseRef type) {
//
//                        final ReferableSchema otherSchema = type.getSchema();
//                        final Table<?> otherTable = resolveTable(context, otherSchema, type.isVersioned());
//                        fields.addAll(selectFields(context, otherSchema, otherTable, tableAlias == null ? null : tableAlias.with(name), ImmutableSet.of()));
//                        return null;
//                    }
//                });
//            } else {
//                fields.addAll(dialect.selectFields(fieldResolver(table, tableAlias, name), tableAlias == null ? Name.of(name) : tableAlias.with(name), prop.typeOf()));
//            }
//        });
//        schema.getLinks().forEach((name, link) -> {
//            if (link.isSingle()) {
//                final Set<Name> branch = branches.get(name);
//                if (branch != null) {
//                    final LinkableSchema otherSchema = link.getSchema();
//                    final Table<?> otherTable = resolveTable(context, otherSchema, false);
//                    fields.addAll(selectFields(context, otherSchema, otherTable, tableAlias == null ? Name.of(name) : tableAlias.with(name), ImmutableSet.of()));
//                }
//            }
//        });
//
//        return fields;
//    }

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

//
//        final Expression bound = query.bind(Context.init());
//
//        final SQLDialect dialect = strategy.dialect();
//        final NamingStrategy namingStrategy = strategy.getNamingStrategy();
//
//        final io.basestar.schema.from.From from = schema.getFrom();
//        if (from instanceof FromSql) {
//            final SQLExpressionVisitor visitor = strategy.expressionVisitor(schema);
//            final Condition condition = visitor.condition(bound);
//
//            final String initialSql = ((FromSql) from).getReplacedSql(s -> namingStrategy.reference(s).toString());
//            final SQL sql = dialect.getReplacedSqlWithBindings(initialSql, schema.getArguments(), arguments);
//
//            return (stats, token, count) -> {
//
//                final long offset = Nullsafe.mapOrDefault(token, Page.Token::getLongValue, 0L);
//
//                final CompletableFuture<Page<Map<String, Object>>> pageFuture = withContext(context -> {
//
//                    final List<SelectFieldOrAsterisk> fields = selectFields(context, schema, null);
//                    final List<OrderField<?>> orderFields = orderFields(null, sort);
//
//                    return context.select(fields).from(DSL.table(sql).as(joinAlias(Name.of()))).where(condition).orderBy(orderFields)
//                            .limit(DSL.inline(count)).offset(DSL.inline(offset)).fetchAsync().thenApply(results -> {
//
//                                final List<Map<String, Object>> objects = all(schema, results, expand);
//
//                                final Page.Token nextToken;
//                                if (objects.size() < count) {
//                                    nextToken = null;
//                                } else {
//                                    nextToken = Page.Token.fromLongValue(offset + count);
//                                }
//
//                                return new Page<>(objects, nextToken);
//                            });
//                });
//
//                if (stats != null && (stats.contains(Page.Stat.TOTAL) || stats.contains(Page.Stat.APPROX_TOTAL))) {
//
//                    // Runs in parallel with query
//                    final CompletableFuture<Page.Stats> statsFuture = withContext(context -> {
//
//                        return context.select(DSL.count().as(COUNT_AS)).from(sql).where(condition).fetchAsync().thenApply(results -> {
//
//                            if (results.isEmpty()) {
//                                return Page.Stats.ZERO;
//                            } else {
//                                final Record1<Integer> record = results.iterator().next();
//                                return Page.Stats.fromTotal(record.value1());
//                            }
//                        });
//                    });
//
//                    return CompletableFuture.allOf(pageFuture, statsFuture).thenApply(ignored -> {
//
//                        final Page<Map<String, Object>> page = pageFuture.getNow(Page.empty());
//                        return page.withStats(statsFuture.getNow(Page.Stats.ZERO));
//                    });
//
//                } else {
//                    return pageFuture;
//                }
//
//            };
//
//        } else {
//            throw new UnsupportedOperationException("Query only supported with SQL from definition");
//        }
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

//    private List<OrderField<?>> orderFields(@Nullable final Table<?> table, final List<Sort> sort) {
//
//        return sort.stream()
//                .flatMap(v -> {
//                    final Optional<Field<?>> opt = resolveField(table, Name.of(), v.getName().toString());
//                    if (opt.isPresent()) {
//                        final Field<?> field = opt.get();
//                        return Stream.of(v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc());
//                    } else {
//                        return Stream.empty();
//                    }
//                })
//                .collect(Collectors.toList());
//    }

//    private Table<?> resolveTable(final DSLContext context, final LinkableSchema schema, final boolean versioned) {
//
//        final org.jooq.Name tableName;
//        if (versioned) {
//            if (schema instanceof ReferableSchema) {
//                tableName = requireHistoryTableName((ReferableSchema) schema);
//            } else {
//                throw new IllegalStateException("Schema " + schema.getQualifiedName() + " cannot be versioned");
//            }
//        } else {
//            tableName = schemaTableName(schema);
//        }
//        return resolveTable(context, tableName, schema, null);
//    }

//    private Table<?> resolveTable(final DSLContext context, final org.jooq.Name qualifiedName, final LinkableSchema schema) {
//
//        return resolveTable(context, qualifiedName, schema, null);
//    }

//    private Table<?> resolveTable(final DSLContext context, final org.jooq.Name qualifiedName, final LinkableSchema schema, @Nullable final Index index) {
//
//        if (strategy.useMetadata()) {
//            return describeTable(context, qualifiedName);
//        } else {
//            final Casing columnCasing = strategy.getNamingStrategy().getColumnCasing();
//            final List<Field<?>> fields = index == null
//                    ? strategy.dialect().fields(columnCasing, schema)
//                    : strategy.dialect().fields(columnCasing, (ReferableSchema) schema, index);
//
//            return new TableImpl<org.jooq.Record>(qualifiedName) {
//                {
//                    fields.forEach(field -> createField(DSL.name(field.getName()), field.getDataType()));
//                }
//            };
//        }
//    }

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

//    private boolean requireDistinct(final LinkableSchema schema, final Set<Name> expand) {
//
//        final Map<String, Set<Name>> branches = Name.branch(expand);
//        for (final Link link : schema.getLinks().values()) {
//            final String name = link.getName();
//            final Set<Name> branch = branches.get(name);
//            if (branch != null && link.isSingle()) {
//                return true;
//            }
//        }
//        return false;
//    }

//    private Table<?> expandTable(final DSLContext context, final LinkableSchema schema, final Table<?> table, final Expression expression, final Set<Name> expand) {
//
//        final NamingStrategy namingStrategy = strategy.getNamingStrategy();
//
//        final Set<Name> joinExpand = expand == null ? new HashSet<>() : new HashSet<>(expand);
//        if (expression != null) {
//            joinExpand.addAll(schema.requiredExpand(expression.names()));
//        }
//
//        Table<?> result = table;
//        final Map<String, Set<Name>> branches = Name.branch(joinExpand);
//        for (final Property property : schema.getProperties().values()) {
//            final String name = property.getName();
//            final Set<Name> branch = branches.get(name);
//            if (branch != null) {
//                final Table<?> current = result;
//                result = property.getType().visit(new Use.Visitor.Defaulting<Table<?>>() {
//                    @Override
//                    public <T> Table<?> visitDefault(final Use<T> type) {
//
//                        return current;
//                    }
//
//                    @Override
//                    public Table<?> visitRef(final UseRef type) {
//
//                        final ReferableSchema otherSchema = type.getSchema();
//                        final Table<?> otherTable = resolveTable(context, otherSchema, type.isVersioned()).as(joinAlias(Name.of()));
//                        final Table<?> otherSelect = DSL.table(context.select(selectFields(context, otherSchema, otherTable, Name.of(), expand)).from(otherTable));
//                        return current.leftJoin(otherSelect.as(joinAlias(Name.of(name))))
//                                .on(
//                                        DSL.field(joinAlias(Name.of()).append(namingStrategy.columnName(Name.of(name).with(ReferableSchema.ID))))
//                                                .eq(DSL.field(joinAlias(Name.of(name)).append(namingStrategy.columnName(Name.of(ReferableSchema.ID)))))
//                                );
//                    }
//                });
//            }
//        }
//        for (final Link link : schema.getLinks().values()) {
//            final String name = link.getName();
//            final Set<Name> branch = branches.get(name);
//            if (branch != null && link.isSingle()) {
//                final LinkableSchema otherSchema = link.getSchema();
//                final Table<?> otherTable = resolveTable(context, otherSchema, false).as(joinAlias(Name.of(name)));
//                final Function<Name, QueryPart> columnResolver = linkColumnResolver(context, table, schema, otherTable, otherSchema);
//                final Condition condition = strategy.expressionVisitor(schema, columnResolver).condition(link.getExpression());
//                result = result.leftJoin(otherTable.as(joinAlias(Name.of(name))))
//                        .on(condition);
//            }
//        }
//        return result;
//    }

    private org.jooq.Name joinAlias(final Name name) {

        return DSL.name("__" + name.toString("__"));
    }

//    private Function<Name, QueryPart> linkColumnResolver(final DSLContext context, final Table<?> table, final LinkableSchema schema, final Table<?> otherTable, final LinkableSchema otherSchema) {
//
//        final Function<Name, QueryPart> resolver = objectColumnResolver(context, table, schema);
//        final Function<Name, QueryPart> otherResolver = objectColumnResolver(context, otherTable, otherSchema);
//
//        return name -> {
//            if (Reserved.THIS.equals(name.first())) {
//                return resolver.apply(name.withoutFirst());
//            } else {
//                return otherResolver.apply(name);
//            }
//        };
//    }

//    private Function<Name, QueryPart> objectColumnResolver(final DSLContext context, final Table<?> table, final LinkableSchema schema) {
//
//        return objectColumnResolver(context, table, Name.of(), schema);
//    }

//    private Function<Name, QueryPart> objectColumnResolver(final DSLContext context, final Table<?> table, final Name joinAlias, final LinkableSchema schema) {
//
//        final SQLDialect dialect = strategy.dialect();
//        final NamingStrategy namingStrategy = strategy.getNamingStrategy();
//
//        return name -> {
//
//            final String first = name.first();
//            if (schema.metadataSchema().containsKey(first)) {
//                final Name rest = name.withoutFirst();
//                if (rest.isEmpty()) {
//                    return resolveField(table, joinAlias, name.first()).map(DSL::field)
//                            .orElseThrow(() -> new UnsupportedOperationException("Field " + name + " not found"));
//                } else {
//                    throw new UnsupportedOperationException("Query of this type is not supported");
//                }
//            } else {
//                final Member member = schema.requireMember(first, true);
//                final Name rest = name.withoutFirst();
//                if (member instanceof Property) {
//                    if (rest.isEmpty()) {
//                        return resolveField(table, joinAlias, first).map(DSL::field)
//                                .orElseThrow(() -> new UnsupportedOperationException("Field " + name + " not found"));
//                    } else {
//                        return member.typeOf().visit(new Use.Visitor.Defaulting<QueryPart>() {
//                            @Override
//                            public <T> QueryPart visitDefault(final Use<T> type) {
//
//                                throw new UnsupportedOperationException("Query of this type is not supported");
//                            }
//
//                            @Override
//                            public QueryPart visitStruct(final UseStruct type) {
//
//                                // FIXME
//                                return DSL.field(DSL.name(joinAlias(joinAlias), namingStrategy.columnName(name)));
//                            }
//
//                            @Override
//                            public QueryPart visitRef(final UseRef type) {
//
//                                if (rest.equals(Name.of(ReferableSchema.ID))) {
//                                    return dialect.refIdField(namingStrategy, type, name);
//                                } else {
//                                    return DSL.field(joinAlias(Name.of(first)).append(namingStrategy.columnName(rest)));
//                                }
//                            }
//                        });
//                    }
//                } else if (member instanceof Link && ((Link) member).isSingle()) {
//                    if (rest.isEmpty()) {
//                        throw new IllegalStateException("Query of this type is not supported (link expansion must target a property");
//                    } else {
//                        final Link link = (Link) member;
//                        final LinkableSchema otherSchema = link.getSchema();
//                        final Table<?> otherTable = resolveTable(context, otherSchema, false);
//                        return objectColumnResolver(context, otherTable, joinAlias.with(link.getName()), otherSchema).apply(rest);
//                    }
//                } else {
//                    throw new IllegalStateException("Expansion of " + first + " not supported in " + schema.getQualifiedName());
//                }
//            }
//        };
//    }

//    private Function<Name, QueryPart> indexColumnResolver(final Table<?> table, final ReferableSchema schema, final Index index) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        // FIXME
//        return name -> DSL.field(strategy.getNamingStrategy().columnName(name));
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

//    private TableResolver tableResolver() {
//
//        return (context, schema, versioned) -> {
//
//            final Table<?> table = resolveTable(context, (LinkableSchema) schema, versioned);
//            return Optional.of(new ResolvedTable() {
//                @Override
//                public org.jooq.Name getName() {
//
//                    return table.getQualifiedName();
//                }
//
//                @Override
//                public Optional<Field<?>> column(final Name name) {
//
//                    return resolveField(table, name);
//                }
//            });
//        };
//    }

//    private SchemaMapping schemaMapping(final QueryableSchema schema) {
//
//        return strategy.dialect().schemaMapping(schema);
//    }

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

//    private Field<String> idField(final ReferableSchema schema) {
//
//        return DSL.field(DSL.name(strategy.getNamingStrategy().columnName(ObjectSchema.ID)), String.class);
//    }

//    private Field<Long> versionField(final ReferableSchema schema) {
//
//        return DSL.field(DSL.name(strategy.getNamingStrategy().columnName(ObjectSchema.VERSION)), Long.class);
//    }

//    private org.jooq.Name schemaTableName(final LinkableSchema schema) {
//
//        if (schema instanceof ReferableSchema) {
//            return strategy.getNamingStrategy().objectTableName((ReferableSchema) schema);
//        } else if (schema instanceof ViewSchema) {
//            return strategy.getNamingStrategy().viewName((ViewSchema) schema);
//        } else {
//            throw new IllegalStateException("Cannot determine name for schema " + schema);
//        }
//    }

//    private org.jooq.Name objectTableName(final ReferableSchema schema) {
//
//        return strategy.getNamingStrategy().objectTableName(schema);
//    }

//    private Optional<org.jooq.Name> historyTableName(final ReferableSchema schema) {
//
//        return strategy.getNamingStrategy().historyTableName(schema);
//    }

//    private org.jooq.Name requireHistoryTableName(final ReferableSchema schema) {
//
//        return historyTableName(schema).orElseThrow(() -> new IllegalStateException("History not enabled for " + schema.getQualifiedName()));
//    }

//    private Optional<org.jooq.Name> indexTableName(final ReferableSchema schema, final Index index) {
//
//        return strategy.getNamingStrategy().indexTableName(schema, index);
//    }

//    private Map<String, Object> first(final LinkableSchema schema, final Result<org.jooq.Record> result, final Set<Name> expand) {
//
//        if (result.isEmpty()) {
//            return null;
//        } else {
//            return fromRecord(schema, result.iterator().next(), expand);
//        }
//    }

//    private List<Map<String, Object>> all(final QueryableSchema schema, final Result<org.jooq.Record> result, final Set<Name> expand) {
//
//        return result.stream()
//                .map(v -> fromRecord(schema, v, expand))
//                .collect(Collectors.toList());
//    }

//    private Map<String, Object> fromRecord(final QueryableSchema schema, final org.jooq.Record record, final Set<Name> expand) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        final Map<String, Object> data = record.intoMap();
//        final Map<String, Object> result = new HashMap<>();
//        schema.metadataSchema().forEach((k, v) ->
//                result.put(k, dialect.fromSQLValue(v, ValueResolver.of(data.get(k)))));
//        final Map<String, Set<Name>> branches = Name.branch(expand);
//        schema.getProperties().forEach((k, v) -> {
//            result.put(k, dialect.fromSQLValue(v.typeOf(), new ValueResolver() {
//                @Override
//                public Object value() {
//
//                    return data.get(k);
//                }
//
//                @Override
//                public Object value(final Name name) {
//
//                    return data.get(Name.of(k).with(name).toString("_"));
//                }
//            }));
//            final Set<Name> branch = branches.get(k);
//            if (branch != null) {
//                v.getType().visit(new Use.Visitor.Defaulting<Void>() {
//                    @Override
//                    public Void visitRef(final UseRef type) {
//
//                        if (data.containsKey(k + "_" + ReferableSchema.ID)) {
//                            result.put(k, fromSubRecord(type.getSchema(), data, k + "_"));
//                        }
//                        return null;
//                    }
//                });
//            }
//        });
//
//        if (Instance.getSchema(result) == null) {
//            Instance.setSchema(result, schema.getQualifiedName());
//        }
//        // Make sure view records have a valid __key field
//        if (schema instanceof ViewSchema) {
//            final ViewSchema viewSchema = (ViewSchema) schema;
//            if (!viewSchema.getFrom().isExternal()) {
//                if (result.get(ViewSchema.ID) == null) {
//                    result.put(ViewSchema.ID, viewSchema.createId(result));
//                }
//            }
//        }
//        return result;
//    }

//    private Object fromSubRecord(final ReferableSchema schema, final Map<String, Object> data, final String prefix) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        final Map<String, Object> result = new HashMap<>();
//        schema.metadataSchema().forEach((k, v) ->
//                result.put(k, dialect.fromSQLValue(v, ValueResolver.of(data.get(k)))));
//        schema.getProperties().forEach((k, v) -> {
//            result.put(k, dialect.fromSQLValue(v.typeOf(), new ValueResolver() {
//                @Override
//                public Object value() {
//
//                    return data.get(prefix + k);
//                }
//
//                @Override
//                public Object value(final Name name) {
//
//                    return data.get(Name.of(prefix + k).with(name).toString("_"));
//                }
//            }));
//        });
//        return result;
//    }

//    private Map<Field<?>, SelectField<?>> toRecord(final DSLContext context, final LinkableSchema schema, final Map<String, Object> object) {
//
//        final SQLDialect dialect = strategy.dialect();
//
//        final Table<?> table = resolveTable(context, schemaTableName(schema), schema);
//
//        final Map<Field<?>, SelectField<?>> result = new HashMap<>();
//        schema.metadataSchema().forEach((k, v) -> {
//            result.putAll(dialect.toSQLValues(v, fieldResolver(table, k), object.get(k)));
//        });
//        schema.getProperties().forEach((k, v) -> {
//            result.putAll(dialect.toSQLValues(v.typeOf(), fieldResolver(table, k), object.get(k)));
//        });
//
//        return result.entrySet().stream().filter(e -> e.getValue() != null)
//                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
//    }

//    public Table<?> describeTable(final DSLContext context, final org.jooq.Name name) {
//
//        try {
//            return tableCache.get(name, () -> strategy.describeTable(context, name));
//        } catch (final ExecutionException e) {
//            throw new IllegalStateException(e);
//        }
//    }

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
