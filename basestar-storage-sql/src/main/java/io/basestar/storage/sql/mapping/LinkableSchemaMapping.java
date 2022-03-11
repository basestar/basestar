//package io.basestar.storage.sql.mapping;
//
//import com.google.common.collect.ImmutableMap;
//import com.google.common.collect.ImmutableSortedMap;
//import io.basestar.expression.Expression;
//import io.basestar.schema.*;
//import io.basestar.schema.Index;
//import io.basestar.schema.expression.InferenceContext;
//import io.basestar.storage.sql.resolver.*;
//import io.basestar.storage.sql.strategy.NamingStrategy;
//import io.basestar.util.Immutable;
//import io.basestar.util.Name;
//import io.basestar.util.Pair;
//import io.basestar.util.Sort;
//import lombok.Getter;
//import org.jooq.*;
//import org.jooq.Query;
//import org.jooq.impl.DSL;
//
//import java.util.*;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//public class LinkableSchemaMapping implements SchemaMapping {
//
//    @Getter
//    private final QueryableSchema schema;
//
//    private final Map<String, Object> arguments;
//
//    private final Index index;
//
//    private final boolean versioned;
//
//    @Getter
//    private final SortedMap<String, PropertyMapping<?>> properties;
//
//    @Getter
//    private final SortedMap<String, LinkMapping> links;
//
//    public LinkableSchemaMapping(final LinkableSchema schema, final Map<String, Object> arguments, final Index index, final boolean versioned, final Map<String, PropertyMapping<?>> properties, final Map<String, LinkMapping> links) {
//
//        this.schema = schema;
//        this.arguments = ImmutableMap.copyOf(arguments);
//        this.index = index;
//        this.versioned = versioned;
//        this.properties = ImmutableSortedMap.copyOf(properties);
//        this.links = ImmutableSortedMap.copyOf(links);
//    }
//
//    @Override
//    public SelectJoinStep<Record> select(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, versioned);
//        final List<Field<?>> fields = selectFields(Name.of(), table);
//
//        return context.select(fields).from(table.getTable());
//    }
//
//    @Override
//    public SelectConditionStep<org.jooq.Record1<Integer>> selectCount(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, versioned);
//
//        return context.select(DSL.count()).from(table.getTable()).where(condition(context, tableResolver, expressionResolver, expression));
//    }
//
//    @Override
//    public List<OrderField<?>> orderFields(final DSLContext context, final TableResolver tableResolver, final List<Sort> sort) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, versioned);
//        return sort.stream()
//                .flatMap(v -> {
//                    final Optional<Field<?>> opt = table.column(v.getName());
//                    if (opt.isPresent()) {
//                        final Field<?> field = opt.get();
//                        return Stream.of(v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc());
//                    } else {
//                        return Stream.empty();
//                    }
//                })
//                .collect(Collectors.toList());
//    }
//
//    @Override
//    public Condition condition(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression) {
//
//        final InferenceContext inferenceContext = InferenceContext.from(schema);
//        return expressionResolver.condition(columnResolver(context, tableResolver), inferenceContext, expression);
//    }
//
//    private ColumnResolver columnResolver(final DSLContext context, final TableResolver tableResolver) {
//
//        return name -> tableResolver.requireTable(context, schema, arguments, index, versioned)
//                .column(name);
//    }
//
//    private List<Pair<Field<?>, SelectField<?>>> emitRecord(final ResolvedTable table, final Map<String, Object> after) {
//
//        return emitFields(after).stream()
//                .flatMap(e -> table.column(e.getFirst())
//                        .map(f -> Stream.of(Pair.<Field<?>, SelectField<?>>of(f, e.getSecond()))).orElse(Stream.of()))
//                .collect(Collectors.toList());
//    }
//
//    public Query createObjectLayer(final DSLContext context, final TableResolver tableResolver, final Map<String, Object> after) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);
//        final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);
//
//        return context.insertInto(table.getTable())
//                .columns(Pair.mapToFirst(record))
//                .select(DSL.select(Pair.mapToSecond(record).toArray(new SelectFieldOrAsterisk[0])));
//    }
//
//    @SuppressWarnings("unchecked")
//    public Query updateObjectLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version, final Map<String, Object> after) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);
//        final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);
//
//        final Field<String> idField = (Field<String>)table.requireColumn(Name.of(ReferableSchema.ID));
//        final Field<Long> versionField = (Field<Long>)table.requireColumn(Name.of(ReferableSchema.VERSION));
//
//        Condition condition = idField.eq(id);
//        if (version != null) {
//            condition = condition.and(versionField.eq(version));
//        }
//
//        return context.update(table.getTable())
//                .set(Pair.toMap(record))
//                .where(condition)
//                .limit(DSL.inline(1));
//    }
//
//    @SuppressWarnings("unchecked")
//    public Query deleteObjectLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version) {
//
//        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);
//
//        final Field<String> idField = (Field<String>)table.requireColumn(Name.of(ReferableSchema.ID));
//        final Field<Long> versionField = (Field<Long>)table.requireColumn(Name.of(ReferableSchema.VERSION));
//
//        Condition condition = idField.eq(id);
//        if (version != null) {
//            condition = condition.and(versionField.eq(version));
//        }
//        return context.deleteFrom(table.getTable())
//                .where(condition).limit(DSL.inline(1));
//    }
//
//    @SuppressWarnings("unchecked")
//    public Optional<Query> createHistoryLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version, final Map<String, Object> after) {
//
//        return tableResolver.table(context, schema,  arguments, index, true).map(table -> {
//
//            final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);
//
//            final Field<String> idField = (Field<String>)table.requireColumn(Name.of(ReferableSchema.ID));
//            final Field<Long> versionField = (Field<Long>)table.requireColumn(Name.of(ReferableSchema.VERSION));
//
//            context.deleteFrom(table.getTable()).where(idField.eq(id).and(versionField.eq(version))).execute();
//
//            return context.insertInto(table.getTable())
//                    .columns(Pair.mapToFirst(record))
//                    .select(DSL.select(Pair.mapToSecond(record).toArray(new SelectFieldOrAsterisk[0])));
//        });
//    }
//}
