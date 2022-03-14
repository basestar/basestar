package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.expression.Expression;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.storage.sql.resolver.*;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pair;
import io.basestar.util.Sort;
import lombok.Getter;
import org.jooq.Query;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryMapping {

    @Getter
    private final QueryableSchema schema;

    private final Map<String, Object> arguments;

    private final Index index;

    private final boolean versioned;

    @Getter
    private final SortedMap<String, PropertyMapping<?>> properties;

    @Getter
    private final SortedMap<String, LinkMapping> links;

    public QueryMapping(final QueryableSchema schema, final Map<String, Object> arguments, final Index index, final boolean versioned, final Map<String, PropertyMapping<?>> properties, final Map<String, LinkMapping> links) {

        this.schema = schema;
        this.arguments = Immutable.map(arguments);
        this.index = index;
        this.versioned = versioned;
        this.properties = ImmutableSortedMap.copyOf(properties);
        this.links = ImmutableSortedMap.copyOf(links);
    }

    public Table<?> subselect(final Name qualifiedName, final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Set<Name> expand) {

        final ResolvedTable resolvedTable = tableResolver.requireTable(context, schema, arguments, index, versioned);
        final List<Field<?>> fields = selectFields(Name.of(), resolvedTable, expand).stream()
                .map(pair -> pair.getSecond().as(selectName(qualifiedName.with(pair.getFirst()))))
                .collect(Collectors.toList());

        Table<?> table = DSL.table(DSL.select(fields).from(resolvedTable.getTable())).as(selectName(qualifiedName));
        final Map<String, Set<Name>> branches = Name.branch(expand);
        for (final Map.Entry<String, PropertyMapping<?>> entry : properties.entrySet()) {
            final String name = entry.getKey();
            final PropertyMapping<?> mapping = entry.getValue();
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                table = mapping.join(qualifiedName.with(name), context, tableResolver, expressionResolver, table, branch);
            }
        }
        for (final Map.Entry<String, LinkMapping> entry : links.entrySet()) {
            final String name = entry.getKey();
            final LinkMapping mapping = entry.getValue();
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                table = mapping.join(qualifiedName.with(name), context, tableResolver, expressionResolver, table, branch);
            }
        }
        return table;
    }

    public SelectConditionStep<org.jooq.Record> select(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression, final Set<Name> expand) {

        return select(context, tableResolver, expressionResolver, expand).where(condition(context, tableResolver, expressionResolver, expression));
    }

    public SelectSeekStepN<org.jooq.Record> select(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression, final List<Sort> sort, final Set<Name> expand) {

        return select(context, tableResolver, expressionResolver, expression, expand).orderBy(orderFields(context, tableResolver, sort));
    }

    public SelectJoinStep<Record> select(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Set<Name> expand) {

        final Table<?> table = subselect(Name.of(), context, tableResolver, expressionResolver, expand);
        return context.select(DSL.asterisk()).from(table);
    }

    public SelectConditionStep<org.jooq.Record1<Integer>> selectCount(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression, final Set<Name> expand) {

        return context.select(DSL.count()).from(subselect(Name.of(), context, tableResolver, expressionResolver, expand)).where(condition(context, tableResolver, expressionResolver, expression));
    }

    public List<OrderField<?>> orderFields(final DSLContext context, final TableResolver tableResolver, final List<Sort> sort) {

        return sort.stream()
                .flatMap(v -> {
                    final Field<?> field = DSL.field(selectName(v.getName()));
                    return Stream.of(v.getOrder() == Sort.Order.ASC ? field.asc() : field.desc());
                })
                .collect(Collectors.toList());
    }

    public Condition condition(final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Expression expression) {

        final InferenceContext inferenceContext = InferenceContext.from(schema);
        return expressionResolver.condition(this::nestedField, inferenceContext, expression);
    }

    public Optional<Field<?>> nestedField(final Name name) {

        return nestedField(Name.of(), name);
    }

    public Optional<Field<?>> nestedField(final Name qualifiedName, final Name name) {

        if (name.isEmpty()) {
            return Optional.empty();
        } else {
            final String first = name.first();
            final PropertyMapping<?> property = properties.get(first);
            if (property != null) {
                return property.nestedField(qualifiedName.with(first), name.withoutFirst());
            } else {
                final LinkMapping link = links.get(first);
                if (link != null) {
                    return link.nestedField(qualifiedName.with(first), name.withoutFirst());
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    protected List<Pair<Field<?>, SelectField<?>>> emitRecord(final ResolvedTable table, final Map<String, Object> after) {

        return emitFields(after).stream()
                .flatMap(e -> table.column(e.getFirst())
                        .map(f -> Stream.of(Pair.<Field<?>, SelectField<?>>of(f, e.getSecond()))).orElse(Stream.of()))
                .collect(Collectors.toList());
    }

    public List<Field<?>> defineFields(final NamingStrategy namingStrategy, final Set<Name> expand) {

        return defineFields(Name.of(), namingStrategy, expand);
    }

    public List<Field<?>> defineFields(final Name qualifiedName, final NamingStrategy namingStrategy, final Set<Name> expand) {

        final Map<String, PropertyMapping<?>> properties = getProperties();
        return properties.entrySet().stream()
                .flatMap(v -> v.getValue().define(qualifiedName.with(v.getKey()), namingStrategy, expand).stream())
                .collect(Collectors.toList());
    }

    public List<Pair<Name, Field<?>>> selectFields(final Name qualifiedName, final ColumnResolver columnResolver, final Set<Name> expand) {

        final Map<String, PropertyMapping<?>> properties = getProperties();
        return properties.entrySet().stream()
                .flatMap(v -> v.getValue().select(qualifiedName.with(v.getKey()), columnResolver, expand).stream())
                .collect(Collectors.toList());
    }

    public List<Pair<Name, SelectField<?>>> emitFields(final Map<String, Object> value) {

        return emitFields(Name.of(), value);
    }

    public List<Pair<Name, SelectField<?>>> emitFields(final Name qualifiedName, final Map<String, Object> value) {

        final Map<String, PropertyMapping<?>> properties = getProperties();
        return properties.entrySet().stream()
                .flatMap(v -> {
                    final String name = v.getKey();
                    @SuppressWarnings("unchecked") final PropertyMapping<Object> mapping = (PropertyMapping<Object>) v.getValue();
                    return mapping.emit(qualifiedName.with(name), value == null ? null : value.get(name), ImmutableSet.of()).stream();
                })
                .collect(Collectors.toList());
    }

    public Set<Name> supportedExpand(final Set<Name> expand) {

        final Map<String, PropertyMapping<?>> properties = getProperties();
        final Map<String, LinkMapping> links = getLinks();

        final Set<Name> result = new HashSet<>();
        final Map<String, Set<Name>> branches = Name.branch(expand);
        properties.forEach((name, prop) -> {
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                prop.supportedExpand(branch).forEach(n -> result.add(Name.of(name).with(n)));
            }
        });
        links.forEach((name, link) -> {
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                link.supportedExpand(branch).forEach(n -> result.add(Name.of(name).with(n)));
            }
        });
        return result;
    }

    public List<Map<String, Object>> fromResult(final Result<org.jooq.Record> result, final Set<Name> expand) {

        return result.stream()
                .map(v -> fromRecord(new RecordResolver() {
                    @Override
                    public <T> T get(final Name name, final DataType<T> targetType) {

                        return targetType.convert(v.get(selectName(name)));
                    }
                }, expand))
                .collect(Collectors.toList());
    }

    public Map<String, Object> fromRecord(final RecordResolver record, final Set<Name> expand) {

        final QueryableSchema schema = getSchema();

        final Map<String, Object> result = new HashMap<>();

        final Map<String, Set<Name>> branches = Name.branch(expand);
        properties.forEach((name, prop) -> {
            final Set<Name> branch = branches.get(name);
            result.put(name, prop.fromRecord(Name.of(name), record, branch));
        });
        links.forEach((name, link) -> {
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                result.put(name, link.fromRecord(Name.of(name), record, branch));
            }
        });

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
        return new Instance(result);
    }

    public Query createObjectLayer(final DSLContext context, final TableResolver tableResolver, final Map<String, Object> after) {

        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);
        final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);

        return context.insertInto(table.getTable())
                .columns(Pair.mapToFirst(record))
                .select(DSL.select(Pair.mapToSecond(record).toArray(new SelectFieldOrAsterisk[0])));
    }

    @SuppressWarnings("unchecked")
    public Query updateObjectLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version, final Map<String, Object> after) {

        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);
        final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);

        final Field<String> idField = (Field<String>) table.requireColumn(Name.of(ReferableSchema.ID));
        final Field<Long> versionField = (Field<Long>) table.requireColumn(Name.of(ReferableSchema.VERSION));

        Condition condition = idField.eq(id);
        if (version != null) {
            condition = condition.and(versionField.eq(version));
        }

        return context.update(table.getTable())
                .set(Pair.toMap(record))
                .where(condition)
                .limit(DSL.inline(1));
    }

    @SuppressWarnings("unchecked")
    public Query deleteObjectLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version) {

        final ResolvedTable table = tableResolver.requireTable(context, schema, arguments, index, false);

        final Field<String> idField = (Field<String>) table.requireColumn(Name.of(ReferableSchema.ID));
        final Field<Long> versionField = (Field<Long>) table.requireColumn(Name.of(ReferableSchema.VERSION));

        Condition condition = idField.eq(id);
        if (version != null) {
            condition = condition.and(versionField.eq(version));
        }
        return context.deleteFrom(table.getTable())
                .where(condition).limit(DSL.inline(1));
    }

    @SuppressWarnings("unchecked")
    public Optional<Query> createHistoryLayer(final DSLContext context, final TableResolver tableResolver, final String id, final Long version, final Map<String, Object> after) {

        return tableResolver.table(context, schema, arguments, index, true).map(table -> {

            final List<Pair<Field<?>, SelectField<?>>> record = emitRecord(table, after);

            final Field<String> idField = (Field<String>) table.requireColumn(Name.of(ReferableSchema.ID));
            final Field<Long> versionField = (Field<Long>) table.requireColumn(Name.of(ReferableSchema.VERSION));

            context.deleteFrom(table.getTable()).where(idField.eq(id).and(versionField.eq(version))).execute();

            return context.insertInto(table.getTable())
                    .columns(Pair.mapToFirst(record))
                    .select(DSL.select(Pair.mapToSecond(record).toArray(new SelectFieldOrAsterisk[0])));
        });
    }

    public static org.jooq.Name selectName(final Name name) {

        return DSL.name("__" + name.toString("_"));
    }

    public Field<?> selectField(final Name name) {

        return DSL.field(selectName(name));
    }
}
