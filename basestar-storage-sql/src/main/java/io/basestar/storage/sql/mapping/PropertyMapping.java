package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.schema.Instance;
import io.basestar.schema.ReferableSchema;
import io.basestar.storage.sql.resolver.ColumnResolver;
import io.basestar.storage.sql.resolver.ExpressionResolver;
import io.basestar.storage.sql.resolver.RecordResolver;
import io.basestar.storage.sql.resolver.TableResolver;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pair;
import lombok.Data;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;

public interface PropertyMapping<T> {

    T fromRecord(Name qualifiedName, RecordResolver record, Set<Name> expand);

    List<Field<?>> define(Name qualifiedName, NamingStrategy namingStrategy, Set<Name> expand);

    List<Pair<Name, Field<?>>> select(Name qualifiedName, ColumnResolver columnResolver, Set<Name> expand);

    List<Pair<Name, SelectField<?>>> emit(Name qualifiedName, T value, Set<Name> expand);

    DataType<?> mergedDataType();

    SelectField<?> emitMerged(T value, Set<Name> expand);

    Table<?> join(Name qualifiedName, DSLContext context, TableResolver tableResolver, ExpressionResolver expressionResolver, Table<?> table, Set<Name> expand);

    PropertyMapping<T> nullable();

    static <T> Simple<T, T> simple(final DataType<T> dataType) {

        return new Simple<>(dataType, ValueTransform.noop());
    }

    static <T, S> Simple<T, S> simple(final DataType<S> dataType, final ValueTransform<T, S> transform) {

        return new Simple<>(dataType, transform);
    }

    static FlattenedStruct flattenedStruct(final Map<String, PropertyMapping<?>> properties) {

        return new FlattenedStruct(properties);
    }

    static FlattenedRef flattenedRef(final Map<String, PropertyMapping<?>> properties) {

        return new FlattenedRef(properties);
    }

    static ExpandedRef expandedRef(final PropertyMapping<Map<String, Object>> mapping, final QueryMapping schema) {

        return new ExpandedRef(mapping, schema);
    }

    Set<Name> supportedExpand(Set<Name> expand);

    Optional<Field<?>> nestedField(Name qualifiedName, Name name);

    @Data
    class Simple<T, S> implements PropertyMapping<T> {

        private final DataType<S> dataType;

        private final ValueTransform<T, S> transform;

        public Simple(final DataType<S> dataType, final ValueTransform<T, S> transform) {

            this.dataType = Nullsafe.require(dataType);
            this.transform = Nullsafe.require(transform);
        }

        @Override
        @SuppressWarnings("unchecked")
        public T fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            final Object raw = record.get(qualifiedName, dataType);
            if (transform != null) {
                return ((ValueTransform<T, Object>) transform).fromSQLValue(raw, expand);
            } else {
                return (T) raw;
            }
        }

        @Override
        public List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy, final Set<Name> expand) {

            final org.jooq.Name columnName = namingStrategy.columnName(qualifiedName);
            return ImmutableList.of(DSL.field(columnName, dataType));
        }

        @Override
        public List<Pair<Name, Field<?>>> select(final Name qualifiedName, final ColumnResolver columnResolver, final Set<Name> expand) {

            return ImmutableList.of(Pair.of(qualifiedName, columnResolver.column(qualifiedName)
                    .orElse(DSL.castNull(dataType))));
        }

        @Override
        public List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final T value, final Set<Name> expand) {

            return ImmutableList.of(Pair.of(qualifiedName, emitMerged(value, expand)));
        }

        @Override
        public DataType<S> mergedDataType() {

            return dataType;
        }

        @Override
        public SelectField<?> emitMerged(final T value, final Set<Name> expand) {

            if (value == null) {
                return DSL.castNull(mergedDataType());
            } else {
                final Object output;
                if (transform != null) {
                    output = transform.toSQLValue(value);
                } else {
                    output = value;
                }
                return DSL.inline(output, dataType);
            }
        }

        @Override
        public Table<?> join(final Name qualifiedName, final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Table<?> table, final Set<Name> expand) {

            return table;
        }

        @Override
        public PropertyMapping<T> nullable() {

            return new Simple<>(dataType.nullable(true), transform);
        }

        @Override
        public Set<Name> supportedExpand(final Set<Name> expand) {

            return ImmutableSet.of();
        }

        @Override
        public Optional<Field<?>> nestedField(final Name qualifiedName, final Name name) {

            if (name.isEmpty()) {
                return Optional.of(DSL.field(QueryMapping.selectName(qualifiedName)));
            } else {
                return Optional.empty();
            }
        }
    }

    interface Flattened extends PropertyMapping<Map<String, Object>> {

        Map<String, PropertyMapping<?>> getMappings();

        @Override
        default Table<?> join(final Name qualifiedName, final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Table<?> table, final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            Table<?> result = table;
            final Map<String, Set<Name>> branches = Name.branch(expand);
            for (final Map.Entry<String, PropertyMapping<?>> entry : mappings.entrySet()) {
                final String name = entry.getKey();
                final PropertyMapping<?> mapping = entry.getValue();
                final Set<Name> branch = branches.get(name);
                result = mapping.join(qualifiedName.with(name), context, tableResolver, expressionResolver, result, branch);
            }
            return result;
        }

        @Override
        default Map<String, Object> fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final Map<String, Object> result = new HashMap<>();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            for (final Map.Entry<String, PropertyMapping<?>> entry : mappings.entrySet()) {
                final String name = entry.getKey();
                final PropertyMapping<?> mapping = entry.getValue();
                final Set<Name> branch = branches.get(name);
                final Object value = mapping.fromRecord(qualifiedName.with(name), record, branch);
                result.put(name, value);
            }
            return result;
        }

        @Override
        default List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy, final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            return mappings.entrySet().stream()
                    .flatMap(v -> {
                        final Set<Name> branch = branches.get(v.getKey());
                        return v.getValue().define(qualifiedName.with(v.getKey()), namingStrategy, branch).stream();
                    })
                    .collect(Collectors.toList());
        }

        @Override
        default List<Pair<Name, Field<?>>> select(final Name qualifiedName, final ColumnResolver columnResolver, final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            return mappings.entrySet().stream()
                    .flatMap(v -> {
                        final Set<Name> branch = branches.get(v.getKey());
                        return v.getValue().select(qualifiedName.with(v.getKey()), columnResolver, branch).stream();
                    })
                    .collect(Collectors.toList());
        }

        @Override
        @SuppressWarnings("unchecked")
        default List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final Map<String, Object> value, final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final List<Pair<Name, SelectField<?>>> result = new ArrayList<>();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            for (final Map.Entry<String, PropertyMapping<?>> entry : mappings.entrySet()) {
                final String name = entry.getKey();
                final Set<Name> branch = branches.get(name);
                final PropertyMapping<?> mapping = entry.getValue();
                final Object v = value == null ? null : value.get(name);
                result.addAll(((PropertyMapping<Object>) mapping).emit(qualifiedName.with(name), v, branch));
            }
            return result;
        }

        @Override
        default Set<Name> supportedExpand(final Set<Name> expand) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final Map<String, Set<Name>> branches = Name.branch(expand);
            final Set<Name> result = new HashSet<>();
            mappings.forEach((name, prop) -> {
                final Set<Name> branch = branches.get(name);
                if (branch != null) {
                    prop.supportedExpand(branch).forEach(n -> result.add(Name.of(name).with(n)));
                }
            });
            return result;
        }

        @Override
        default Optional<Field<?>> nestedField(final Name qualifiedName, final Name name) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            if (name.isEmpty()) {
                return Optional.empty();
            } else {
                final String first = name.first();
                final PropertyMapping<?> mapping = mappings.get(first);
                if (mapping != null) {
                    return mapping.nestedField(qualifiedName.with(first), name.withoutFirst());
                } else {
                    return Optional.empty();
                }
            }
        }

        @Override
        default DataType<?> mergedDataType() {

            throw new UnsupportedOperationException();
        }

        @Override
        default SelectField<?> emitMerged(final Map<String, Object> value, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }
    }

    @Data
    class FlattenedStruct implements Flattened {

        private final SortedMap<String, PropertyMapping<?>> mappings;

        public FlattenedStruct(final Map<String, PropertyMapping<?>> mappings) {

            this.mappings = ImmutableSortedMap.copyOf(mappings);
        }

        @Override
        public PropertyMapping<Map<String, Object>> nullable() {

            return new FlattenedStruct(mappings.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().nullable()
            )));
        }
    }

    @Data
    class FlattenedRef implements Flattened {

        private final SortedMap<String, PropertyMapping<?>> mappings;

        public FlattenedRef(final Map<String, PropertyMapping<?>> mappings) {

            this.mappings = ImmutableSortedMap.copyOf(mappings);
        }

        @Override
        public Map<String, Object> fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            final Map<String, Object> instance = Flattened.super.fromRecord(qualifiedName, record, expand);
            if (Instance.getId(instance) == null) {
                return null;
            } else {
                return instance;
            }
        }

        @Override
        public PropertyMapping<Map<String, Object>> nullable() {

            return new FlattenedRef(mappings.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().nullable()
            )));
        }
    }

    @Data
    class ExpandedRef implements PropertyMapping<Map<String, Object>> {

        private final PropertyMapping<Map<String, Object>> mapping;

        private final QueryMapping schema;

        public ExpandedRef(final PropertyMapping<Map<String, Object>> mapping, final QueryMapping schema) {

            this.mapping = mapping;
            this.schema = schema;
        }

        @Override
        public Map<String, Object> fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            final Map<String, Object> instance = mapping.fromRecord(qualifiedName, record, expand);
            if (instance != null) {
                final Map<String, Object> merged = new HashMap<>(instance);
                schema.fromRecord(new RecordResolver() {
                    @Override
                    public <T> T get(final Name name, final DataType<T> targetType) {

                        return record.get(qualifiedName.with("_").with(name), targetType);
                    }
                }, expand).forEach((k, v) -> {
                    if (!merged.containsKey(k)) {
                        merged.put(k, v);
                    }
                });
                return merged;
            } else {
                return null;
            }
        }

        @Override
        public List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy, final Set<Name> branch) {

            return mapping.define(qualifiedName, namingStrategy, branch);
        }

        @Override
        public List<Pair<Name, Field<?>>> select(final Name qualifiedName, final ColumnResolver columnResolver, final Set<Name> branch) {

            return mapping.select(qualifiedName, columnResolver, branch);
        }

        @Override
        public List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final Map<String, Object> value, final Set<Name> branch) {

            return mapping.emit(qualifiedName, value, branch);
        }

        @Override
        public DataType<?> mergedDataType() {

            throw new UnsupportedOperationException();
        }

        @Override
        public SelectField<?> emitMerged(final Map<String, Object> value, final Set<Name> expand) {

            throw new UnsupportedOperationException();
        }

        @Override
        public Table<?> join(final Name qualifiedName, final DSLContext context, final TableResolver tableResolver, final ExpressionResolver expressionResolver, final Table<?> table, final Set<Name> expand) {

            final Table<?> target = schema.subselect(qualifiedName.with("_"), context, tableResolver, expressionResolver, expand);
            final Field<Object> sourceId = DSL.field(DSL.name(QueryMapping.selectName(qualifiedName.with(ReferableSchema.ID))));
            final Field<Object> targetId = DSL.field(DSL.name(QueryMapping.selectName(qualifiedName.with("_").with(ReferableSchema.ID))));
            return table.leftJoin(DSL.table(DSL.select(DSL.asterisk()).from(target)).as(QueryMapping.selectName(qualifiedName)))
                    .on(sourceId.eq(targetId));
        }

        @Override
        public PropertyMapping<Map<String, Object>> nullable() {

            return new ExpandedRef(mapping.nullable(), schema);
        }

        @Override
        public Set<Name> supportedExpand(final Set<Name> expand) {

            final Set<Name> result = new HashSet<>();
            result.add(Name.empty());
            result.addAll(schema.supportedExpand(expand));
            return result;
        }

        @Override
        public Optional<Field<?>> nestedField(final Name qualifiedName, final Name name) {

            final Optional<Field<?>> base = mapping.nestedField(qualifiedName, name);
            if (base.isPresent()) {
                return base;
            } else {
                return schema.nestedField(qualifiedName.with("_"), name);
            }
        }
    }
}
