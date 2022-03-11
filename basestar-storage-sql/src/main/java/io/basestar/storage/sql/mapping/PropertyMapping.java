package io.basestar.storage.sql.mapping;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.basestar.schema.Instance;
import io.basestar.storage.sql.resolver.ColumnResolver;
import io.basestar.storage.sql.resolver.RecordResolver;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pair;
import lombok.Data;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.SelectField;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;

public interface PropertyMapping<T> {

    T fromRecord(Name qualifiedName, RecordResolver record, Set<Name> expand);

    //Map<Name, DataType<?>> dataTypes(Name alias);

    List<Field<?>> define(Name qualifiedName, NamingStrategy namingStrategy);

    List<Field<?>> select(Name qualifiedName, ColumnResolver columnResolver);

    List<Pair<Name, SelectField<?>>> emit(Name qualifiedName, T value);

//    SelectJoinStep<?> join(Name qualifiedName, SchemaMapping schema, TableResolver tableResolver, SelectJoinStep<?> step, Set<Name> expand);

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

    static ExpandedRef expandedRef(final PropertyMapping<Instance> mapping, final SchemaMapping schema) {

        return new ExpandedRef(mapping, schema);
    }

    Set<Name> supportedExpand(Set<Name> expand);

//    Field<?> field(Name qualifiedName, Name name, ColumnResolver columnResolver);

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
        public List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy) {

            final org.jooq.Name columnName = namingStrategy.columnName(qualifiedName);
            return ImmutableList.of(DSL.field(columnName, dataType));
        }

        @Override
        public List<Field<?>> select(final Name qualifiedName, final ColumnResolver columnResolver) {

            return ImmutableList.of(columnResolver.column(qualifiedName)
                    .orElse(DSL.castNull(dataType)).as(DSL.name(qualifiedName.toString())));
        }

        @Override
        public List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final T value) {

            final Object output;
            if (transform != null) {
                output = transform.toSQLValue(value);
            } else {
                output = value;
            }
            return ImmutableList.of(Pair.of(qualifiedName, DSL.inline(output, dataType)));
        }

        @Override
        public PropertyMapping<T> nullable() {

            return new Simple<>(dataType.nullable(true), transform);
        }

        @Override
        public Set<Name> supportedExpand(final Set<Name> expand) {

            return ImmutableSet.of();
        }

//        @Override
//        public Field<?> field(final Name qualifiedName, final Name name, final ColumnResolver columnResolver) {
//
//            if(name.isEmpty()) {
//                return columnResolver.column(qualifiedName, dataType);
//            } else {
//                throw new UndefinedNameException(name.first());
//            }
//        }
    }

    interface Flattened extends PropertyMapping<Instance> {

        Map<String, PropertyMapping<?>> getMappings();

        @Override
        default Instance fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

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
            return new Instance(result);
        }

        @Override
        default List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            return mappings.entrySet().stream()
                    .flatMap(v -> v.getValue().define(qualifiedName.with(v.getKey()), namingStrategy).stream())
                    .collect(Collectors.toList());
        }

        @Override
        default List<Field<?>> select(final Name qualifiedName, final ColumnResolver columnResolver) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            return mappings.entrySet().stream()
                    .flatMap(v -> v.getValue().select(qualifiedName.with(v.getKey()), columnResolver).stream())
                    .collect(Collectors.toList());
        }

        @Override
        @SuppressWarnings("unchecked")
        default List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final Instance value) {

            final Map<String, PropertyMapping<?>> mappings = getMappings();
            final List<Pair<Name, SelectField<?>>> result = new ArrayList<>();
            for (final Map.Entry<String, PropertyMapping<?>> entry : mappings.entrySet()) {
                final String name = entry.getKey();
                final PropertyMapping<?> mapping = entry.getValue();
                final Object v = value == null ? null : value.get(name);
                result.addAll(((PropertyMapping<Object>) mapping).emit(qualifiedName.with(name), v));
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

//        @Override
//        default Field<?> field(final Name qualifiedName, final Name name, final ColumnResolver columnResolver) {
//
//            final Map<String, PropertyMapping<?>> mappings = getMappings();
//            final PropertyMapping<?> mapping = mappings.get(name.first());
//            if(mapping != null) {
//                return mapping.field(qualifiedName, name.withoutFirst(), columnResolver);
//            } else {
//                throw new UndefinedNameException(name.first(), mappings.keySet());
//            }
//        }

//        @Override
//        default SelectJoinStep<?> join(final Name qualifiedName, final SchemaMapping schema, final TableResolver tableResolver, final SelectJoinStep<?> step, final Set<Name> expand) {
//
//            final Map<String, PropertyMapping<?>> mappings = getMappings();
//            SelectJoinStep<?> result = step;
//            for (final Map.Entry<String, PropertyMapping<?>> entry : mappings.entrySet()) {
//                final String name = entry.getKey();
//                final PropertyMapping<?> mapping = entry.getValue();
//                result = mapping.join(qualifiedName.with(name), schema, tableResolver, result, expand);
//            }
//            return result;
//        }
    }

    @Data
    class FlattenedStruct implements Flattened {

        private final SortedMap<String, PropertyMapping<?>> mappings;

        public FlattenedStruct(final Map<String, PropertyMapping<?>> mappings) {

            this.mappings = ImmutableSortedMap.copyOf(mappings);
        }

        @Override
        public PropertyMapping<Instance> nullable() {

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
        public Instance fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            final Instance instance = Flattened.super.fromRecord(qualifiedName, record, expand);
            if (Instance.getId(instance) == null) {
                return null;
            } else {
                return instance;
            }
        }

        @Override
        public PropertyMapping<Instance> nullable() {

            return new FlattenedRef(mappings.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().nullable()
            )));
        }
    }

    @Data
    class ExpandedRef implements PropertyMapping<Instance> {

        private final PropertyMapping<Instance> mapping;

        private final SchemaMapping schema;

        public ExpandedRef(final PropertyMapping<Instance> mapping, final SchemaMapping schema) {

            this.mapping = mapping;
            this.schema = schema;
        }

        @Override
        public Instance fromRecord(final Name qualifiedName, final RecordResolver record, final Set<Name> expand) {

            return mapping.fromRecord(qualifiedName, record, expand);
        }

        @Override
        public List<Field<?>> define(final Name qualifiedName, final NamingStrategy namingStrategy) {

            return mapping.define(qualifiedName, namingStrategy);
        }

        @Override
        public List<Field<?>> select(final Name qualifiedName, final ColumnResolver columnResolver) {

            return mapping.select(qualifiedName, columnResolver);
        }

        @Override
        public List<Pair<Name, SelectField<?>>> emit(final Name qualifiedName, final Instance value) {

            return mapping.emit(qualifiedName, value);
        }

        @Override
        public PropertyMapping<Instance> nullable() {

            return new ExpandedRef(mapping.nullable(), schema);
        }

        @Override
        public Set<Name> supportedExpand(final Set<Name> expand) {

            final Set<Name> result = new HashSet<>();
            result.add(Name.empty());
            result.addAll(schema.supportedExpand(expand));
            return result;
        }

//        @Override
//        public Field<?> field(final Name qualifiedName, final Name name, final ColumnResolver columnResolver) {
//
//            return mapping.field(qualifiedName, name, columnResolver);
//        }

    }
}
