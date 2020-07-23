package io.basestar.storage.sql.mapper;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.util.Name;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface RowMapper<T> {

    Name ROOT = Name.of(Reserved.PREFIX);

    List<Field<?>> columns();

    T fromRecord(Record record);

    Map<Field<?>, Object> toRecord(T record);

    Field<?> resolve(Name name);

    Table<Record> joined(Table<Record> source, Function<ObjectSchema, Table<Record>> resolver);

    default List<SelectFieldOrAsterisk> select() {

        // Select-as-name to avoid issue with athena lowercased column names
        return columns().stream().map(field -> field.as(DSL.name(field.getName())))
                .collect(Collectors.toList());
    }

    default List<T> all(Result<Record> result) {

        return result.stream()
                .map(this::fromRecord)
                .collect(Collectors.toList());
    }

    default T first(Result<Record> result) {

        if(result.isEmpty()) {
            return null;
        } else {
            return fromRecord(result.iterator().next());
        }
    }

    @SuppressWarnings("unchecked")
    static RowMapper<Map<String, Object>> forInstance(final ColumnStrategy strategy, final InstanceSchema schema, final Set<Name> expand) {

        final Map<String, ColumnMapper<Object>> mappers = new HashMap<>();
        final Map<String, Set<Name>> branches = Name.branch(expand);
        schema.metadataSchema().forEach((k, v) -> {
            mappers.put(k, (ColumnMapper<Object>) strategy.columnMapper(v, true, Collections.emptySet()));
        });
        schema.getProperties().forEach((k, v) -> {
            final Set<Name> branch = branches.get(k);
            mappers.put(k, (ColumnMapper<Object>) strategy.columnMapper(v.getType(), !v.isRequired(), branch));
        });

        return new RowMapper<Map<String, Object>>() {

            @Override
            public List<Field<?>> columns() {

                final List<Field<?>> result = new ArrayList<>();
                mappers.forEach((k, v) -> v.toColumns(Name.of(k)).forEach((k2, v2) -> {
                    result.add(DSL.field(DSL.name(ROOT.with(k2).toArray()), v2));
                }));
                return result;
            }

            @Override
            public Map<String, Object> fromRecord(final Record record) {

                final Map<String, Object> values = record.intoMap();
                final Map<String, Object> result = new HashMap<>();
                mappers.forEach((name, mapper) -> {
                    result.put(name, mapper.fromSQLValues(name, values));
                });
                return result;
            }

            @Override
            public Map<Field<?>, Object> toRecord(final Map<String, Object> record) {

                final Map<Field<?>, Object> result = new HashMap<>();
                mappers.forEach((name, mapper) -> {
                    final Object value = record.get(name);
                    mapper.toSQLValues(name, value).forEach((k, v) -> {
                        result.put(DSL.field(DSL.name(k)), v);
                    });
                });
                return result;
            }

            @Override
            public Field<?> resolve(final Name name) {

                if(name.isEmpty()) {
                    throw new IllegalStateException();
                }
                final ColumnMapper<?> mapper = mappers.get(name.first());
                if(mapper == null) {
                    throw new IllegalStateException();
                }
                Name resolved = mapper.resolve(name);
                if(!resolved.first().startsWith(Reserved.PREFIX)) {
                    resolved = ROOT.with(resolved);
                }
                return DSL.field(DSL.name(resolved.toArray()));
            }

            @Override
            public Table<Record> joined(final Table<Record> source, final Function<ObjectSchema, Table<Record>> resolver) {

                Table<Record> result = source.as(DSL.name(ROOT.toString()));
                final Map<String, Set<Name>> branches = Name.branch(expand);
                for(final Map.Entry<String, ColumnMapper<Object>> entry : mappers.entrySet()) {
                    final String name = entry.getKey();
                    final ColumnMapper<Object> mapper = entry.getValue();
                    final Set<Name> branch = branches.get(name);
                    if(branch != null) {
                        result = mapper.joined(Name.of(name), result, resolver);
                    }
                }
                return result;
            }
        };
    }
}
