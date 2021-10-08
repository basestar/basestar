package io.basestar.storage.sql.mapper;

import io.basestar.schema.InstanceSchema;
import io.basestar.schema.Reserved;
import io.basestar.storage.sql.mapper.column.ColumnMapper;
import io.basestar.util.Name;
import org.jooq.*;
import org.jooq.impl.DSL;

import java.util.*;
import java.util.stream.Collectors;

public interface RowMapper<T> {

    List<Field<?>> columns();

    T fromRecord(Record record);

    Map<Field<?>, Object> toRecord(T record);

    Field<?> resolve(Name name);

    Table<Record> joined(Table<Record> source, TableResolver resolver);

    List<SelectFieldOrAsterisk> select();

    default List<T> all(final Result<Record> result) {

        return result.stream()
                .map(this::fromRecord)
                .collect(Collectors.toList());
    }

    default T first(final Result<Record> result) {

        if (result.isEmpty()) {
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
            mappers.put(k, (ColumnMapper<Object>) strategy.columnMapper(v, false, Collections.emptySet()));
        });
        schema.getProperties().forEach((k, v) -> {
            final Set<Name> branch = branches.get(k);
            mappers.put(k, (ColumnMapper<Object>) strategy.columnMapper(v.typeOf(), false, branch));
        });

        final String rootTable = Reserved.PREFIX;

        return new RowMapper<Map<String, Object>>() {

            @Override
            public List<Field<?>> columns() {

                final List<Field<?>> result = new ArrayList<>();
                mappers.forEach((k, v) -> v.columns(rootTable, Name.of(k)).forEach((k2, v2) -> {
                    result.add(DSL.field(DSL.name(k2.toArray()), v2));
                }));
                return result;
            }

            @Override
            public List<SelectFieldOrAsterisk> select()  {

                final List<SelectFieldOrAsterisk> result = new ArrayList<>();
                mappers.forEach((k, v) -> v.select(rootTable, Name.of(k)).forEach((k2, v2) -> {
                    result.add(DSL.field(DSL.name(k2.toArray())).as(v2));
                }));
                return result;
            }

            @Override
            public Map<String, Object> fromRecord(final Record record) {

                final Map<String, Object> values = record.intoMap();
                final Map<String, Object> result = new HashMap<>();
                mappers.forEach((name, mapper) -> {
                    result.put(name, mapper.fromSQLValues(rootTable, Name.of(name), values));
                });
                return result;
            }

            @Override
            public Map<Field<?>, Object> toRecord(final Map<String, Object> record) {

                final Map<Field<?>, Object> result = new HashMap<>();
                mappers.forEach((name, mapper) -> {
                    final Object value = record.get(name);
                    mapper.toSQLValues(Name.of(name), value).forEach((k, v) -> {
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
                final Name resolved = mapper.absoluteName(rootTable, Name.of(), name);
                return DSL.field(DSL.name(resolved.toArray()));
            }

            @Override
            public Table<Record> joined(final Table<Record> source, final TableResolver resolver) {

                Table<Record> result = source.as(DSL.name(rootTable));
                final Map<String, Set<Name>> branches = Name.branch(expand);
                for(final Map.Entry<String, ColumnMapper<Object>> entry : mappers.entrySet()) {
                    final String name = entry.getKey();
                    final ColumnMapper<Object> mapper = entry.getValue();
                    final Set<Name> branch = branches.get(name);
                    if(branch != null) {
                        result = mapper.joined(rootTable, Name.of(name), result, resolver);
                    }
                }
                return result;
            }
        };
    }
}
