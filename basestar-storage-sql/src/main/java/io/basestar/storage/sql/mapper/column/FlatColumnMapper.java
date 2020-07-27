package io.basestar.storage.sql.mapper.column;

import io.basestar.schema.Reserved;
import io.basestar.storage.sql.mapper.TableResolver;
import io.basestar.util.Name;
import lombok.RequiredArgsConstructor;
import org.jooq.DataType;
import org.jooq.Record;
import org.jooq.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

@RequiredArgsConstructor
public class FlatColumnMapper<T> implements ColumnMapper<T> {

    private final Map<String, ColumnMapper<Object>> mappers;

    private final Function<Map<String, Object>, T> marshaller;

    private final Function<T, Map<String, Object>> unmarshaller;

    private final String delimiter;

    public FlatColumnMapper(final Map<String, ColumnMapper<Object>> mappers,
                            final Function<Map<String, Object>, T> marshaller,
                            final Function<T, Map<String, Object>> unmarshaller) {

        this(mappers, marshaller, unmarshaller, Reserved.PREFIX);
    }

    protected String columnName(final String a, final String b) {

        return a + delimiter + b;
    }

    protected Name columnName(final Name a, final String b) {

        return a.withoutLast().with(columnName(a.last(), b));
    }

    @Override
    public Map<Name, DataType<?>> columns(final String table, final Name name) {

        final Map<Name, DataType<?>> result = new HashMap<>();
        mappers.forEach((k, v) -> {
            final Name columnName = columnName(name, k);
            result.putAll(v.columns(table, columnName));
        });
        return result;
    }

    @Override
    public Map<Name, String> select(final String table, final Name name) {

        final Map<Name, String> result = new HashMap<>();
        mappers.forEach((k, v) -> {
            final Name columnName = columnName(name, k);
            result.putAll(v.select(table, columnName));
        });
        return result;
    }

    @Override
    public Map<String, Object> toSQLValues(final Name name, final T value) {

        final Map<String, Object> result = new HashMap<>();
        final Map<String, Object> unmarshalled = value == null ? null : unmarshaller.apply(value);
        mappers.forEach((k, v) -> {
            final Name columnName = columnName(name, k);
            result.putAll(v.toSQLValues(columnName, unmarshalled == null ? null : unmarshalled.get(k)));
        });
        return result;
    }

    @Override
    public T fromSQLValues(final String table, final Name name, final Map<String, Object> values) {

        final Map<String, Object> result = new HashMap<>();
        mappers.forEach((k, v) -> {
            final Name columnName = columnName(name, k);
            result.put(k, v.fromSQLValues(table, columnName, values));
        });
        return marshaller.apply(result);
    }

    @Override
    public Name absoluteName(final String table, final Name head, final Name rest) {

        if(rest.size() < 2) {
            throw new IllegalStateException(rest + " not found in " + head);
        }
        final String outer = rest.first();
        final String inner = rest.get(1);
        final ColumnMapper<?> mapper = mappers.get(inner);
        if(mapper == null) {
            throw new IllegalStateException("Flattened column " + inner + " not found in " + head);
        }
        final Name next = Name.of(columnName(outer, inner)).with(rest.withoutFirst(2));
        return mapper.absoluteName(table, head, next);
    }

    @Override
    public Table<Record> joined(final String table, final Name name, final Table<Record> source,
                                final TableResolver resolver) {

        return source;
    }
}
