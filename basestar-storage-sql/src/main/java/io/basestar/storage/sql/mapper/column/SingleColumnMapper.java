package io.basestar.storage.sql.mapper.column;

import io.basestar.storage.sql.mapper.TableResolver;
import io.basestar.util.Name;
import org.jooq.DataType;
import org.jooq.Record;
import org.jooq.Table;

import java.util.Collections;
import java.util.Map;

public interface SingleColumnMapper<T, V> extends ColumnMapper<T> {

    DataType<V> dataType();

    T marshall(V value);

    V unmarshall(T value);

    @Override
    default Map<Name, DataType<?>> columns(final String table, final Name name) {

        return Collections.singletonMap(qualifiedName(table, name), dataType());
    }

    @Override
    default Map<Name, String> select(final String table, final Name name) {

        return Collections.singletonMap(qualifiedName(table, name), selectName(table, name));
    }

    @Override
    default Map<String, Object> toSQLValues(final Name name, final T value) {

        if(value == null) {
            return Collections.singletonMap(simpleName(name), null);
        } else {
            final V converted = dataType().convert(unmarshall(value));
            return Collections.singletonMap(simpleName(name), converted);
        }
    }

    @Override
    default T fromSQLValues(final String table, final Name name, final Map<String, Object> values) {

        final Object value = values.get(selectName(table, name));
        if(value == null) {
            return null;
        } else {
            final V converted = dataType().convert(value);
            return marshall(converted);
        }
    }

    @Override
    default Name absoluteName(final String table, final Name head, final Name rest) {

        if(rest.size() != 1) {
            throw new IllegalStateException(rest + " not found in " + head);
        }
        return qualifiedName(table, head.with(rest));
    }

    @Override
    default Table<Record> joined(final String table, final Name name, final Table<Record> source,
                                 final TableResolver resolver) {

        return source;
    }
}
