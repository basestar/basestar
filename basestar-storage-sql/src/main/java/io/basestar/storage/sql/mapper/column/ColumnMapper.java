package io.basestar.storage.sql.mapper.column;

import io.basestar.schema.Reserved;
import io.basestar.storage.sql.mapper.TableResolver;
import io.basestar.util.Name;
import org.jooq.DataType;
import org.jooq.Record;
import org.jooq.Table;

import java.util.Map;

public interface ColumnMapper<T> {

    Map<Name, DataType<?>> columns(String table, Name column);

    Map<Name, String> select(String table, Name column);

    Map<String, Object> toSQLValues(Name column, T value);

    T fromSQLValues(String table, Name column, Map<String, Object> values);

    // In other methods, name can be built recursively, but in this method, full name is provided in
    // 'rest' and target name will be built recursively using 'head', then returned as a fully
    // qualified name (with table)

    Name absoluteName(String table, Name head, Name rest);

    Table<Record> joined(String table, Name name, Table<Record> source, TableResolver resolver);

    default String simpleName(final Name name) {

        assert name.size() == 1;
        return name.toString();
    }

    default Name qualifiedName(final String table, final Name name) {

        return Name.of(table).with(name);
    }

    default String selectName(final String table, final Name name) {

        return qualifiedName(table, name).toString(Reserved.PREFIX);
    }

    default String tableName(final Name name) {

        return Reserved.PREFIX + name.toString(Reserved.PREFIX);
    }
}
