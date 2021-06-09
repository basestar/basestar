package io.basestar.storage.sql.dialect;

import io.basestar.schema.use.*;
import io.basestar.util.Page;
import org.jooq.Configuration;
import org.jooq.DataType;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import java.util.*;
import java.util.stream.Collectors;

public class TrinoDialect extends JSONDialect {

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return SQLDialect.POSTGRES;
    }

    @Override
    public SQLDialect ddlDialect() {

        return SQLDialect.HSQLDB;
    }

    @Override
    protected boolean isJsonEscaped() {

        return false;
    }

    @Override
    public boolean supportsConstraints() {

        return false;
    }

    @Override
    public boolean supportsIndexes() {

        return false;
    }

    @Override
    public boolean supportsILike() {

        return false;
    }

    @Override
    public DataType<?> stringType(final UseString type) {

        return SQLDataType.VARCHAR;
    }

    @Override
    public <T> DataType<?> arrayType(final UseArray<T> type) {

        return dataType(type.getType()).getArrayDataType();
    }

    @Override
    public <T> DataType<?> setType(final UseSet<T> type) {

        return dataType(type.getType()).getArrayDataType();
    }

    @Override
    public <T> DataType<?> pageType(final UsePage<T> type) {

        return dataType(type.getType()).getArrayDataType();
    }

    @Override
    public <T> DataType<?> mapType(final UseMap<T> type) {

        return new MapDataType<>(dataType(type.getType()));
    }

    @Override
    public <T> Object arrayToSQLValue(final UseArray<T> type, final List<T> value) {

        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
    }

    @Override
    public <T> Object pageToSQLValue(final UsePage<T> type, final Page<T> value) {

        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
    }

    @Override
    public <T> Object setToSQLValue(final UseSet<T> type, final Set<T> value) {

        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
    }

    @Override
    public <T> List<T> arrayFromSQLValue(final UseArray<T> type, final Object value) {

        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toList()));
    }

    @Override
    public <T> Page<T> pageFromSQLValue(final UsePage<T> type, final Object value) {

        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toList()));
    }

    @Override
    public <T> Set<T> setFromSQLValue(final UseSet<T> type, final Object value) {

        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toSet()));
    }

    static class MapDataType<T> extends DefaultDataType<Map<String, T>> {

        final DataType<T> valueType;

        @SuppressWarnings("unchecked")
        public MapDataType(final DataType<T> valueType) {

            super(null, (Class<Map<String, T>>)(Class<?>)Map.class, valueType.getTypeName());
            this.valueType = valueType;
        }

        @Override
        public final String getTypeName(Configuration configuration) {

            final String typeName = this.valueType.getTypeName(configuration);
            return getMapType(configuration, typeName);
        }

        @Override
        public final String getCastTypeName(Configuration configuration) {

            final String castTypeName = this.valueType.getCastTypeName(configuration);
            return getMapType(configuration, castTypeName);
        }

        private static String getMapType(final Configuration configuration, final String dataType) {

            return "map<varchar," + dataType + ">";
        }
    }
}
