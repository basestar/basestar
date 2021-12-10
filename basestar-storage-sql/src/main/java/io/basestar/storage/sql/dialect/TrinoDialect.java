package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.core.type.TypeReference;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.use.UseRef;
import io.basestar.schema.use.UseString;
import io.basestar.storage.sql.resolver.ValueResolver;
import io.basestar.util.Name;
import org.jooq.Configuration;
import org.jooq.DataType;
import org.jooq.QueryPart;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;

import java.util.Map;

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
    public DataType<?> refType(final UseRef type) {

        return new RefDataType<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Instance refFromSQLValue(final UseRef type, final ValueResolver value) {

        final Object v = value.value();
        if (v == null) {
            return null;
        } else {
            final Map<String, ?> map = fromJson(v, new TypeReference<Map<String, ?>>() {
            });
            return map == null ? null : new Instance((Map<String, Object>) map);
        }
    }

    @Override
    public QueryPart refIdField(final UseRef type, final Name name) {

        final Name rest = name.withoutFirst();
        if (rest.equals(ObjectSchema.ID_NAME)) {
            return DSL.field(DSL.name(name.first(), name.last()), String.class);
        } else {
            throw new UnsupportedOperationException("Query of this type is not supported");
        }
    }

//    @Override
//    public <T> DataType<?> arrayType(final UseArray<T> type) {
//
//        return dataType(type.getType()).getArrayDataType();
//    }
//
//    @Override
//    public <T> DataType<?> setType(final UseSet<T> type) {
//
//        return dataType(type.getType()).getArrayDataType();
//    }
//
//    @Override
//    public <T> DataType<?> pageType(final UsePage<T> type) {
//
//        return dataType(type.getType()).getArrayDataType();
//    }
//
//    @Override
//    public <T> DataType<?> mapType(final UseMap<T> type) {
//
//        return new MapDataType<>(dataType(type.getType()));
//    }
//
//    @Override
//    public <T> Object arrayToSQLValue(final UseArray<T> type, final List<T> value) {
//
//        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
//    }
//
//    @Override
//    public <T> Object pageToSQLValue(final UsePage<T> type, final Page<T> value) {
//
//        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
//    }
//
//    @Override
//    public <T> Object setToSQLValue(final UseSet<T> type, final Set<T> value) {
//
//        return ((Collection<?>)value).stream().map(v -> toSQLValue(type.getType(), v)).toArray();
//    }
//
//    @Override
//    public <T> List<T> arrayFromSQLValue(final UseArray<T> type, final Object value) {
//
//        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toList()));
//    }
//
//    @Override
//    public <T> Page<T> pageFromSQLValue(final UsePage<T> type, final Object value) {
//
//        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toList()));
//    }
//
//    @Override
//    public <T> Set<T> setFromSQLValue(final UseSet<T> type, final Object value) {
//
//        return type.create(Arrays.stream((Object[]) value).map(v -> fromSQLValue(type.getType(), v)).collect(Collectors.toSet()));
//    }

//    static class MapDataType<T> extends DefaultDataType<Map<String, T>> {
//
//        final DataType<T> valueType;
//
//        @SuppressWarnings("unchecked")
//        public MapDataType(final DataType<T> valueType) {
//
//            super(null, (Class<Map<String, T>>)(Class<?>)Map.class, valueType.getTypeName());
//            this.valueType = valueType;
//        }
//
//        @Override
//        public final String getTypeName(Configuration configuration) {
//
//            final String typeName = this.valueType.getTypeName(configuration);
//            return getMapType(configuration, typeName);
//        }
//
//        @Override
//        public final String getCastTypeName(Configuration configuration) {
//
//            final String castTypeName = this.valueType.getCastTypeName(configuration);
//            return getMapType(configuration, castTypeName);
//        }
//
//        private static String getMapType(final Configuration configuration, final String dataType) {
//
//            return "map<varchar," + dataType + ">";
//        }
//    }

    static class RefDataType<T> extends DefaultDataType<Map<String, String>> {

        @SuppressWarnings("unchecked")
        public RefDataType() {

            super(null, (Class<Map<String, String>>)(Class<?>)Map.class, getRefType());
        }

        @Override
        public final String getTypeName(final Configuration configuration) {

            return getRefType();
        }

        @Override
        public final String getCastTypeName(final Configuration configuration) {

            return getRefType();
        }

        private static String getRefType() {

            return "row(id varchar)";
        }
    }

    @Override
    public boolean supportsUDFs() {

        return false;
    }

    @Override
    public QueryPart bind(final Object value) {

        // Use inline rather than val because some JDBC drivers (Athena) don't support positional params
        return DSL.inline(value);
    }
}
