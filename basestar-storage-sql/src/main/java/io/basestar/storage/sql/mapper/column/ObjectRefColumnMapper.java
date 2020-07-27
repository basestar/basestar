//package io.basestar.storage.sql.mapper.column;
//
//import io.basestar.schema.Instance;
//import io.basestar.schema.ObjectSchema;
//import io.basestar.schema.Reserved;
//import io.basestar.util.Name;
//import org.jooq.DataType;
//import org.jooq.Field;
//import org.jooq.Record;
//import org.jooq.Table;
//import org.jooq.impl.DSL;
//import org.jooq.impl.SQLDataType;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.function.Function;
//
//public class ObjectRefColumnMapper implements ColumnMapper<Map<String, Object>> {
//
//
//
//    @Override
//    public Map<Name, DataType<?>> columns(final String table, final Name name) {
//
//        final Map<Name, DataType<?>> result = new HashMap<>();
//        result.put(name, SQLDataType.LONGVARCHAR);
//        return result;
//    }
//
//    @Override
//    public Map<String, Object> toSQLValues(final String table, final Name name, final Map<String, Object> value) {
//
//        final Map<String, Object> result = new HashMap<>();
//        result.put(name, value == null ? null : Instance.getId(value));
//        if(expand != null) {
//        }
//        return result;
//    }
//
//    @Override
//    public Map<String, Object> fromSQLValues(final String table, final Name name, final Map<String, Object> values) {
//
//        final Object value = values.get(name);
//        if(value != null) {
//            return ObjectSchema.ref((String)value);
//        } else {
//            return null;
//        }
//    }
//
//    @Override
//    public Name resolve(final Name name) {
//
//        if(name.size() < 2) {
//            throw new IllegalStateException();
//        } else {
//            final Name rest = name.withoutFirst();
//            if(rest.equals(Reserved.ID_NAME)) {
//                return name.withoutLast();
//            } else {
//                final ColumnMapper<Object> mapper = mappers.get(rest.first());
//                return Name.of(Reserved.PREFIX + name.first()).with(mapper.absoluteName(rest));
//            }
//        }
//    }
//
//    @Override
//    public Table<Record> joined(final Name name, final Table<Record> source, final Function<ObjectSchema, Table<Record>> resolver) {
//
//        if(expand != null) {
//            final Table<Record> target = resolver.apply(type.getSchema()).as(tableName(name));
//            final Field<String> targetId = DSL.field(DSL.name(target.getUnqualifiedName(), DSL.name(Reserved.ID)), String.class);
//            final Field<String> sourceId = DSL.field(DSL.name(source.getUnqualifiedName(), columnName(name)), String.class);
//            return source.leftJoin(target).on(targetId.eq(sourceId));
//        } else {
//            return source;
//        }
//    }
//}
