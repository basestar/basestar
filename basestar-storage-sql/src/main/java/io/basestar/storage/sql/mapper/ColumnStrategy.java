package io.basestar.storage.sql.mapper;

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.*;
import io.basestar.storage.sql.mapper.column.ColumnMapper;
import io.basestar.storage.sql.mapper.column.FlatColumnMapper;
import io.basestar.storage.sql.mapper.column.JsonColumnMapper;
import io.basestar.storage.sql.mapper.column.SimpleColumnMapper;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import org.jooq.DataType;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public interface ColumnStrategy {

    <T> ColumnMapper<T> columnMapper(Use<T> type, boolean nullable, Set<Name> expand);

    class Simple implements ColumnStrategy {

        public enum StructMode {

            SQL,
            JSON,
            FLAT
        }

        private final DataType<String> stringDataType;

        private final StructMode structMode;

        @lombok.Builder
        Simple(final DataType<String> stringDataType, final StructMode structMode) {

            this.stringDataType = Nullsafe.option(stringDataType, SQLDataType.LONGVARCHAR);
            this.structMode = Nullsafe.option(structMode, StructMode.FLAT);
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> ColumnMapper<T> columnMapper(final Use<T> type, final boolean nullable, final Set<Name> expand) {

            return type.visit(new Use.Visitor<ColumnMapper<T>>() {

                protected <V> ColumnMapper<T> simple(final DataType<V> dataType) {

                    return new SimpleColumnMapper<>(dataType.nullable(nullable), type::create, v -> (V)type.create(v));
                }

                protected ColumnMapper<T> json() {

                    return new JsonColumnMapper<>(stringDataType, type::create, v -> v);
                }

                @Override
                public ColumnMapper<T> visitBoolean(final UseBoolean type) {

                    return simple(SQLDataType.BOOLEAN);
                }

                @Override
                public ColumnMapper<T> visitInteger(final UseInteger type) {

                    return simple(SQLDataType.BIGINT);
                }

                @Override
                public ColumnMapper<T> visitNumber(final UseNumber type) {

                    return simple(SQLDataType.DOUBLE);
                }

                @Override
                public ColumnMapper<T> visitString(final UseString type) {

                    return simple(stringDataType);
                }

                @Override
                public ColumnMapper<T> visitEnum(final UseEnum type) {

                    return simple(stringDataType);
                }

                @Override
                public ColumnMapper<T> visitBinary(final UseBinary type) {

                    return simple(SQLDataType.LONGVARBINARY);
                }

                @Override
                public ColumnMapper<T> visitDate(final UseDate type) {

                    return simple(SQLDataType.LOCALDATE);
                }

                @Override
                public ColumnMapper<T> visitDateTime(final UseDateTime type) {

                    return simple(SQLDataType.LOCALDATETIME);
                }

                @Override
                public ColumnMapper<T> visitView(final UseView type) {

                    throw new UnsupportedOperationException();
                }

                @Override
                @SuppressWarnings("unchecked")
                public <V> ColumnMapper<T> visitOptional(final UseOptional<V> type) {

                    return columnMapper((Use<T>)type.getType(), true, expand);
                }

                @Override
                public <V> ColumnMapper<T> visitArray(final UseArray<V> type) {

                    return json();
                }

                @Override
                public <V> ColumnMapper<T> visitSet(final UseSet<V> type) {

                    return json();
                }

                @Override
                public <V> ColumnMapper<T> visitMap(final UseMap<V> type) {

                    return json();
                }

                @Override
                @SuppressWarnings("unchecked")
                public ColumnMapper<T> visitStruct(final UseStruct type) {

                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final String delimiter = "_";
                    final Map<String, ColumnMapper<Object>> mappers = new HashMap<>();
                    type.getSchema().getProperties().forEach((k, v) -> {
                        final Set<Name> branch = branches.get(k);
                        mappers.put(k, (ColumnMapper<Object>)columnMapper(v.getType(), false, branch));
                    });
                    return new FlatColumnMapper<T>(mappers, v -> (T)type.create(v), type::create, delimiter);
                }

                @Override
                public ColumnMapper<T> visitObject(final UseObject type) {

                    final Map<String, ColumnMapper<Object>> mappers = new HashMap<>();
                    if(expand != null) {
                        final ObjectSchema schema = type.getSchema();
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        schema.metadataSchema().forEach((k, v) -> {
                            mappers.put(k, (ColumnMapper<Object>) columnMapper(v, false, Collections.emptySet()));
                        });
                        schema.getProperties().forEach((k, v) -> {
                            final Set<Name> branch = branches.get(k);
                            mappers.put(k, (ColumnMapper<Object>) columnMapper(v.getType(), false, branch));
                        });
                    }

                    return (ColumnMapper<T>)new ColumnMapper<Map<String, Object>>() {

                        @Override
                        public Map<Name, DataType<?>> columns(final String table, final Name name) {

                            final Map<Name, DataType<?>> result = new HashMap<>();
                            result.put(qualifiedName(table, name), SQLDataType.LONGVARCHAR);
                            return result;
                        }

                        @Override
                        public Map<Name, String> select(final String table, final Name name) {

                            final Map<Name, String> result = new HashMap<>();
                            result.put(qualifiedName(table, name), selectName(table, name));
                            mappers.forEach((k, v) -> {
//                                final Name nextName = name.with(k);
                                final String newTable = tableName(table, k);
                                result.putAll(v.select(newTable, Name.of(k)));
                            });
                            return result;
                        }

                        @Override
                        public Map<String, Object> toSQLValues(final Name name, final Map<String, Object> value) {

                            final Map<String, Object> result = new HashMap<>();
                            result.put(simpleName(name), value == null ? null : Instance.getId(value));
                            if(expand != null) {
                            }
                            return result;
                        }

                        @Override
                        public Map<String, Object> fromSQLValues(final String table, final Name name, final Map<String, Object> values) {

                            final Object value = values.get(selectName(table, name));
                            if(value != null) {
                                return ObjectSchema.ref((String)value);
                            } else {
                                return null;
                            }
                        }

                        @Override
                        public Name absoluteName(final String table, final Name head, final Name rest) {

                            if(rest.size() < 2) {
                                throw new IllegalStateException(rest + " not found in " + head);
                            }
                            final String outer = rest.first();
                            final String inner = rest.get(1);
                            if(inner.equals(ObjectSchema.ID) && rest.size() == 2) {
                                return qualifiedName(table, head.with(rest.withoutLast()));
                            }
                            final ColumnMapper<?> mapper = mappers.get(inner);
                            if(mapper == null) {
                                throw new IllegalStateException("Object column " + inner + " not found in " + head);
                            }
                            final String newTable = tableName(table, outer);
                            return mapper.absoluteName(newTable, head, rest.withoutFirst());
                        }

                        private String tableName(final String table, final String name) {

                            return table + Reserved.PREFIX + name;
                        }

                        @Override
                        public Table<Record> joined(final String table, final Name name, final Table<Record> source,
                                                    final TableResolver resolver) {

                            if(expand != null) {
                                final String targetTableName = tableName(name);
                                final Table<Record> target = resolver.table(type.getSchema()).as(targetTableName);
                                final Name sourceColumn = qualifiedName(table, name);
                                final Field<String> targetId = DSL.field(DSL.name(DSL.name(targetTableName), DSL.name(ObjectSchema.ID)), String.class);
                                final Field<String> sourceId = DSL.field(DSL.name(DSL.name(table), columnName(sourceColumn)), String.class);
                                final Table<Record> initial = source.leftJoin(target).on(targetId.eq(sourceId));
                                return joinAll(targetTableName, name, initial, resolver);
                            } else {
                                return source;
                            }
                        }

                        private Table<Record> joinAll(final String table, final Name name, final Table<Record> source,
                                                      final TableResolver resolver) {

                            Table<Record> target = source;
                            for(final Map.Entry<String, ColumnMapper<Object>> entry : mappers.entrySet()) {
                                final Name nextName = name.with(entry.getKey());
                                target = entry.getValue().joined(table, nextName, target, resolver);
                            }
                            return target;
                        }
                    };
                }

                private org.jooq.Name tableName(final Name name) {

                    return DSL.name(Reserved.PREFIX + name.toString(Reserved.PREFIX));
                }

                private org.jooq.Name columnName(final Name name) {

                    return DSL.name(name.toArray());
                }
            });
        }

    }

}

