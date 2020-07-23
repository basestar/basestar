package io.basestar.storage.sql.mapper;

import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.*;
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
import java.util.function.Function;

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

            return (ColumnMapper<T>)type.visit(new Use.Visitor<ColumnMapper<?>>() {

                private ColumnMapper<?> simple(final DataType<?> dataType) {

                    return ColumnMapper.simple(dataType.nullable(nullable), type);
                }

                @Override
                public ColumnMapper<?> visitBoolean(final UseBoolean type) {

                    return simple(SQLDataType.BOOLEAN);
                }

                @Override
                public ColumnMapper<?> visitInteger(final UseInteger type) {

                    return simple(SQLDataType.BIGINT);
                }

                @Override
                public ColumnMapper<?> visitNumber(final UseNumber type) {

                    return simple(SQLDataType.DOUBLE);
                }

                @Override
                public ColumnMapper<?> visitString(final UseString type) {

                    return simple(stringDataType);
                }

                @Override
                public ColumnMapper<?> visitEnum(final UseEnum type) {

                    return simple(stringDataType);
                }

                @Override
                public ColumnMapper<?> visitBinary(final UseBinary type) {

                    return simple(SQLDataType.LONGVARBINARY);
                }

                @Override
                public ColumnMapper<?> visitDate(final UseDate type) {

                    return simple(SQLDataType.LOCALDATE);
                }

                @Override
                public ColumnMapper<?> visitDateTime(final UseDateTime type) {

                    return simple(SQLDataType.LOCALDATETIME);
                }

                @Override
                public ColumnMapper<?> visitView(final UseView type) {

                    throw new UnsupportedOperationException();
                }

                @Override
                public <V> ColumnMapper<?> visitArray(final UseArray<V> type) {

                    return ColumnMapper.json(stringDataType, type);
                }

                @Override
                public <V> ColumnMapper<?> visitSet(final UseSet<V> type) {

                    return ColumnMapper.json(stringDataType, type);
                }

                @Override
                public <V> ColumnMapper<?> visitMap(final UseMap<V> type) {

                    return ColumnMapper.json(stringDataType, type);
                }

                @Override
                @SuppressWarnings("unchecked")
                public ColumnMapper<?> visitStruct(final UseStruct type) {

                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final String delimiter = Reserved.PREFIX;
                    final Map<String, ColumnMapper<Object>> mappers = new HashMap<>();
                    type.getSchema().getProperties().forEach((k, v) -> {
                        final Set<Name> branch = branches.get(k);
                        mappers.put(k, (ColumnMapper<Object>)columnMapper(v.getType(), !v.isRequired(), branch));
                    });
                    return new ColumnMapper<Map<String, Object>>() {
                        @Override
                        public Map<Name, DataType<?>> toColumns(final Name name) {

                            final Map<Name, DataType<?>> result = new HashMap<>();
                            mappers.forEach((k, v) -> {
                                final Name resolvedName = name.withoutLast().with(name.last() + delimiter + k);
                                result.putAll(v.toColumns(resolvedName));
                            });
                            return result;
                        }

                        @Override
                        public Map<String, Object> toSQLValues(final String name, final Map<String, Object> value) {

                            final Map<String, Object> result = new HashMap<>();
                            mappers.forEach((k, v) -> {
                                final String resolvedName = name + delimiter + k;
                                result.putAll(v.toSQLValues(resolvedName, value == null ? null : value.get(k)));
                            });
                            return result;
                        }

                        @Override
                        public Map<String, Object> fromSQLValues(final String name, final Map<String, Object> values) {

                            final Map<String, Object> result = new HashMap<>();
                            mappers.forEach((k, v) -> {
                                final String resolvedName = name + delimiter + k;
                                result.put(k, v.fromSQLValues(resolvedName, values));
                            });
                            return result;
                        }

                        @Override
                        public Name resolve(final Name name) {

                            if(name.size() < 2) {
                                throw new IllegalStateException();
                            } else {
                                final String first = name.first();
                                throw new IllegalStateException();
                            }
                        }

                        @Override
                        public Table<Record> joined(final Name name, final Table<Record> source, final Function<ObjectSchema, Table<Record>> resolver) {

                            return source;
                        }
                    };
                }

                @Override
                public ColumnMapper<?> visitObject(final UseObject type) {

                    final Map<String, ColumnMapper<Object>> mappers = new HashMap<>();
                    if(expand != null) {
                        final ObjectSchema schema = type.getSchema();
                        final Map<String, Set<Name>> branches = Name.branch(expand);
                        schema.metadataSchema().forEach((k, v) -> {
                            mappers.put(k, (ColumnMapper<Object>) columnMapper(v, true, Collections.emptySet()));
                        });
                        schema.getProperties().forEach((k, v) -> {
                            final Set<Name> branch = branches.get(k);
                            mappers.put(k, (ColumnMapper<Object>) columnMapper(v.getType(), !v.isRequired(), branch));
                        });
                    }

                    return new ColumnMapper<Map<String, Object>>() {
                        @Override
                        public Map<Name, DataType<?>> toColumns(final Name name) {

                            final Map<Name, DataType<?>> result = new HashMap<>();
                            result.put(name, stringDataType);
                            // Add schema-level expand here
//                            mappers.forEach((k, v) -> result.putAll(v.toColumns(name.with(k))));
                            return result;
                        }

                        @Override
                        public Map<String, Object> toSQLValues(final String name, final Map<String, Object> value) {

                            final Map<String, Object> result = new HashMap<>();
                            result.put(name, value == null ? null : Instance.getId(value));
                            if(expand != null) {
                            }
                            return result;
                        }

                        @Override
                        public Map<String, Object> fromSQLValues(final String name, final Map<String, Object> values) {

                            final Object value = values.get(name);
                            if(value != null) {
                                return ObjectSchema.ref((String)value);
                            } else {
                                return null;
                            }
                        }

                        @Override
                        public Name resolve(final Name name) {

                            if(name.size() < 2) {
                                throw new IllegalStateException();
                            } else {
                                final Name rest = name.withoutFirst();
                                if(rest.equals(Reserved.ID_NAME)) {
                                    return name.withoutLast();
                                } else {
                                    final ColumnMapper<Object> mapper = mappers.get(rest.first());
                                    return Name.of(Reserved.PREFIX + name.first()).with(mapper.resolve(rest));
                                }
                            }
                        }

                        @Override
                        public Table<Record> joined(final Name name, final Table<Record> source, final Function<ObjectSchema, Table<Record>> resolver) {

                            if(expand != null) {
                                final Table<Record> target = resolver.apply(type.getSchema()).as(tableName(name));
                                final Field<String> targetId = DSL.field(DSL.name(target.getUnqualifiedName(), DSL.name(Reserved.ID)), String.class);
                                final Field<String> sourceId = DSL.field(DSL.name(source.getUnqualifiedName(), columnName(name)), String.class);
                                return source.leftJoin(target).on(targetId.eq(sourceId));
                            } else {
                                return source;
                            }
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

