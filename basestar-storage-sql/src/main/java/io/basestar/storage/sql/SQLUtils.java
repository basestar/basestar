package io.basestar.storage.sql;

/*-
 * #%L
 * basestar-storage-sql
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.*;
import io.basestar.util.Name;
import io.basestar.util.*;
import org.apache.commons.text.StringEscapeUtils;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static DataType<?> dataType(final Use<?> type) {

        return type.visit(new Use.Visitor<DataType<?>>() {

            @Override
            public DataType<?> visitBoolean(final UseBoolean type) {

                return SQLDataType.BOOLEAN;
            }

            @Override
            public DataType<?> visitInteger(final UseInteger type) {

                return SQLDataType.BIGINT;
            }

            @Override
            public DataType<?> visitNumber(final UseNumber type) {

                return SQLDataType.DOUBLE;
            }

            @Override
            public DataType<?> visitString(final UseString type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public DataType<?> visitEnum(final UseEnum type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public DataType<?> visitRef(final UseRef type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public <T> DataType<?> visitArray(final UseArray<T> type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public <T> DataType<?> visitPage(final UsePage<T> type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public <T> DataType<?> visitSet(final UseSet<T> type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public <T> DataType<?> visitMap(final UseMap<T> type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public DataType<?> visitStruct(final UseStruct type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public DataType<?> visitBinary(final UseBinary type) {

                return SQLDataType.LONGVARBINARY;
            }

            @Override
            public DataType<?> visitDate(final UseDate type) {

                return SQLDataType.LOCALDATE;
            }

            @Override
            public DataType<?> visitDateTime(final UseDateTime type) {

                return SQLDataType.TIMESTAMP;
            }

            @Override
            public DataType<?> visitView(final UseView type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public <T> DataType<?> visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this).nullable(true);
            }

            @Override
            public DataType<?> visitAny(final UseAny type) {

                return SQLDataType.LONGVARCHAR;
            }

            @Override
            public DataType<?> visitSecret(final UseSecret type) {

                return SQLDataType.LONGVARBINARY;
            }
        });
    }

    public static Object toSQLValue(final Use<?> type, final Object value) {

        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Boolean visitBoolean(final UseBoolean type) {

                return type.create(value);
            }

            @Override
            public Long visitInteger(final UseInteger type) {

                return type.create(value);
            }

            @Override
            public Number visitNumber(final UseNumber type) {

                return type.create(value);
            }

            @Override
            public String visitString(final UseString type) {

                return type.create(value);
            }

            @Override
            public String visitEnum(final UseEnum type) {

                return type.create(value);
            }

            @Override
            @SuppressWarnings("unchecked")
            public String visitRef(final UseRef type) {

                if(value == null) {
                    return null;
                } else {
                    final Map<String, Object> instance = (Map<String, Object>)value;
                    return Instance.getId(instance);
                }
            }

            private String toJson(final Object value) {

                if(value == null) {
                    return null;
                }
                try {
                    return objectMapper.writeValueAsString(value);
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public <T> String visitArray(final UseArray<T> type) {

                return toJson(value);
            }

            @Override
            public <T> String visitPage(final UsePage<T> type) {

                return toJson(value);
            }

            @Override
            public <T> String visitSet(final UseSet<T> type) {

                return toJson(value);
            }

            @Override
            public <T> String visitMap(final UseMap<T> type) {

                return toJson(value);
            }

            @Override
            public String visitStruct(final UseStruct type) {

                return toJson(value);
            }

            @Override
            public String visitAny(final UseAny type) {

                return toJson(value);
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return Nullsafe.map(type.create(value), v -> v.getBytes());
            }

            @Override
            public Object visitDate(final UseDate type) {

                return type.create(value);
            }

            @Override
            public Object visitDateTime(final UseDateTime type) {

                return type.create(value);
            }

            @Override
            public Object visitView(final UseView type) {

                return toJson(value);
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                if(value == null) {
                    return null;
                } else {
                    return type.getType().visit(this);
                }
            }

            @Override
            @SuppressWarnings(Warnings.RETURN_NULL_ARRAY_OR_COLLECTION)
            public byte[] visitSecret(final UseSecret type) {

                if(value == null) {
                    return null;
                } else {
                    return type.create(value).encrypted();
                }
            }
        });
    }

    public static Object fromSQLValue(final Use<?> type, final Object value) {

        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Boolean visitBoolean(final UseBoolean type) {

                return type.create(value);
            }

            @Override
            public Long visitInteger(final UseInteger type) {

                return type.create(value);
            }

            @Override
            public Number visitNumber(final UseNumber type) {

                return type.create(value);
            }

            @Override
            public String visitString(final UseString type) {

                return type.create(value);
            }

            @Override
            public String visitEnum(final UseEnum type) {

                return type.create(value);
            }

            @Override
            public Map<String, Object> visitRef(final UseRef type) {

                if(value == null) {
                    return null;
                } else {
                    final String id = (String)value;
                    return ReferableSchema.ref(id);
                }
            }

            private <T> T fromJson(final Object value, final TypeReference<T> ref) {

                if(value == null) {
                    return null;
                }
                try {
                    final String str;
                    if(value instanceof String) {
                        str = (String) value;
                    } else if(value instanceof byte[]) {
                        str = new String((byte[])value, StandardCharsets.UTF_8);
                    } else if(value instanceof JSON) {
                        str = unescape(((JSON)value).data());
                    } else if(value instanceof JSONB) {
                        str = unescape(((JSONB)value).data());
                    } else {
                        throw new IllegalStateException();
                    }
                    return objectMapper.readValue(str, ref);
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            // FIXME
            private String unescape(final String data) {

                return StringEscapeUtils.unescapeJava(data.substring(1, data.length() - 1));
            }

            @Override
            public <T> Collection<T> visitArray(final UseArray<T> type) {

                final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
                return type.create(results);
            }

            @Override
            public <T> Collection<T> visitPage(final UsePage<T> type) {

                final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
                return type.create(results);
            }

            @Override
            public <T> Collection<T> visitSet(final UseSet<T> type) {

                final Collection<?> results = fromJson(value, new TypeReference<Collection<?>>() {});
                return type.create(results);
            }

            @Override
            public <T> Map<String, T> visitMap(final UseMap<T> type) {

                return type.create(fromJson(value, new TypeReference<Map<String, ?>>() {}));
            }

            @Override
            public Map<String, Object> visitStruct(final UseStruct type) {

                return type.create(fromJson(value, new TypeReference<Map<String, Object>>() {}));
            }

            @Override
            public Bytes visitBinary(final UseBinary type) {

                return type.create(value);
            }

            @Override
            public LocalDate visitDate(final UseDate type) {

                return type.create(value);
            }

            @Override
            public Instant visitDateTime(final UseDateTime type) {

                return type.create(value);
            }

            @Override
            public Object visitView(final UseView type) {

                return type.create(fromJson(value, new TypeReference<Map<String, Object>>() {}));
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                if(value == null) {
                    return null;
                } else {
                    return type.getType().visit(this);
                }
            }

            @Override
            public Object visitAny(final UseAny type) {

                return type.create(fromJson(value, new TypeReference<Object>() {}));
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return type.create(value);
            }
        });
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static List<Field<?>> fields(final ReferableSchema schema) {

        return Stream.concat(
                schema.metadataSchema().entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()), dataType(e.getValue()))),
                schema.getProperties().entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()),
                                dataType(e.getValue().typeOf())))
        ).collect(Collectors.toList());
    }

    private static SortOrder sort(final Sort.Order order) {

        return order == Sort.Order.ASC ? SortOrder.ASC : SortOrder.DESC;
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static List<OrderField<?>> indexKeys(final ReferableSchema schema, final Index index) {

        return Stream.concat(
                index.getPartition().stream().map(v -> indexField(schema, index, v)),
                index.getSort().stream().map(v -> indexField(schema, index, v.getName())
                        .sort(sort(v.getOrder())))
        ).collect(Collectors.toList());
    }

    private static Field<Object> indexField(final ReferableSchema schema, final Index index, final Name name) {

        // FIXME: BUG: hacky heuristic
        if(ReferableSchema.ID.equals(name.last())) {
            return DSL.field(DSL.name(name.withoutLast().toString()));
        } else {
            return DSL.field(DSL.name(name.toString()));
        }
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static List<Field<?>> fields(final ReferableSchema schema, final Index index) {

        final List<Name> partitionNames = index.resolvePartitionNames();
        final List<Sort> sortPaths = index.getSort();

        return Stream.of(
                partitionNames.stream()
                        .map(v -> DSL.field(columnName(v), dataType(schema.typeOf(v)).nullable(true))),
                sortPaths.stream()
                        .map(Sort::getName)
                        .map(v -> DSL.field(columnName(v), dataType(schema.typeOf(v)).nullable(true))),
                index.projectionSchema(schema).entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()), dataType(e.getValue()).nullable(true)))

        ).flatMap(v -> v).collect(Collectors.toList());
    }

    public static Name columnPath(final Name v) {

        return Name.of(v.toString(Reserved.PREFIX));
    }

    public static org.jooq.Name columnName(final Name v) {

        return DSL.name(v.toString(Reserved.PREFIX));
    }

    public static Constraint primaryKey(final ReferableSchema schema, final Index index) {

        final List<org.jooq.Name> names = new ArrayList<>();
        index.resolvePartitionNames().forEach(v -> names.add(columnName(v)));
        index.getSort().forEach(v -> names.add(columnName(v.getName())));
        return DSL.primaryKey(names.toArray(new org.jooq.Name[0]));
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static Field<?> selectField(final Field<?> field, final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Field<?>>() {

            @Override
            public <T> Field<?> visitDefault(final Use<T> type) {

                return field;
            }

            private Field<?> toJson(final Field<?> field) {

                return field.cast(JSON.class);
            }

            @Override
            public <V, T extends Collection<V>> Field<?> visitCollection(final UseCollection<V, T> type) {

                return toJson(field);
            }

            @Override
            public <V> Field<?> visitMap(final UseMap<V> type) {

                return toJson(field);
            }

            @Override
            public Field<?> visitStruct(final UseStruct type) {

                return toJson(field);
            }
        });
    }

    public static <T> Field<T> field(final QueryPart part, final Class<T> type) {

        if(part == null) {
            return null;
        } else if(part instanceof Field<?>) {
            return cast((Field<?>) part, type);
        } else if(part instanceof Condition){
            return cast(DSL.field((Condition)part), type);
        } else {
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> Field<T> cast(final Field<?> field, final Class<T> type) {

        if(type == Object.class) {
            return (Field<T>)field;
        } else {
            return field.cast(type);
        }
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    public static Field<?> field(final QueryPart part) {

        if(part == null) {
            return null;
        } else if(part instanceof Field<?>) {
            return (Field<?>)part;
        } else if(part instanceof Condition){
            return DSL.field((Condition)part);
        } else {
            throw new IllegalStateException();
        }
    }

    public static Condition condition(final QueryPart part) {

        if(part == null) {
            return null;
        } else if(part instanceof Field<?>) {
            return DSL.condition(((Field<?>)part).cast(Boolean.class));
        } else if(part instanceof Condition){
            return (Condition)part;
        } else {
            throw new IllegalStateException();
        }
    }
}
