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
import io.basestar.schema.Index;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.Reserved;
import io.basestar.schema.use.*;
import io.basestar.util.Path;
import io.basestar.util.Sort;
import org.apache.commons.text.StringEscapeUtils;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

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

                return SQLDataType.JSONB;
            }

            @Override
            public <T> DataType<?> visitSet(final UseSet<T> type) {

                return SQLDataType.JSONB;
            }

            @Override
            public <T> DataType<?> visitMap(final UseMap<T> type) {

                return SQLDataType.JSONB;
            }

            @Override
            public DataType<?> visitStruct(final UseStruct type) {

                return SQLDataType.JSONB;
            }

            @Override
            public DataType<?> visitBinary(final UseBinary type) {

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
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public <T> String visitArray(final UseArray<T> type) {

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
            public byte[] visitBinary(final UseBinary type) {

                return type.create(value);
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
                    return ObjectSchema.ref(id);
                }
            }

            private <T> T fromJson(final Object value, final TypeReference<T> ref) {

                if(value == null) {
                    return null;
                }
                try {
                    final String str;
                    if(value instanceof String) {
                        str = (String)value;
                    } else if(value instanceof JSON) {
                        str = unescape(((JSON)value).data());
                    } else if(value instanceof JSONB) {
                        str = unescape(((JSONB)value).data());
                    } else {
                        throw new IllegalStateException();
                    }
                    return objectMapper.readValue(str, ref);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            // FIXME
            private String unescape(final String data) {

                return StringEscapeUtils.unescapeJava(data.substring(1, data.length() - 1));
            }

            @Override
            public <T> Collection<T> visitArray(final UseArray<T> type) {

                return type.create(fromJson(value, new TypeReference<Collection<?>>() {}));
            }

            @Override
            public <T> Collection<T> visitSet(final UseSet<T> type) {

                return type.create(fromJson(value, new TypeReference<Collection<?>>() {}));
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
            public byte[] visitBinary(final UseBinary type) {

                return type.create(value);
            }
        });
    }

    public static List<Field<?>> fields(final ObjectSchema schema) {

        return Stream.concat(
                ObjectSchema.METADATA_SCHEMA.entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()), dataType(e.getValue()))),
                schema.getProperties().entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()),
                                dataType(e.getValue().getType()).nullable(!e.getValue().isRequired())))
        ).collect(Collectors.toList());
    }

    private static SortOrder sort(final Sort.Order order) {

        return order == Sort.Order.ASC ? SortOrder.ASC : SortOrder.DESC;
    }

//    public static List<OrderField<?>> orderFields(final Index index) {
//
//        return Stream.concat(
//                index.getPartition().stream().map(v -> DSL.field(DSL.name(v.toString()))),
//                index.getSort().stream().map(v -> DSL.field(DSL.name(v.getPath().toString()))
//                        .sort(sort(v.getOrder())))
//        ).collect(Collectors.toList());
//    }

    public static List<OrderField<?>> indexKeys(final Index index) {

        return Stream.concat(
                index.getPartition().stream().map(v -> DSL.field(DSL.name(v.toString()))),
                index.getSort().stream().map(v -> DSL.field(DSL.name(v.getPath().toString()))
                        .sort(sort(v.getOrder())))
        ).collect(Collectors.toList());
    }

    public static List<Field<?>> fields(final ObjectSchema schema, final Index index) {

        final List<Path> partitionPaths = index.resolvePartitionPaths();
        final List<Sort> sortPaths = index.getSort();

        return Stream.of(
                partitionPaths.stream()
                        .map(v -> DSL.field(columnName(v), dataType(schema.typeOf(v)).nullable(true))),
                sortPaths.stream()
                        .map(Sort::getPath)
                        .map(v -> DSL.field(columnName(v), dataType(schema.typeOf(v)).nullable(true))),
                index.projectionSchema(schema).entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()), dataType(e.getValue()).nullable(true)))

        ).flatMap(v -> v).collect(Collectors.toList());
    }

    public static Path columnPath(final Path v) {

        return Path.of(v.toString(Reserved.PREFIX));
    }

    public static Name columnName(final Path v) {

        return DSL.name(v.toString(Reserved.PREFIX));
    }

    public static Constraint primaryKey(final ObjectSchema schema, final Index index) {

        final List<Name> names = new ArrayList<>();
        index.resolvePartitionPaths().forEach(v -> names.add(columnName(v)));
        index.getSort().forEach(v -> names.add(columnName(v.getPath())));
        return DSL.primaryKey(names.toArray(new Name[0]));
    }
}
