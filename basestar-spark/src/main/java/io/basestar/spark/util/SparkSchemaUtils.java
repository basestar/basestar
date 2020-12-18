package io.basestar.spark.util;

/*-
 * #%L
 * basestar-spark
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.basestar.expression.call.Callable;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Page;
import io.basestar.util.Sort;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SparkSchemaUtils {

    public static final String PARTITION = Reserved.PREFIX + "partition";

    public static final String SORT = Reserved.PREFIX + "sort";

    private SparkSchemaUtils() {

    }

    public static StructType structType(final Layout layout) {

        return structType(layout.getSchema(), layout.getExpand());
    }

    public static StructType structType(final Map<String, Use<?>> schema, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final List<StructField> fields = new ArrayList<>();
        schema.forEach((name, type) -> fields.add(field(name, type, branches.get(name))));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields).asNullable();
    }

    public static StructType structType(final InstanceSchema schema, final Set<Name> expand) {

        return structType(schema, expand, ImmutableMap.of());
    }

    public static StructType structType(final InstanceSchema schema, final Set<Name> expand, final Map<String, Use<?>> extraMetadata) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final List<StructField> fields = new ArrayList<>();
        schema.layoutSchema(expand).forEach((name, type) -> fields.add(field(name, type, branches.get(name))));
        extraMetadata.forEach((name, type) -> fields.add(field(name, type, branches.get(name))));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields).asNullable();
    }

    public static StructType structType(final ObjectSchema schema, final Index index) {

        return structType(schema, index, ImmutableMap.of());
    }

    public static StructType structType(final ObjectSchema schema, final Index index, final Map<String, Use<?>> extraMetadata) {

        final List<StructField> fields = new ArrayList<>();
        fields.add(field(PARTITION, DataTypes.BinaryType));
        fields.add(field(SORT, DataTypes.BinaryType));
        index.projectionSchema(schema).forEach((name, type) -> fields.add(field(name, type, null)));
        extraMetadata.forEach((name, type) -> fields.add(field(name, type, null)));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields).asNullable();
    }

    public static Set<Name> names(final InstanceSchema schema, final StructType structType) {

        // FIXME: remove
        final SortedMap<String, Use<?>> tmp = new TreeMap<>();
        schema.metadataSchema().forEach(tmp::put);
        schema.getMembers().forEach((name, member) -> tmp.put(name, member.getType()));

        final Set<Name> names = new HashSet<>();
        tmp.forEach((name, type) -> findField(structType, name)
                .ifPresent(field -> names(type, field.dataType())
                        .forEach(rest -> names.add(Name.of(name).with(rest)))));
        return names;
    }

    public static Set<Name> names(final Use<?> type, final DataType dataType) {

        return type.visit(new Use.Visitor.Defaulting<Set<Name>>() {

            @Override
            public <T> Set<Name> visitDefault(final Use<T> type) {

                return ImmutableSet.of(Name.of());
            }

            @Override
            public <V, T extends Collection<V>> Set<Name> visitCollection(final UseCollection<V, T> type) {

                if(dataType instanceof ArrayType) {
                    return names(type.getType(), ((ArrayType) dataType).elementType());
                } else {
                    return ImmutableSet.of();
                }
            }

            @Override
            public <T> Set<Name> visitMap(final UseMap<T> type) {

                if(dataType instanceof MapType) {
                    return names(type.getType(), ((MapType) dataType).valueType());
                } else {
                    return ImmutableSet.of();
                }
            }

            @Override
            public Set<Name> visitInstance(final UseInstance type) {

                if(dataType instanceof StructType) {
                    return names(type.getSchema(), (StructType)dataType);
                } else {
                    return ImmutableSet.of();
                }
            }
        });
    }

    public static StructType refType(final ReferableSchema schema, final Set<Name> expand) {

        if(expand == null) {
            return refType();
        } else {
            return structType(schema, expand);
        }
    }

    public static StructType refType() {

        final List<StructField> fields = new ArrayList<>();
        ObjectSchema.REF_SCHEMA.forEach((name, type) -> fields.add(field(name, type, null)));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields).asNullable();
    }

    public static StructField field(final String name, final Use<?> type, final Set<Name> expand) {

        return field(name, type(type, expand));
    }

    public static StructField field(final String name, final DataType type) {

        return StructField.apply(name, type, true, Metadata.empty());
    }

    public static DataType type(final Schema<?> schema, final Set<Name> expand) {

        if (schema instanceof ObjectSchema) {
            return structType((ObjectSchema) schema, expand);
        } else if(schema instanceof StructSchema) {
            return structType((StructSchema) schema, expand);
        } else if (schema instanceof EnumSchema) {
            return DataTypes.StringType.asNullable();
        } else {
            throw new IllegalStateException();
        }
    }

    public static DataType type(final Type type) {

        return type(Use.fromJavaType(type), Collections.emptySet());
    }

    public static DataType type(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor<DataType>() {

            @Override
            public DataType visitBoolean(final UseBoolean type) {

                return DataTypes.BooleanType.asNullable();
            }

            @Override
            public DataType visitInteger(final UseInteger type) {

                return DataTypes.LongType.asNullable();
            }

            @Override
            public DataType visitNumber(final UseNumber type) {

                return DataTypes.DoubleType.asNullable();
            }

            @Override
            public DataType visitString(final UseString type) {

                return DataTypes.StringType.asNullable();
            }

            @Override
            public DataType visitEnum(final UseEnum type) {

                return DataTypes.StringType.asNullable();
            }

            @Override
            public DataType visitRef(final UseRef type) {

                return refType(type.getSchema(), expand);
            }

            @Override
            public <T> DataType visitArray(final UseArray<T> type) {

                return DataTypes.createArrayType(type.getType().visit(this)).asNullable();
            }

            @Override
            public <T> DataType visitPage(final UsePage<T> type) {

                return DataTypes.createArrayType(type.getType().visit(this)).asNullable();
            }

            @Override
            public <T> DataType visitSet(final UseSet<T> type) {

                return DataTypes.createArrayType(type.getType().visit(this)).asNullable();
            }

            @Override
            public <T> DataType visitMap(final UseMap<T> type) {

                return DataTypes.createMapType(DataTypes.StringType, type.getType().visit(this)).asNullable();
            }

            @Override
            public DataType visitStruct(final UseStruct type) {

                return structType(type.getSchema(), expand).asNullable();
            }

            @Override
            public DataType visitBinary(final UseBinary type) {

                return DataTypes.BinaryType.asNullable();
            }

            @Override
            public DataType visitDate(final UseDate type) {

                return DataTypes.DateType.asNullable();
            }

            @Override
            public DataType visitDateTime(final UseDateTime type) {

                return DataTypes.TimestampType.asNullable();
            }

            @Override
            public DataType visitView(final UseView type) {

                return structType(type.getSchema(), expand);
            }

            @Override
            public <T> DataType visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this).asNullable();
            }

            @Override
            public DataType visitAny(final UseAny type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public DataType visitSecret(final UseSecret type) {

                return DataTypes.BinaryType.asNullable();
            }
        });
    }

    public static Map<String, Object> fromSpark(final Layout layout, final Row row) {

        return fromSpark(layout, NamingConvention.DEFAULT, row);
    }

    public static Map<String, Object> fromSpark(final Layout layout, final NamingConvention naming, final Row row) {

        if(row == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(layout.getExpand());
        final Map<String, Object> object = new HashMap<>();
        layout.getSchema().forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), get(naming, row, name))));
        return new Instance(object);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final Row row) {

        return fromSpark(schema, NamingConvention.DEFAULT, ImmutableSet.of(), ImmutableMap.of(), row);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final Set<Name> expand, final Row row) {

        return fromSpark(schema, NamingConvention.DEFAULT, expand, Collections.emptyMap(), row);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, final Row row) {

        return fromSpark(schema, NamingConvention.DEFAULT, expand, extraMetadata, row);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final NamingConvention naming, final Row row) {

        return fromSpark(schema, naming, ImmutableSet.of(), ImmutableMap.of(), row);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final NamingConvention naming, final Set<Name> expand, final Row row) {

        return fromSpark(schema, naming, expand, Collections.emptyMap(), row);
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final NamingConvention naming, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, final Row row) {

        if(row == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> object = new HashMap<>();
        schema.layoutSchema(expand).forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), get(naming, row, name))));
        extraMetadata.forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), get(naming, row, name))));
        return new Instance(object);
    }

    protected static Object fromSpark(final Link link, final NamingConvention naming, final Set<Name> expand, final Object value) {

        if(link.isSingle()) {
            return fromSpark(link.getSchema(), naming, expand, (Row)value);
        } else {
            final List<Map<String, Object>> results = new ArrayList<>();
            ((scala.collection.Iterable<?>)value).foreach(ScalaUtils.scalaFunction(v -> {
                results.add(fromSpark(link.getSchema(), naming, expand, (Row)v));
                return null;
            }));
            return results;
        }
    }

    public static Map.Entry<Index.Key.Binary, Map<String, Object>> fromSpark(final ObjectSchema schema, final Index index, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, final Row row) {

        if(row == null) {
            return null;
        }
        final byte[] partition = (byte[])get(row, PARTITION);
        final byte[] sort = (byte[])get(row, SORT);
        final Index.Key.Binary key = Index.Key.Binary.of(partition, sort);
        final Map<String, Set<Name>> branches = Name.branch(expand);
        final NamingConvention naming = NamingConvention.DEFAULT;
        final Map<String, Object> projection = new HashMap<>();
        index.projectionSchema(schema).forEach((name, type) -> projection.put(name, fromSpark(type, naming, branches.get(name), get(naming, row, name))));
        extraMetadata.forEach((name, type) -> projection.put(name, fromSpark(type, naming, branches.get(name), get(naming, row, name))));
        return new AbstractMap.SimpleImmutableEntry<>(key, projection);
    }

    public static Map<String, Object> refFromSpark(final NamingConvention naming, final Row row) {

        final Map<String, Object> object = new HashMap<>();
        ObjectSchema.REF_SCHEMA
                .forEach((name, type) -> object.put(name, fromSpark(type, naming, Collections.emptySet(), get(naming, row, name))));
        return new Instance(object);
    }

    public static Object fromSpark(final Use<?> type, final NamingConvention naming, final Set<Name> expand, final Object value) {

        return fromSpark(type, naming, expand, value, true);
    }

    public static Object fromSpark(final Use<?> type, final NamingConvention naming, final Set<Name> expand, final Object value, final boolean suppress) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Object visitBoolean(final UseBoolean type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitInteger(final UseInteger type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitNumber(final UseNumber type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitString(final UseString type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitEnum(final UseEnum type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitRef(final UseRef type) {

                if(value instanceof String) {
                    return ReferableSchema.ref((String)value);
                } else if(value instanceof Row) {
                    if(expand != null) {
                        return fromSpark(type.getSchema(), naming, expand, (Row)value);
                    } else {
                        return refFromSpark(naming, (Row) value);
                    }
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitArray(final UseArray<T> type) {

                if(value instanceof Seq<?>) {
                    final List<T> result = new ArrayList<>();
                    ((Seq<?>)value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T)fromSpark(type.getType(), naming, expand, v, suppress));
                        return null;
                    }));
                    return result;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitPage(final UsePage<T> type) {

                if(value instanceof Seq<?>) {
                    final List<T> result = new ArrayList<>();
                    ((Seq<?>)value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T)fromSpark(type.getType(), naming, expand, v, suppress));
                        return null;
                    }));
                    return new Page<>(result, null);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitSet(final UseSet<T> type) {

                if(value instanceof Seq<?>) {
                    final Set<T> result = new HashSet<>();
                    ((Seq<?>)value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T)fromSpark(type.getType(), naming, expand, v, suppress));
                        return null;
                    }));
                    return result;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitMap(final UseMap<T> type) {

                if(value instanceof scala.collection.Map<?, ?>) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final Map<String, T> result = new HashMap<>();
                    ((scala.collection.Map<?, ?>) value).foreach(ScalaUtils.scalaFunction(e -> {
                        final String k = (String) e._1();
                        final Object v = e._2();
                        result.put(k, (T) fromSpark(type.getType(), naming, branches.get(k), v, suppress));
                        return null;
                    }));
                    return result;
                } else if(value instanceof Row) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final Map<String, T> result = new HashMap<>();
                    final Row row = (Row)value;
                    final StructField[] fields = row.schema().fields();
                    for(int i = 0; i != fields.length; ++i) {
                        final String k = fields[i].name();
                        final Object v = row.get(i);
                        result.put(k, (T) fromSpark(type.getType(), naming, branches.get(k), v, suppress));
                    }
                    return result;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                if(value instanceof Row) {
                    return fromSpark(type.getSchema(), naming, expand, (Row)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitDate(final UseDate type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitDateTime(final UseDateTime type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitView(final UseView type) {

                if(value instanceof Row) {
                    return fromSpark(type.getSchema(), naming, expand, (Row)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this);
            }

            @Override
            public Object visitAny(final UseAny type) {

                if(value instanceof Row || value instanceof scala.collection.Map<?, ?>) {
                    return visitMap(UseMap.DEFAULT);
                } else if(value instanceof Seq<?>) {
                    return visitArray(UseArray.DEFAULT);
                } else {
                    return value;
                }
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return type.create(value, expand, suppress);
            }
        });
    }

    public static Row toSpark(final ObjectSchema schema, final Index index, final Map<String, Use<?>> extraMetadata, final StructType structType, final Index.Key.Binary key, final Map<String, Object> object) {

        if(object == null) {
            return null;
        }
        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        values[structType.fieldIndex(PARTITION)] = key.getPartition();
        values[structType.fieldIndex(SORT)] = key.getSort();
        index.projectionSchema(schema).forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, null, fields[i].dataType(), object.get(name));
        });
        extraMetadata.forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, null, fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    public static Row toSpark(final InstanceSchema schema, final Set<Name> expand, final StructType structType, final Map<String, Object> object) {

        return toSpark(schema, expand, ImmutableMap.of(), structType, object);
    }

    public static Row toSpark(final InstanceSchema schema, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, final StructType structType, final Map<String, Object> object) {

        if(object == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(expand);
        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        schema.layoutSchema(expand).forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, branches.get(name), fields[i].dataType(), object.get(name));
        });
        extraMetadata.forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, branches.get(name), fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    public static Row toSpark(final Layout layout, final StructType structType, final Map<String, Object> object) {

        if(object == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(layout.getExpand());
        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        layout.getSchema().forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, branches.get(name), fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    // FIXME::
    public static Row refToSpark(final StructType structType, final Map<String, Object> object) {

        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        ReferableSchema.REF_SCHEMA.forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, Collections.emptySet(), fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    public static Object toSpark(final Use<?> type, final Set<Name> expand, final DataType dataType, final Object value) {

        return toSpark(type, expand, dataType, value, true);
    }

    public static Object toSpark(final Use<?> type, final Set<Name> expand, final DataType dataType, final Object value, final boolean suppress) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Object visitBoolean(final UseBoolean type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitInteger(final UseInteger type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitNumber(final UseNumber type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitString(final UseString type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitEnum(final UseEnum type) {

                return type.create(value, expand, suppress);
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitRef(final UseRef type) {

                if(value instanceof Map<?, ?> && dataType instanceof StructType) {
                    if(expand != null) {
                        return toSpark(type.getSchema(), expand, (StructType) dataType, (Map<String, Object>) value);
                    } else {
                        return refToSpark((StructType) dataType, (Map<String, Object>) value);
                    }
                } else {
                    throw new IllegalStateException();
                }
            }

            @SuppressWarnings("unchecked")
            private <T> Object visitCollection(final UseCollection<T, ?> type) {

                if(value instanceof Collection<?> && dataType instanceof ArrayType) {
                    final DataType valueDataType = ((ArrayType) dataType).elementType();
                    final Stream<Object> stream = ((Collection<T>)value).stream()
                            .map(v -> toSpark(type.getType(), expand, valueDataType, v, suppress));
                    return ScalaUtils.asScalaSeq(stream.iterator());
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                return visitCollection(type);
            }

            @Override
            public <T> Object visitPage(final UsePage<T> type) {

                return visitCollection(type);
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                return visitCollection(type);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitMap(final UseMap<T> type) {

                if(value instanceof Map<?, ?> && dataType instanceof MapType) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final DataType valueDataType = ((MapType) dataType).valueType();
                    final Map<String, Object> tmp = new HashMap<>();
                    ((Map<String, T>)value)
                            .forEach((k, v) -> tmp.put(k, toSpark(type.getType(), UseMap.branch(branches, k), valueDataType, v, suppress)));
                    return ScalaUtils.asScalaMap(tmp);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitStruct(final UseStruct type) {

                if(value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return toSpark(type.getSchema(), expand, (StructType)dataType, (Map<String, Object>)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitDate(final UseDate type) {

                final LocalDate converted = type.create(value, expand, suppress);
                return converted == null ? null : ISO8601.toSqlDate(converted);
            }

            @Override
            public Object visitDateTime(final UseDateTime type) {

                final Instant converted = type.create(value, expand, suppress);
                return converted == null ? null : ISO8601.toSqlTimestamp(converted);
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitView(final UseView type) {

                if(value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return toSpark(type.getSchema(), expand, (StructType) dataType, (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this);
            }

            @Override
            public Object visitAny(final UseAny type) {

                if(value instanceof Map<?, ?>) {
                    return visitMap(UseMap.DEFAULT);
                } else if(value instanceof Collection<?>) {
                    return visitArray(UseArray.DEFAULT);
                } else {
                    return value;
                }
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return type.create(value, expand, suppress);
            }
        });
    }

    public static Optional<StructField> findField(final StructType type, final String name) {

        return Arrays.stream(type.fields()).filter(f -> name.equalsIgnoreCase(f.name())).findFirst();
    }

    public static StructSchema structSchema(final DataType dataType) {

        return StructSchema.builder()
                .setProperties(Arrays.stream(((StructType) dataType).fields()).collect(Collectors.toMap(
                        StructField::name, f -> Property.builder().setType(type(f.dataType()))
                )))
                .build();
    }

    public static Use<?> type(final DataType dataType) {

        if(dataType instanceof BooleanType) {
            return UseBoolean.DEFAULT;
        } else if(dataType instanceof ByteType
                | dataType instanceof ShortType
                | dataType instanceof IntegerType
                | dataType instanceof LongType) {
            return UseInteger.DEFAULT;
        } else if(dataType instanceof FloatType
                | dataType instanceof DoubleType
                | dataType instanceof DecimalType) {
            return UseNumber.DEFAULT;
        } else if(dataType instanceof StringType) {
            return UseString.DEFAULT;
        } else if(dataType instanceof BinaryType) {
            return UseBinary.DEFAULT;
        } else if(dataType instanceof ArrayType) {
            return new UseArray<>(type(((ArrayType) dataType).elementType()));
        } else if(dataType instanceof MapType) {
            return new UseMap<>(type(((MapType) dataType).valueType()));
        } else if(dataType instanceof StructType) {
            return new UseStruct(structSchema(dataType));
        } else if(dataType instanceof ObjectType) {
            return new UseStruct(structSchema(dataType));
        } else if(dataType instanceof DateType) {
            return new UseDate();
        } else if(dataType instanceof TimestampType) {
            return new UseDateTime();
        } else {
            throw new UnsupportedOperationException("Cannot understand " + dataType);
        }
    }

    /**
     * Move row fields around and insert nulls to match the target schema.
     *
     * Note: this will not try to cast or coerce scalars.
     */

    public static Object conform(final Object source, final DataType targetType) {

        if (source == null) {
            return null;
        } else if (targetType instanceof ArrayType) {
            final DataType elementType = ((ArrayType) targetType).elementType();
            final Seq<?> seq = (Seq<?>)source;
            final Object[] arr = new Object[seq.size()];
            for(int i = 0; i != seq.size(); ++i) {
                arr[i] = conform(seq.apply(i), elementType);
            }
            return scala.Predef.wrapRefArray(arr);
        } else if (targetType instanceof MapType) {
            final DataType valueType = ((MapType) targetType).valueType();
            return ((scala.collection.Map<?, ?>) source).mapValues(v -> conform(v, valueType));
        } else if (targetType instanceof StructType) {
            return conform((Row) source, (StructType) targetType);
        } else {
            return source;
        }
    }

    public static boolean areStrictlyEqual(final DataType a, final DataType b) {

        if(a.getClass().equals(b.getClass())) {
            if(a instanceof ArrayType) {
                final ArrayType arrA = (ArrayType)a;
                final ArrayType arrB = (ArrayType)b;
                return arrA.containsNull() == arrB.containsNull()
                        && areStrictlyEqual(arrA.elementType(), arrB.elementType());
            } else if(a instanceof MapType) {
                final MapType mapA = (MapType) a;
                final MapType mapB = (MapType) b;
                return mapA.valueContainsNull() == mapB.valueContainsNull()
                        && areStrictlyEqual(mapA.keyType(), mapB.keyType())
                        && areStrictlyEqual(mapA.valueType(), mapB.valueType());
            } else if(a instanceof StructType) {
                final StructType objA = (StructType) a;
                final StructType objB = (StructType) b;
                final StructField[] fieldsA = objA.fields();
                final StructField[] fieldsB = objB.fields();
                if(fieldsA.length == fieldsB.length) {
                    for(int i = 0; i != fieldsA.length; ++i) {
                        final StructField fieldA = fieldsA[i];
                        final StructField fieldB = fieldsB[i];
                        if(!(fieldA.name().equals(fieldB.name())
                                && fieldA.nullable() == fieldB.nullable()
                                && areStrictlyEqual(fieldA.dataType(), fieldB.dataType()))) {
                            return false;
                        }
                    }
                    return true;
                } else {
                    return false;
                }
            } else {
                return a.equals(b);
            }
        } else {
            return false;
        }
    }

    public static Row conform(final Row source, final StructType targetType) {

        if(source == null) {
            return null;
//        } else if(source.schema().equals(targetType)) {
//            return source;
        } else {
            final StructType sourceType = source.schema();
            final StructField[] sourceFields = sourceType.fields();
            final StructField[] targetFields = targetType.fields();
            final Seq<Object> sourceValues = source.toSeq();
            final Object[] targetValues = new Object[targetFields.length];
            for (int i = 0; i != targetFields.length; ++i) {
                final StructField targetField = targetFields[i];
                for (int j = 0; j != sourceFields.length; ++j) {
                    if (sourceFields[j].name().equalsIgnoreCase(targetField.name())) {
//                        if(sourceFields[j].dataType().equals(targetField.dataType())) {
//                            targetValues[i] = sourceValues.apply(j);
//                        } else {
                            targetValues[i] = conform(sourceValues.apply(j), targetField.dataType());
//                        }
                        break;
                    }
                }
            }
            return new GenericRowWithSchema(targetValues, targetType);
        }
    }

    public static Row transform(final Row source, final BiFunction<StructField, Object, ?> fn) {

        final StructType sourceType = source.schema();
        final StructField[] sourceFields = sourceType.fields();
        final Object[] targetValues = new Object[sourceFields.length];
        for (int i = 0; i != sourceFields.length; ++i) {
            final StructField sourceField = sourceFields[i];
            targetValues[i] = fn.apply(sourceField, source.get(i));
        }
        return new GenericRowWithSchema(targetValues, sourceType);
    }

    public static Row remove(final Row source, final String name) {

        return remove(source, (field, value) -> name.equals(field.name()));
    }

    public static Row remove(final Row source, final BiPredicate<StructField, Object> fn) {

        final StructType sourceType = source.schema();
        final StructField[] sourceFields = sourceType.fields();
        final List<StructField> outputFields = new ArrayList<>();
        final List<Object> outputValues = new ArrayList<>();
        for (int i = 0; i != sourceFields.length; ++i) {
            final StructField sourceField = sourceFields[i];
            final Object sourceValue = source.get(i);
            if(!fn.test(sourceField, sourceValue)) {
                outputFields.add(sourceField);
                outputValues.add(sourceValue);
            }
        }
        return new GenericRowWithSchema(outputValues.toArray(),
                DataTypes.createStructType(outputFields.toArray(new StructField[0])));
    }

    public static Row set(final Row source, final Map<String, ?> of) {

        return transform(source, (field, value) -> {
            if(of.containsKey(field.name())) {
                return of.get(field.name());
            } else {
                return value;
            }
        });
    }

    public static Row set(final Row source, final String name, final Object newValue) {

        assert(Arrays.asList(source.schema().fieldNames()).contains(name));
        return transform(source, (field, value) -> {
            if(name.equals(field.name())) {
                return newValue;
            } else {
                return value;
            }
        });
    }

    public static Object get(final Row source, final Name name) {

        return get(NamingConvention.DEFAULT, source, name);
    }

    public static Object get(final NamingConvention naming, final Row source, final Name name) {

        if(name.isEmpty()) {
            return source;
        } else {
            final Object first = get(naming, source, name.first());
            final Name rest = name.withoutFirst();
            if(!rest.isEmpty()) {
                if(first instanceof Row) {
                    return get(naming, (Row) first, rest);
                } else {
                    return null;
                }
            } else {
                return first;
            }
        }
    }

    public static Object get(final Row source, final String name) {

        return get(NamingConvention.DEFAULT, source, name);
    }

    public static Object get(final NamingConvention naming, final Row source, final String name) {

        final StructType sourceType = source.schema();
        final StructField[] sourceFields = sourceType.fields();
        for (int i = 0; i != sourceFields.length; ++i) {
            final StructField sourceField = sourceFields[i];
            if(naming.equals(name, sourceField.name())) {
                return source.get(i);
            }
        }
        return null;
    }

    public static Row append(final Row source, final StructField field, final Object value) {

        final StructType sourceType = source.schema();
        final StructField[] sourceFields = sourceType.fields();
        final StructType outputType = append(sourceType, field);
        final List<Object> outputValues = new ArrayList<>();
        for (int i = 0; i != sourceFields.length; ++i) {
            outputValues.add(source.get(i));
        }
        outputValues.add(value);
        return new GenericRowWithSchema(outputValues.toArray(), outputType);
    }

    public static StructType append(final StructType sourceType, final StructField field) {

        final List<StructField> fields = Lists.newArrayList(sourceType.fields());
        fields.add(field);
        return DataTypes.createStructType(fields.toArray(new StructField[0]));
    }

    // Presto in default config wants partition columns after all other columns
    public static StructType orderForPresto(final StructType structType, final List<String> partitionColumns) {

        final SortedMap<String, StructField> data = new TreeMap<>();
        final SortedMap<Integer, StructField> partition = new TreeMap<>();
        Arrays.stream(structType.fields()).forEach(field -> {
            final int indexOf = partitionColumns.indexOf(field.name());
            if(indexOf < 0) {
                data.put(field.name(), field);
            } else {
                partition.put(0, field);
            }
        });
        final List<StructField> fields = new ArrayList<>();
        fields.addAll(data.values());
        fields.addAll(partition.values());
        return DataTypes.createStructType(fields);
    }

    public static Encoder<?> encoder(final Use<?> type) {

        return type.visit(new Use.Visitor<Encoder<?>>() {

            @Override
            public Encoder<Boolean> visitBoolean(final UseBoolean type) {

                return Encoders.BOOLEAN();
            }

            @Override
            public Encoder<Long> visitInteger(final UseInteger type) {

                return Encoders.LONG();
            }

            @Override
            public Encoder<?> visitNumber(final UseNumber type) {

                return Encoders.DOUBLE();
            }

            @Override
            public Encoder<String> visitString(final UseString type) {

                return Encoders.STRING();
            }

            @Override
            public Encoder<String> visitEnum(final UseEnum type) {

                return Encoders.STRING();
            }

            @Override
            public Encoder<?> visitRef(final UseRef type) {

                return RowEncoder.apply(refType().asNullable());
            }

            @Override
            public <V> Encoder<?> visitArray(final UseArray<V> type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <V> Encoder<?> visitPage(final UsePage<V> type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <V> Encoder<?> visitSet(final UseSet<V> type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <V> Encoder<?> visitMap(final UseMap<V> type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public Encoder<Row> visitStruct(final UseStruct type) {

                return RowEncoder.apply(structType(type.getSchema(), Collections.emptySet()));
            }

            @Override
            public Encoder<byte[]> visitBinary(final UseBinary type) {

                return Encoders.BINARY();
            }

            @Override
            public Encoder<java.sql.Date> visitDate(final UseDate type) {

                return Encoders.DATE();
            }

            @Override
            public Encoder<java.sql.Timestamp> visitDateTime(final UseDateTime type) {

                return Encoders.TIMESTAMP();
            }

            @Override
            public Encoder<?> visitView(final UseView type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public Encoder<?> visitAny(final UseAny type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public Encoder<byte[]> visitSecret(final UseSecret type) {

                return Encoders.BINARY();
            }

            @Override
            public <T> Encoder<?> visitOptional(final UseOptional<T> type) {

                return encoder(type.getType());
            }
        });
    }

    public static Column order(final Column column, final Sort.Order order, final Sort.Nulls nulls) {

        if(order == Sort.Order.ASC) {
            if(nulls == Sort.Nulls.FIRST) {
                return column.asc_nulls_first();
            } else {
                return column.asc_nulls_last();
            }
        } else {
            if(nulls == Sort.Nulls.FIRST) {
                return column.desc_nulls_first();
            } else {
                return column.desc_nulls_last();
            }
        }
    }

    public static Object toSpark(final Object value) {

        if(value instanceof Map) {
            final Map<Object, Object> tmp = new HashMap<>();
            ((Map<?, ?>) value).forEach((k, v) -> tmp.put(k, toSpark(v)));
            return ScalaUtils.asScalaMap(tmp);
        } else if(value instanceof Collection) {
            final List<Object> tmp = new ArrayList<>();
            ((Collection<?>) value).forEach(v -> tmp.add(toSpark(v)));
            return ScalaUtils.asScalaSeq(tmp);
        } else if(value instanceof Instant) {
            return ISO8601.toSqlTimestamp((Instant)value);
        } else if(value instanceof LocalDate) {
            return ISO8601.toSqlDate((LocalDate)value);
        } else {
            return value;
        }
    }

    public static Object fromSpark(final Object value) {

        if(value instanceof scala.collection.Map) {
            final Map<Object, Object> tmp = new HashMap<>();
            ScalaUtils.asJavaMap((scala.collection.Map<?, ?>)value).forEach((k, v) -> tmp.put(k, fromSpark(v)));
            return tmp;
        } else if(value instanceof scala.collection.Seq) {
            final List<Object> tmp = new ArrayList<>();
            ScalaUtils.asJavaStream((scala.collection.Seq<?>)value).forEach(v -> tmp.add(fromSpark(v)));
            return tmp;
        } else if(value instanceof java.sql.Timestamp) {
            return ISO8601.toDateTime(value);
        } else if(value instanceof java.sql.Date) {
            return ISO8601.toDate(value);
        } else {
            return value;
        }
    }

    public static UserDefinedFunction udf(final Callable callable) {

        final DataType returnType = type(callable.type());
        switch (callable.args().length) {
            case 0:
                return functions.udf(() -> toSpark(callable.call()), returnType);
            case 1:
                return functions.udf(v1 -> toSpark(callable.call(fromSpark(v1))), returnType);
            case 2:
                return functions.udf((v1, v2) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2))), returnType);
            case 3:
                return functions.udf((v1, v2, v3) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2),
                        fromSpark(v3))), returnType);
            case 4:
                return functions.udf((v1, v2, v3, v4) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2),
                        fromSpark(v3), fromSpark(v4))), returnType);
            case 5:
                return functions.udf((v1, v2, v3, v4, v5) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2),
                        fromSpark(v3), fromSpark(v4), fromSpark(v5))), returnType);
            case 6:
                return functions.udf((v1, v2, v3, v4, v5, v6) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2),
                        fromSpark(v3), fromSpark(v4), fromSpark(v5), fromSpark(v6))), returnType);
            case 7:
                return functions.udf((v1, v2, v3, v4, v5, v6, v7) -> toSpark(callable.call(fromSpark(v1), fromSpark(v2),
                        fromSpark(v3), fromSpark(v4), fromSpark(v5), fromSpark(v6), fromSpark(v7))), returnType);
            case 8:
                return functions.udf((v1, v2, v3, v4, v5, v6, v7, v8) -> toSpark(callable.call(fromSpark(v1),
                        fromSpark(v2), fromSpark(v3), fromSpark(v4), fromSpark(v5), fromSpark(v6), fromSpark(v7),
                        fromSpark(v8))), returnType);
            case 9:
                return functions.udf((v1, v2, v3, v4, v5, v6, v7, v8, v9) -> toSpark(callable.call(fromSpark(v1),
                        fromSpark(v2), fromSpark(v3), fromSpark(v4), fromSpark(v5), fromSpark(v6), fromSpark(v7),
                        fromSpark(v8), fromSpark(v9))), returnType);
            case 10:
                return functions.udf((v1, v2, v3, v4, v5, v6, v7, v8, v9, v10) -> toSpark(callable.call(fromSpark(v1),
                        fromSpark(v2), fromSpark(v3), fromSpark(v4), fromSpark(v5), fromSpark(v6), fromSpark(v7),
                        fromSpark(v8), fromSpark(v9), fromSpark(v10))), returnType);
            default:
                throw new IllegalStateException("Too many UDF parameters");
        }
    }

    public static String getId(final Row row) {

        return (String)SparkSchemaUtils.get(row, ObjectSchema.ID);
    }

    public static Long getVersion(final Row row) {

        return (Long)SparkSchemaUtils.get(row, ObjectSchema.VERSION);
    }

    public static java.sql.Timestamp getCreated(final Row row) {

        return (java.sql.Timestamp)SparkSchemaUtils.get(row, ObjectSchema.CREATED);
    }

    public static java.sql.Timestamp getUpdated(final Row row) {

        return (java.sql.Timestamp)SparkSchemaUtils.get(row, ObjectSchema.UPDATED);
    }

    public static String getHash(final Row row) {

        return (String)SparkSchemaUtils.get(row, ObjectSchema.HASH);
    }

    public static Column cast(final Column column, final Use<?> fromType, final Use<?> toType, final Set<Name> expand) {

        final DataType toDataType = type(toType, expand);
        if(fromType.equals(toType)) {
            return column.cast(toDataType);
        } else {
            final UserDefinedFunction udf = functions.udf((UDF1<Object, Object>) sparkValue -> {

                final Object value = fromSpark(fromType, NamingConvention.DEFAULT, expand, sparkValue);
                return toSpark(toType, expand, toDataType, value);

            }, toDataType);
            return udf.apply(column);
        }
    }
}
