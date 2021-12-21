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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.jackson.BasestarModule;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.secret.Secret;
import io.basestar.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import java.io.IOException;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class SparkSchemaUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(BasestarModule.INSTANCE);

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
        fields.add(SparkRowUtils.field(PARTITION, DataTypes.BinaryType));
        fields.add(SparkRowUtils.field(SORT, DataTypes.BinaryType));
        index.projectionSchema(schema).forEach((name, type) -> fields.add(field(name, type, null)));
        extraMetadata.forEach((name, type) -> fields.add(field(name, type, null)));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields).asNullable();
    }

    public static StructType refType(final ReferableSchema schema, final Set<Name> expand) {

        if (expand == null) {
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

        return SparkRowUtils.field(name, type(type, expand));
    }

    public static DataType type(final Schema<?> schema, final Set<Name> expand) {

        if (schema instanceof ObjectSchema) {
            return structType((ObjectSchema) schema, expand);
        } else if (schema instanceof StructSchema) {
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
            public DataType visitDecimal(final UseDecimal type) {

                return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
            }

            @Override
            public DataType visitComposite(final UseComposite type) {

                return structType(type.getTypes(), expand).asNullable();
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
            public DataType visitQuery(final UseQuery type) {

                return structType(type.getSchema(), expand);
            }

            @Override
            public <T> DataType visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this).asNullable();
            }

            @Override
            public DataType visitAny(final UseAny type) {

                return DataTypes.StringType.asNullable();
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

        if (row == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(layout.getExpand());
        final Map<String, Object> object = new HashMap<>();
        layout.getSchema().forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), SparkRowUtils.get(naming, row, name))));
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

        if (row == null) {
            return null;
        }
        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, Object> object = new HashMap<>();
        schema.layoutSchema(expand).forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), SparkRowUtils.get(naming, row, name))));
        extraMetadata.forEach((name, type) -> object.put(name, fromSpark(type, naming, branches.get(name), SparkRowUtils.get(naming, row, name))));
        return new Instance(object);
    }

    protected static Object fromSpark(final Link link, final NamingConvention naming, final Set<Name> expand, final Object value) {

        if (link.isSingle()) {
            return fromSpark(link.getSchema(), naming, expand, (Row) value);
        } else {
            final List<Map<String, Object>> results = new ArrayList<>();
            ((scala.collection.Iterable<?>) value).foreach(ScalaUtils.scalaFunction(v -> {
                results.add(fromSpark(link.getSchema(), naming, expand, (Row) v));
                return null;
            }));
            return results;
        }
    }

    public static Map.Entry<Index.Key.Binary, Map<String, Object>> fromSpark(final ObjectSchema schema, final Index index, final Set<Name> expand, final Map<String, Use<?>> extraMetadata, final Row row) {

        if (row == null) {
            return null;
        }
        final byte[] partition = (byte[]) SparkRowUtils.get(row, PARTITION);
        final byte[] sort = (byte[]) SparkRowUtils.get(row, SORT);
        final Index.Key.Binary key = Index.Key.Binary.of(new BinaryKey(partition), new BinaryKey(sort));
        final Map<String, Set<Name>> branches = Name.branch(expand);
        final NamingConvention naming = NamingConvention.DEFAULT;
        final Map<String, Object> projection = new HashMap<>();
        index.projectionSchema(schema).forEach((name, type) -> projection.put(name, fromSpark(type, naming, branches.get(name), SparkRowUtils.get(naming, row, name))));
        extraMetadata.forEach((name, type) -> projection.put(name, fromSpark(type, naming, branches.get(name), SparkRowUtils.get(naming, row, name))));
        return new AbstractMap.SimpleImmutableEntry<>(key, projection);
    }

    public static Map<String, Object> refFromSpark(final NamingConvention naming, final Row row) {

        final Map<String, Object> object = new HashMap<>();
        ObjectSchema.REF_SCHEMA
                .forEach((name, type) -> object.put(name, fromSpark(type, naming, Collections.emptySet(), SparkRowUtils.get(naming, row, name))));
        return new Instance(object);
    }

    public static Object fromSpark(final Use<?> type, final NamingConvention naming, final Set<Name> expand, final Object value) {

        return fromSpark(type, naming, expand, value, true);
    }

    public static Object fromSpark(final Use<?> type, final NamingConvention naming, final Set<Name> expand, final Object value, final boolean suppress) {

        if (value == null) {
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

                if (value instanceof String) {
                    return ReferableSchema.ref((String) value);
                } else if (value instanceof Row) {
                    if (expand != null) {
                        return fromSpark(type.getSchema(), naming, expand, (Row) value);
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

                if (value instanceof Seq<?>) {
                    final List<T> result = new ArrayList<>();
                    ((Seq<?>) value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T) fromSpark(type.getType(), naming, expand, v, suppress));
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

                if (value instanceof Seq<?>) {
                    final List<T> result = new ArrayList<>();
                    ((Seq<?>) value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T) fromSpark(type.getType(), naming, expand, v, suppress));
                        return null;
                    }));
                    return new Page<>(result, null);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitDecimal(final UseDecimal type) {

                return type.create(value, expand, suppress);
            }

            @Override
            public Object visitComposite(final UseComposite type) {

                return type.create(fromSpark(Layout.simple(type.getTypes(), expand), (Row) value));
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitSet(final UseSet<T> type) {

                if (value instanceof Seq<?>) {
                    final Set<T> result = new HashSet<>();
                    ((Seq<?>) value).foreach(ScalaUtils.scalaFunction(v -> {
                        result.add((T) fromSpark(type.getType(), naming, expand, v, suppress));
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

                if (value instanceof scala.collection.Map<?, ?>) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final Map<String, T> result = new HashMap<>();
                    ((scala.collection.Map<?, ?>) value).foreach(ScalaUtils.scalaFunction(e -> {
                        final String k = (String) e._1();
                        final Object v = e._2();
                        result.put(k, (T) fromSpark(type.getType(), naming, branches.get(k), v, suppress));
                        return null;
                    }));
                    return result;
                } else if (value instanceof Row) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final Map<String, T> result = new HashMap<>();
                    final Row row = (Row) value;
                    final StructField[] fields = row.schema().fields();
                    for (int i = 0; i != fields.length; ++i) {
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

                if (value instanceof Row) {
                    return fromSpark(type.getSchema(), naming, expand, (Row) value);
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

                if (value instanceof Row) {
                    return fromSpark(type.getSchema(), naming, expand, (Row) value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitQuery(final UseQuery type) {

                if (value instanceof Row) {
                    return fromSpark(type.getSchema(), naming, expand, (Row) value);
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

                if (value instanceof String) {
                    try {
                        return OBJECT_MAPPER.readValue((String) value, Object.class);
                    } catch (final IOException e) {
                        throw new IllegalStateException();
                    }
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return type.create(value, expand, suppress);
            }
        });
    }

    public static Row toSpark(final ObjectSchema schema, final Index index, final Map<String, Use<?>> extraMetadata, final StructType structType, final Index.Key.Binary key, final Map<String, Object> object) {

        if (object == null) {
            return null;
        }
        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        values[structType.fieldIndex(PARTITION)] = key.getPartition().getBytes();
        values[structType.fieldIndex(SORT)] = key.getSort().getBytes();
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

        if (object == null) {
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

        if (object == null) {
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

        if (value == null) {
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

                if (value instanceof Map<?, ?> && dataType instanceof StructType) {
                    if (expand != null) {
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

                if (value instanceof Collection<?> && dataType instanceof ArrayType) {
                    final DataType valueDataType = ((ArrayType) dataType).elementType();
                    final Stream<Object> stream = ((Collection<T>) value).stream()
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
            public Object visitDecimal(final UseDecimal type) {

                return type.create(value, expand, suppress);
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitComposite(final UseComposite type) {

                return toSpark(Layout.simple(type.getTypes(), expand), (StructType) dataType, (Map<String, Object>) value);
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                return visitCollection(type);
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitMap(final UseMap<T> type) {

                if (value instanceof Map<?, ?> && dataType instanceof MapType) {
                    final Map<String, Set<Name>> branches = Name.branch(expand);
                    final DataType valueDataType = ((MapType) dataType).valueType();
                    final Map<String, Object> tmp = new HashMap<>();
                    ((Map<String, T>) value)
                            .forEach((k, v) -> tmp.put(k, toSpark(type.getType(), UseMap.branch(branches, k), valueDataType, v, suppress)));
                    return ScalaUtils.asScalaMap(tmp);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitStruct(final UseStruct type) {

                if (value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return toSpark(type.getSchema(), expand, (StructType) dataType, (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return Nullsafe.map(type.create(value, expand, suppress), Bytes::getBytes);
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

                if (value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return toSpark(type.getSchema(), expand, (StructType) dataType, (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitQuery(final UseQuery type) {

                if (value instanceof Map<?, ?> && dataType instanceof StructType) {
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

                try {
                    return OBJECT_MAPPER.writeValueAsString(value);
                } catch (final IOException e) {
                    throw new IllegalStateException(e);
                }
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                final Secret converted = type.create(value, expand, suppress);
                return converted == null ? null : converted.encrypted();
            }
        });
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
            public Encoder<?> visitDecimal(final UseDecimal type) {

                return Encoders.DECIMAL();
            }

            @Override
            public Encoder<?> visitComposite(final UseComposite type) {

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
            public Encoder<?> visitQuery(final UseQuery type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public Encoder<?> visitAny(final UseAny type) {

                return Encoders.STRING();
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

    public static String getId(final Row row) {

        return (String) SparkRowUtils.get(row, ObjectSchema.ID);
    }

    public static Long getVersion(final Row row) {

        return (Long) SparkRowUtils.get(row, ObjectSchema.VERSION);
    }

    public static java.sql.Timestamp getCreated(final Row row) {

        return (java.sql.Timestamp) SparkRowUtils.get(row, ObjectSchema.CREATED);
    }

    public static java.sql.Timestamp getUpdated(final Row row) {

        return (java.sql.Timestamp) SparkRowUtils.get(row, ObjectSchema.UPDATED);
    }

    public static String getHash(final Row row) {

        return (String) SparkRowUtils.get(row, ObjectSchema.HASH);
    }

    public static Object fromSpark(final Object value) {

        if (value instanceof scala.collection.Map) {
            final Map<String, Object> result = new HashMap<>();
            ((scala.collection.Map<?, ?>) value).foreach(ScalaUtils.scalaFunction(e -> {
                final String k = (String) e._1();
                final Object v = e._2();
                result.put(k, fromSpark(v));
                return null;
            }));
            return result;
        } else if (value instanceof Seq<?>) {
            final List<Object> result = new ArrayList<>();
            ((Seq<?>) value).foreach(ScalaUtils.scalaFunction(v -> {
                result.add(fromSpark(v));
                return null;
            }));
            return result;
        } else if (value instanceof java.sql.Date) {
            return ISO8601.toDate(value);
        } else if (value instanceof java.sql.Timestamp) {
            return ISO8601.toDateTime(value);
        } else {
            return value;
        }
    }
}
