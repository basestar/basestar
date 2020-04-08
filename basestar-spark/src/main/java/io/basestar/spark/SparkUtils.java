package io.basestar.spark;

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

import io.basestar.schema.*;
import io.basestar.schema.use.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.Function1;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.runtime.AbstractFunction1;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class SparkUtils {

    public static StructType structType(final InstanceSchema schema) {

        final List<StructField> fields = new ArrayList<>();
        schema.getAllProperties()
                .forEach((name, property) -> fields.add(field(name, property)));
        schema.metadataSchema()
                .forEach((name, type) -> fields.add(field(name, type, true)));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static StructType refType() {

        final List<StructField> fields = new ArrayList<>();
        ObjectSchema.REF_SCHEMA
                .forEach((name, type) -> fields.add(field(name, type, true)));
        fields.sort(Comparator.comparing(StructField::name));
        return DataTypes.createStructType(fields);
    }

    public static StructField field(final String name, final Property property) {

        return field(name, property.getType(), property.isRequired());
    }

    public static StructField field(final String name, final Use<?> type, final boolean required) {

        return field(name, type(type), required);
    }

    public static StructField field(final String name, final DataType type, final boolean required) {

        return StructField.apply(name, type, !required, Metadata.empty());
    }

    public static DataType type(final Schema<?> schema) {

        if (schema instanceof ObjectSchema) {
            return structType((ObjectSchema) schema);
        } else if(schema instanceof StructSchema) {
            return structType((StructSchema) schema);
        } else if (schema instanceof EnumSchema) {
            return DataTypes.StringType;
        } else {
            throw new IllegalStateException();
        }
    }

    public static DataType type(final Use<?> type) {

        return type.visit(new Use.Visitor<DataType>() {

            @Override
            public DataType visitBoolean(final UseBoolean type) {

                return DataTypes.BooleanType;
            }

            @Override
            public DataType visitInteger(final UseInteger type) {

                return DataTypes.LongType;
            }

            @Override
            public DataType visitNumber(final UseNumber type) {

                return DataTypes.DoubleType;
            }

            @Override
            public DataType visitString(final UseString type) {

                return DataTypes.StringType;
            }

            @Override
            public DataType visitEnum(final UseEnum type) {

                return DataTypes.StringType;
            }

            @Override
            public DataType visitRef(final UseObject type) {

                return refType();
            }

            @Override
            public <T> DataType visitArray(final UseArray<T> type) {

                return DataTypes.createArrayType(type.getType().visit(this));
            }

            @Override
            public <T> DataType visitSet(final UseSet<T> type) {

                return DataTypes.createArrayType(type.getType().visit(this));
            }

            @Override
            public <T> DataType visitMap(final UseMap<T> type) {

                return DataTypes.createMapType(DataTypes.StringType, type.getType().visit(this));
            }

            @Override
            public DataType visitStruct(final UseStruct type) {

                return structType(type.getSchema());
            }

            @Override
            public DataType visitBinary(final UseBinary type) {

                return DataTypes.BinaryType;
            }
        });
    }

    public static Map<String, Object> fromSpark(final InstanceSchema schema, final Row row) {

        final Map<String, Object> object = new HashMap<>();
        for (final Map.Entry<String, Property> entry : schema.getAllProperties().entrySet()) {
            final String name = entry.getKey();
            final Property property = entry.getValue();
            final Object value = row.getAs(name);
            object.put(name, fromSpark(property.getType(), value));
        }
        ObjectSchema.METADATA_SCHEMA
                .forEach((name, type) -> object.put(name, fromSpark(type, row.getAs(name))));
        return schema.create(object);
    }

    public static Map<String, Object> refFromSpark(final Row row) {

        final Map<String, Object> object = new HashMap<>();
        ObjectSchema.REF_SCHEMA
                .forEach((name, type) -> object.put(name, fromSpark(type, row.getAs(name))));
        return object;
    }

    public static <T, R> Function1<T, R> f1(final Function<T, R> fn) {

        return new AbstractFunction1<T, R>() {
            @Override
            public R apply(final T v1) {

                return fn.apply(v1);
            }
        };
    }

    public static Object fromSpark(final Use<?> type, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Object visitBoolean(final UseBoolean type) {

                return type.create(value);
            }

            @Override
            public Object visitInteger(final UseInteger type) {

                return type.create(value);
            }

            @Override
            public Object visitNumber(final UseNumber type) {

                return type.create(value);
            }

            @Override
            public Object visitString(final UseString type) {

                return type.create(value);
            }

            @Override
            public Object visitEnum(final UseEnum type) {

                return type.create(value);
            }

            @Override
            public Object visitRef(final UseObject type) {

                if(value instanceof Row) {
                    return refFromSpark((Row)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitArray(final UseArray<T> type) {

                if(value instanceof Seq<?>) {
                    final List<T> result = new ArrayList<>();
                    ((Seq<?>)value).foreach(f1(v -> {
                        result.add((T)fromSpark(type.getType(), v));
                        return null;
                    }));
                    return result;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitSet(final UseSet<T> type) {

                if(value instanceof Seq<?>) {
                    final Set<T> result = new HashSet<>();
                    ((Seq<?>)value).foreach(f1(v -> {
                        result.add((T)fromSpark(type.getType(), v));
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
                    final Map<String, T> result = new HashMap<>();
                    ((scala.collection.Map<?, ?>)value).foreach(f1(e -> {
                        final String k = (String)e._1();
                        final Object v = e._2();
                        result.put(k, (T)fromSpark(type.getType(), v));
                        return null;
                    }));
                    return result;
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                if(value instanceof Row) {
                    return fromSpark(type.getSchema(), (Row)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return type.create(value);
            }
        });
    }

    // FIXME::
    public static Row toSpark(final InstanceSchema schema, final StructType structType, final Map<String, Object> object) {

        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        schema.metadataSchema().forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, fields[i].dataType(), object.get(name));
        });
        schema.getAllProperties().forEach((name, property) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(property.getType(), fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    // FIXME::
    public static Row refToSpark(final StructType structType, final Map<String, Object> object) {

        final StructField[] fields = structType.fields();
        final Object[] values = new Object[fields.length];
        ObjectSchema.REF_SCHEMA.forEach((name, type) -> {
            final int i = structType.fieldIndex(name);
            values[i] = toSpark(type, fields[i].dataType(), object.get(name));
        });
        return new GenericRowWithSchema(values, structType);
    }

    public static Object toSpark(final Use<?> type, final DataType dataType, final Object value) {

        if(value == null) {
            return null;
        }
        return type.visit(new Use.Visitor<Object>() {

            @Override
            public Object visitBoolean(final UseBoolean type) {

                return type.create(value);
            }

            @Override
            public Object visitInteger(final UseInteger type) {

                return type.create(value);
            }

            @Override
            public Object visitNumber(final UseNumber type) {

                return type.create(value);
            }

            @Override
            public Object visitString(final UseString type) {

                return type.create(value);
            }

            @Override
            public Object visitEnum(final UseEnum type) {

                return type.create(value);
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitRef(final UseObject type) {

                if(value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return refToSpark((StructType)dataType, (Map<String, Object>) value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitArray(final UseArray<T> type) {

                if(value instanceof Collection<?> && dataType instanceof ArrayType) {
                    final DataType valueDataType = ((ArrayType) dataType).elementType();
                    final Stream<Object> stream = ((Collection<T>)value).stream()
                        .map(v -> toSpark(type.getType(), valueDataType, v));
                    return JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala().toSeq();
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitSet(final UseSet<T> type) {

                if(value instanceof Collection<?> && dataType instanceof ArrayType) {
                    final DataType valueDataType = ((ArrayType) dataType).elementType();
                    final Stream<Object> stream = ((Collection<T>)value).stream()
                            .map(v -> toSpark(type.getType(), valueDataType, v));
                    return JavaConverters.asScalaIteratorConverter(stream.iterator()).asScala().toSeq();
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T> Object visitMap(final UseMap<T> type) {

                if(value instanceof Map<?, ?> && dataType instanceof MapType) {
                    final DataType valueDataType = ((MapType) dataType).valueType();
                    final Map<String, Object> tmp = new HashMap<>();
                    ((Map<String, T>)value)
                            .forEach((k, v) -> tmp.put(k, toSpark(type.getType(), valueDataType, v)));
                    return JavaConverters.mapAsScalaMapConverter(tmp).asScala();
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            @SuppressWarnings("unchecked")
            public Object visitStruct(final UseStruct type) {

                if(value instanceof Map<?, ?> && dataType instanceof StructType) {
                    return toSpark(type.getSchema(), (StructType)dataType, (Map<String, Object>)value);
                } else {
                    throw new IllegalStateException();
                }
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return type.create(value);
            }
        });
    }
}
