package io.basestar.spark.util;

import com.google.common.collect.Lists;
import io.basestar.util.ISO8601;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import scala.collection.Seq;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

public class SparkRowUtils {
    public static StructType remove(final StructType structType, final String fieldName) {

        final List<StructField> fields = Lists.newArrayList(structType.fields());
        fields.removeIf(v -> v.name().equals(fieldName));
        return DataTypes.createStructType(fields.toArray(new StructField[0]));
    }

    public static StructType append(final StructType sourceType, final StructField field) {

        final List<StructField> fields = Lists.newArrayList(sourceType.fields());
        fields.add(field);
        return DataTypes.createStructType(fields.toArray(new StructField[0]));
    }

    public static Optional<StructField> findField(final StructType type, final String name) {

        return Arrays.stream(type.fields()).filter(f -> name.equalsIgnoreCase(f.name())).findFirst();
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
        } else if(value instanceof Seq) {
            final List<Object> tmp = new ArrayList<>();
            ScalaUtils.asJavaStream((Seq<?>)value).forEach(v -> tmp.add(fromSpark(v)));
            return tmp;
        } else if(value instanceof java.sql.Timestamp) {
            return ISO8601.toDateTime(value);
        } else if(value instanceof java.sql.Date) {
            return ISO8601.toDate(value);
        } else {
            return value;
        }
    }

    public static StructField getField(final StructType outputType, final String key) {

        return findField(outputType, key).orElseThrow(() ->
                new IllegalStateException("Struct type missing " + key
                        + " (got " + Arrays.toString(outputType.fieldNames()) + ")"));
    }

    public static StructField field(final String name, final DataType type) {

        return StructField.apply(name, type, true, Metadata.empty());
    }
}
