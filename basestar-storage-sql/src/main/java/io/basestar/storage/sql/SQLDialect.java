package io.basestar.storage.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.*;
import io.basestar.schema.util.Casing;
import io.basestar.secret.Secret;
import io.basestar.storage.sql.resolver.FieldResolver;
import io.basestar.storage.sql.resolver.ValueResolver;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.storage.sql.util.DelegatingDatabaseMetaData;
import io.basestar.util.Name;
import io.basestar.util.*;
import org.jooq.Constraint;
import org.jooq.Named;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface SQLDialect {

    org.jooq.SQLDialect dmlDialect();

    default org.jooq.SQLDialect ddlDialect() {

        return dmlDialect();
    }

    DataType<?> stringType(UseString type);

    default DataType<?> booleanType(final UseBoolean type) {

        return SQLDataType.BOOLEAN;
    }

    default DataType<?> integerType(final UseInteger type) {

        return SQLDataType.BIGINT;
    }

    default DataType<?> numberType(final UseNumber type) {

        return SQLDataType.DOUBLE;
    }

    default DataType<?> dateType(final UseDate type) {

        return SQLDataType.LOCALDATE;
    }

    default DataType<?> dateTimeType(final UseDateTime type) {

        return SQLDataType.TIMESTAMP;
    }

    default DataType<?> enumType(final UseEnum type) {

        return stringType(UseString.DEFAULT);
    }

    <T> DataType<?> arrayType(UseArray<T> type);

    <T> DataType<?> setType(UseSet<T> type);

    <T> DataType<?> pageType(UsePage<T> type);

    <T> DataType<?> mapType(UseMap<T> type);

    DataType<?> structType(UseStruct type);

    DataType<?> viewType(UseView type);

    DataType<?> refType(UseRef type);

    DataType<?> binaryType(UseBinary type);

    default DataType<?> secretType(final UseSecret type) {

        return binaryType(UseBinary.DEFAULT);
    }

    DataType<?> anyType(UseAny type);

    SelectField<?> dateToSQLValue(UseDate type, LocalDate value);

    SelectField<?> dateTimeToSQLValue(UseDateTime type, Instant value);

    SelectField<?> binaryToSQLValue(UseBinary type, Bytes value);

    default SelectField<?> secretToSQLValue(final UseSecret type, final Secret value) {

        return binaryToSQLValue(UseBinary.DEFAULT, new Bytes(value.encrypted()));
    }

    <T> SelectField<?> mapToSQLValue(UseMap<T> type, Map<String, T> value);

    <T> SelectField<?> arrayToSQLValue(UseArray<T> type, List<T> value);

    <T> SelectField<?> setToSQLValue(UseSet<T> type, Set<T> value);

    <T> SelectField<?> pageToSQLValue(UsePage<T> type, Page<T> value);

    SelectField<?> refToSQLValue(UseRef type, Instance value);

    SelectField<?> structToSQLValue(UseStruct type, Instance value);

    SelectField<?> viewToSQLValue(UseView type, Instance value);

    SelectField<?> anyToSQLValue(UseAny type, Object value);

    default <T> Map<Field<?>, SelectField<?>> arrayToSQLValues(final UseArray<T> type, final FieldResolver field, final List<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, arrayToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> dateToSQLValues(final UseDate type, final FieldResolver field, final LocalDate value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, dateToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> dateTimeToSQLValues(final UseDateTime type, final FieldResolver field, final Instant value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, dateTimeToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default <T> Map<Field<?>, SelectField<?>> pageToSQLValues(final UsePage<T> type, final FieldResolver field, final Page<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, pageToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default <T> Map<Field<?>, SelectField<?>> setToSQLValues(final UseSet<T> type, final FieldResolver field, final Set<T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, setToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default <T> Map<Field<?>, SelectField<?>> mapToSQLValues(final UseMap<T> type, final FieldResolver field, final Map<String, T> value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, mapToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> viewToSQLValues(final UseView type, final FieldResolver field, final Instance value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, viewToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> binaryToSQLValues(final UseBinary type, final FieldResolver field, final Bytes value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, binaryToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> anyToSQLValues(final UseAny type, final FieldResolver field, final Object value) {

        return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, anyToSQLValue(type, value))).orElseGet(ImmutableMap::of);
    }

    default Map<Field<?>, SelectField<?>> secretToSQLValues(final UseSecret type, final FieldResolver field, final Secret value) {

        return binaryToSQLValues(UseBinary.DEFAULT, field, new Bytes(value.encrypted()));
    }

    Map<Field<?>, SelectField<?>> structToSQLValues(UseStruct type, FieldResolver field, Instance value);

    Map<Field<?>, SelectField<?>> refToSQLValues(UseRef type, FieldResolver field, Instance value);

    <T> List<T> arrayFromSQLValue(UseArray<T> type, ValueResolver value);

    <T> Page<T> pageFromSQLValue(UsePage<T> type, ValueResolver value);

    <T> Set<T> setFromSQLValue(UseSet<T> type, ValueResolver value);

    <T> Map<String, T> mapFromSQLValue(UseMap<T> type, ValueResolver value);

    Instance structFromSQLValue(UseStruct type, ValueResolver value);

    Instance viewFromSQLValue(UseView type, ValueResolver value);

    Instance refFromSQLValue(UseRef type, ValueResolver value);

    Bytes binaryFromSQLValue(UseBinary type, ValueResolver value);

    default Secret secretFromSQLValue(final UseSecret type, final ValueResolver value) {

        final Bytes v = binaryFromSQLValue(UseBinary.DEFAULT, value);
        if (v == null) {
            return null;
        } else {
            return Secret.encrypted(v.getBytes());
        }
    }

    Object anyFromSQLValue(UseAny type, ValueResolver value);

    <V, T extends Collection<V>> List<Field<?>> selectCollection(final UseCollection<V, T> type, final Name name, final FieldResolver field);

    <V> List<Field<?>> selectMap(final UseMap<V> type, final Name name, final FieldResolver field);

    List<Field<?>> selectRef(final UseRef type, final Name name, final FieldResolver field);

    List<Field<?>> selectStruct(final UseStruct type, final Name name, final FieldResolver field);

    List<Field<?>> selectView(final UseView type, final Name name, final FieldResolver field);

    boolean supportsConstraints();

    boolean supportsIndexes();

    boolean supportsILike();

    default DataType<?> dataType(final Use<?> type) {

        return type.visit(new Use.Visitor<DataType<?>>() {

            @Override
            public DataType<?> visitBoolean(final UseBoolean type) {

                return booleanType(type);
            }

            @Override
            public DataType<?> visitInteger(final UseInteger type) {

                return integerType(type);
            }

            @Override
            public DataType<?> visitNumber(final UseNumber type) {

                return numberType(type);
            }

            @Override
            public DataType<?> visitString(final UseString type) {

                return stringType(type);
            }

            @Override
            public DataType<?> visitEnum(final UseEnum type) {

                return enumType(type);
            }

            @Override
            public DataType<?> visitRef(final UseRef type) {

                return refType(type);
            }

            @Override
            public <T> DataType<?> visitArray(final UseArray<T> type) {

                return arrayType(type);
            }

            @Override
            public <T> DataType<?> visitPage(final UsePage<T> type) {

                return pageType(type);
            }

            @Override
            public DataType<?> visitDecimal(final UseDecimal type) {

                return SQLDataType.DECIMAL(type.getPrecision(), type.getScale());
            }

            @Override
            public DataType<?> visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> DataType<?> visitSet(final UseSet<T> type) {

                return setType(type);
            }

            @Override
            public <T> DataType<?> visitMap(final UseMap<T> type) {

                return mapType(type);
            }

            @Override
            public DataType<?> visitStruct(final UseStruct type) {

                return structType(type);
            }

            @Override
            public DataType<?> visitBinary(final UseBinary type) {

                return binaryType(type);
            }

            @Override
            public DataType<?> visitDate(final UseDate type) {

                return dateType(type);
            }

            @Override
            public DataType<?> visitDateTime(final UseDateTime type) {

                return dateTimeType(type);
            }

            @Override
            public DataType<?> visitView(final UseView type) {

                return viewType(type);
            }

            @Override
            public DataType<?> visitQuery(final UseQuery type) {

                throw new IllegalStateException();
            }

            @Override
            public <T> DataType<?> visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this).nullable(true);
            }

            @Override
            public DataType<?> visitAny(final UseAny type) {

                return anyType(type);
            }

            @Override
            public DataType<?> visitSecret(final UseSecret type) {

                return secretType(type);
            }
        });
    }

    default Map<Field<?>, SelectField<?>> toSQLValues(final Use<?> type, final FieldResolver field, final Object value) {

        if (value == null) {
            return Collections.emptyMap();
        }

        return type.visit(new Use.Visitor.Defaulting<Map<Field<?>, SelectField<?>>>() {

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitScalar(final UseScalar<T> type) {

                return field.field().<Map<Field<?>, SelectField<?>>>map(f -> ImmutableMap.of(f, DSL.val(type.create(value)))).orElseGet(ImmutableMap::of);
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitRef(final UseRef type) {

                return refToSQLValues(type, field, (Instance) value);
            }

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitArray(final UseArray<T> type) {

                return arrayToSQLValues(type, field, type.create(value));
            }

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitPage(final UsePage<T> type) {

                return pageToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitSet(final UseSet<T> type) {

                return setToSQLValues(type, field, type.create(value));
            }

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitMap(final UseMap<T> type) {

                return mapToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitStruct(final UseStruct type) {

                return structToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitAny(final UseAny type) {

                return anyToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitBinary(final UseBinary type) {

                return binaryToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitDate(final UseDate type) {

                return dateToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitDateTime(final UseDateTime type) {

                return dateTimeToSQLValues(type, field, type.create(value));
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitView(final UseView type) {

                return viewToSQLValues(type, field, type.create(value));
            }

            @Override
            public <T> Map<Field<?>, SelectField<?>> visitOptional(final UseOptional<T> type) {

                return toSQLValues(type.getType(), field, value);
            }

            @Override
            public Map<Field<?>, SelectField<?>> visitSecret(final UseSecret type) {

                return secretToSQLValues(type, field, type.create(value));
            }
        });
    }

    default SelectField<?> toSQLValue(final Use<?> type, final Object value) {

        if (value == null) {
            return null;
        }

        return type.visit(new Use.Visitor.Defaulting<SelectField<?>>() {

            @Override
            public <T> SelectField<?> visitScalar(final UseScalar<T> type) {

                return DSL.val(type.create(value));
            }

            @Override
            public SelectField<?> visitRef(final UseRef type) {

                return refToSQLValue(type, (Instance) value);
            }

            @Override
            public <T> SelectField<?> visitArray(final UseArray<T> type) {

                return arrayToSQLValue(type, type.create(value));
            }

            @Override
            public <T> SelectField<?> visitPage(final UsePage<T> type) {

                return pageToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> SelectField<?> visitSet(final UseSet<T> type) {

                return setToSQLValue(type, type.create(value));
            }

            @Override
            public <T> SelectField<?> visitMap(final UseMap<T> type) {

                return mapToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitStruct(final UseStruct type) {

                return structToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitAny(final UseAny type) {

                return anyToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitBinary(final UseBinary type) {

                return binaryToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitDate(final UseDate type) {

                return dateToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitDateTime(final UseDateTime type) {

                return dateTimeToSQLValue(type, type.create(value));
            }

            @Override
            public SelectField<?> visitView(final UseView type) {

                return viewToSQLValue(type, type.create(value));
            }

            @Override
            public <T> SelectField<?> visitOptional(final UseOptional<T> type) {

                return toSQLValue(type.getType(), value);
            }

            @Override
            public SelectField<?> visitSecret(final UseSecret type) {

                return secretToSQLValue(type, type.create(value));
            }
        });
    }

    default Object fromSQLValue(final Use<?> type, final ValueResolver value) {

        return type.visit(new Use.Visitor.Defaulting<Object>() {

            @Override
            public <T> Object visitScalar(final UseScalar<T> type) {

                final Object v = value.value();
                if (v == null) {
                    return null;
                } else {
                    return type.create(v);
                }
            }

            @Override
            public Map<String, Object> visitRef(final UseRef type) {

                return refFromSQLValue(type, value);
            }

            @Override
            public <T> Collection<T> visitArray(final UseArray<T> type) {

                return arrayFromSQLValue(type, value);
            }

            @Override
            public <T> Collection<T> visitPage(final UsePage<T> type) {

                return pageFromSQLValue(type, value);
            }

            @Override
            public Object visitDecimal(final UseDecimal type) {

                return type.create(value.value());
            }

            @Override
            public Object visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Collection<T> visitSet(final UseSet<T> type) {

                return setFromSQLValue(type, value);
            }

            @Override
            public <T> Map<String, T> visitMap(final UseMap<T> type) {

                return mapFromSQLValue(type, value);
            }

            @Override
            public Map<String, Object> visitStruct(final UseStruct type) {

                return structFromSQLValue(type, value);
            }

            @Override
            public Bytes visitBinary(final UseBinary type) {

                return binaryFromSQLValue(type, value);
            }

            @Override
            public Object visitView(final UseView type) {

                return viewFromSQLValue(type, value);
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this);
            }

            @Override
            public Object visitAny(final UseAny type) {

                return anyFromSQLValue(type, value);
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return secretFromSQLValue(type, value);
            }
        });
    }

    default List<Field<?>> fields(final Casing casing, final Name name, final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<List<Field<?>>>() {

            @Override
            public <T> List<Field<?>> visitDefault(final Use<T> type) {

                return Immutable.list(DSL.field(columnName(casing, name), dataType(type)));
            }

            @Override
            public List<Field<?>> visitStruct(final UseStruct type) {

                final InstanceSchema schema = type.getSchema();
                return fields(casing, name, schema);
            }

            @Override
            public List<Field<?>> visitRef(final UseRef type) {

                if (type.isVersioned()) {
                    return ImmutableList.of(
                            DSL.field(columnName(casing, name.with(ReferableSchema.ID)), dataType(UseString.DEFAULT)),
                            DSL.field(columnName(casing, name.with(ReferableSchema.VERSION)), dataType(UseInteger.DEFAULT))
                    );
                } else {
                    return ImmutableList.of(
                            DSL.field(columnName(casing, name.with(ReferableSchema.ID)), dataType(UseString.DEFAULT))
                    );
                }
            }

            @Override
            public List<Field<?>> visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> List<Field<?>> visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this);
            }
        });
    }

    default List<Field<?>> fields(final Casing casing, final InstanceSchema schema) {

        return fields(casing, Name.empty(), schema);
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default List<Field<?>> fields(final Casing casing, final Name name, final InstanceSchema schema) {

        return Stream.concat(
                schema.metadataSchema().entrySet().stream()
                        .flatMap(e -> fields(casing, name.with(e.getKey()), e.getValue()).stream()),
                schema.getProperties().entrySet().stream()
                        .flatMap(e -> fields(casing, name.with(e.getKey()), e.getValue().typeOf()).stream())
        ).collect(Collectors.toList());
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default List<OrderField<?>> indexKeys(final Casing casing, final ReferableSchema schema, final io.basestar.schema.Index index) {

        return Stream.concat(
                index.getPartition().stream().map(v -> indexField(casing, schema, index, v)),
                index.getSort().stream().map(v -> indexField(casing, schema, index, v.getName())
                        .sort(SQLUtils.sort(v.getOrder())))
        ).collect(Collectors.toList());
    }

    default Field<Object> indexField(final Casing casing, final ReferableSchema schema, final io.basestar.schema.Index index, final io.basestar.util.Name name) {

        // FIXME: BUG: hacky heuristic
//        if (ReferableSchema.ID.equals(name.last())) {
//            return DSL.field(DSL.name(columnName(casing, name.withoutLast())));
//        } else {
        return DSL.field(DSL.name(columnName(casing, name)));
//        }
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default List<Field<?>> fields(final Casing casing, final ReferableSchema schema, final io.basestar.schema.Index index) {

        final List<io.basestar.util.Name> partitionNames = index.resolvePartitionNames();
        final List<Sort> sortPaths = index.getSort();

        return Stream.of(
                partitionNames.stream()
                        .map(v -> DSL.field(columnName(casing, v), dataType(schema.typeOf(v)).nullable(true))),
                sortPaths.stream()
                        .map(Sort::getName)
                        .map(v -> DSL.field(columnName(casing, v), dataType(schema.typeOf(v)).nullable(true))),
                index.projectionSchema(schema).entrySet().stream()
                        .map(e -> DSL.field(DSL.name(e.getKey()), dataType(e.getValue()).nullable(true)))

        ).flatMap(v -> v).collect(Collectors.toList());
    }

    default org.jooq.Name columnName(final Casing casing, final io.basestar.util.Name v) {

        return DSL.name(v.stream().map(casing::name).collect(Collectors.joining("_")));
    }

    default org.jooq.Name columnName(final io.basestar.util.Name v) {

        return columnName(Casing.AS_SPECIFIED, v);
    }

    default Constraint primaryKey(final Casing casing, final ReferableSchema schema, final io.basestar.schema.Index index) {

        final List<org.jooq.Name> names = new ArrayList<>();
        index.resolvePartitionNames().forEach(v -> names.add(columnName(casing, v)));
        index.getSort().forEach(v -> names.add(columnName(casing, v.getName())));
        return DSL.primaryKey(names.toArray(new org.jooq.Name[0]));
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default List<Field<?>> selectFields(final FieldResolver field, final Name name, final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<List<Field<?>>>() {

            @Override
            public <T> List<Field<?>> visitDefault(final Use<T> type) {

                return field.field().<List<Field<?>>>map(f -> ImmutableList.of(f.as(columnName(name)))).orElseGet(ImmutableList::of);

            }

            @Override
            public <V, T extends Collection<V>> List<Field<?>> visitCollection(final UseCollection<V, T> type) {

                return selectCollection(type, name, field);
            }

            @Override
            public <V> List<Field<?>> visitMap(final UseMap<V> type) {

                return selectMap(type, name, field);
            }

            @Override
            public List<Field<?>> visitRef(final UseRef type) {

                return selectRef(type, name, field);
            }

            @Override
            public List<Field<?>> visitStruct(final UseStruct type) {

                return selectStruct(type, name, field);
            }

            @Override
            public List<Field<?>> visitView(final UseView type) {

                return selectView(type, name, field);
            }
        });
    }

    default <T> Field<T> field(final QueryPart part, final Class<T> type) {

        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            return cast((Field<?>) part, type);
        } else if (part instanceof Condition) {
            return cast(DSL.field((Condition) part), type);
        } else {
            throw new IllegalStateException();
        }
    }

    @SuppressWarnings("unchecked")
    default <T> Field<T> cast(final Field<?> field, final Class<T> type) {

        if (type == Object.class) {
            return (Field<T>) field;
        } else if (type.equals(Double.class)) {
            return (Field<T>) field.cast(SQLDataType.DOUBLE);
        } else {
            return field.cast(type);
        }
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Field<?> field(final QueryPart part) {

        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            return (Field<?>) part;
        } else if (part instanceof Condition) {
            return DSL.field((Condition) part);
        } else {
            throw new IllegalStateException();
        }
    }

    default Condition condition(final QueryPart part) {

        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            return DSL.condition(((Field<?>) part).cast(Boolean.class));
        } else if (part instanceof Condition) {
            return (Condition) part;
        } else {
            throw new IllegalStateException();
        }
    }

    default QueryPart refIdField(final NamingStrategy namingStrategy, final UseRef type, final Name name) {

        return DSL.field(columnName(namingStrategy.getColumnCasing(), name));
    }

    default Optional<? extends Field<?>> missingMetadataValue(final LinkableSchema schema, final String name) {

        return Optional.empty();
    }

    default String createFunctionDDL(final DSLContext context, final org.jooq.Name name, final Use<?> returns, final List<Argument> arguments, final String language, final String definition) {

        return createFunctionDDLHeader(context, name, returns, arguments, language) + " " + createFunctionDDLBody(context, definition);
    }

    default String createSequenceDDL(final DSLContext context, final org.jooq.Name name, final Long start, final Long increment) {

        throw new UnsupportedOperationException();
    }

    default String createFunctionDDLHeader(final DSLContext context, final org.jooq.Name name, final Use<?> returns, final List<Argument> arguments, final String language) {

        return "CREATE OR REPLACE FUNCTION " + name.toString() + createFunctionDDLArguments(context, returns, arguments, language);
    }

    default String createFunctionDDLArguments(final DSLContext context, final Use<?> returns, final List<Argument> arguments, final String language) {

        final Configuration configuration = context.configuration();
        final StringBuilder result = new StringBuilder();
        result.append("(");
        boolean first = true;
        for (final Argument arg : arguments) {
            if (!first) {
                result.append(",");
            }
            result.append(arg.getName());
            result.append(" ");
            result.append(dataType(arg.getType()).getTypeName(configuration));
            first = false;
        }
        result.append(") RETURNS ");
        result.append(dataType(returns).getTypeName(configuration));
        result.append(" ");
        result.append(createFunctionDDLLanguage(language));
        result.append(" ");
        return result.toString();
    }

    default String createFunctionDDLLanguage(final String language) {

        return "LANGUAGE " + language;
    }

    default String createFunctionDDLBody(final DSLContext context, final String definition) {

        return "AS\n$$\n" + definition + "\n$$";
    }

    default boolean supportsUDFs() {

        return false;
    }

    default boolean supportsSequences() {

        return false;
    }

    default boolean supportsMaterializedView(final ViewSchema schema) {

        return false;
    }

    default QueryPart in(final Field<Object> lhs, final Field<Object> rhs) {

        return lhs.in(rhs);
    }

    default QueryPart simpleIn(final Field<Object> lhs, final List<Field<?>> rhs) {

        return lhs.in(rhs.toArray(new Field<?>[0]));
    }

    default Field<?> bind(final Object value) {

        return DSL.val(value);
    }

    default List<Table<?>> describeTables(final DSLContext context, final String tableCatalog, final String tableSchema, final String tableName) {

        final AtomicReference<List<Table<?>>> result = new AtomicReference<>(Immutable.list());
        context.connection(c -> {

            final DelegatingDatabaseMetaData jdbcMeta = new DelegatingDatabaseMetaData(c.getMetaData()) {

                @Override
                public ResultSet getSchemas() throws SQLException {

                    return getSchemas(tableCatalog, tableSchema);
                }

                @Override
                public ResultSet getTables(final String catalog, final String schemaPattern, final String tableNamePattern, final String[] types) throws SQLException {

                    return super.getTables(tableCatalog, tableSchema, tableName, types);
                }
            };
            final Meta meta = context.meta(jdbcMeta);
            result.set(meta.getTables());
        });
        return result.get();
    }

    default Table<?> describeTable(final DSLContext context, final org.jooq.Name name) {

        final String tableCatalog;
        final String tableSchema;
        final String tableName;
        final String[] parts = name.getName();
        if (parts.length == 3) {
            tableCatalog = parts[0];
            tableSchema = parts[1];
            tableName = parts[2];
        } else if (name.parts().length == 2) {
            tableCatalog = null;
            tableSchema = parts[0];
            tableName = parts[1];
        } else {
            throw new IllegalStateException("Cannot understand table name " + name);
        }

        final List<Table<?>> tables = describeTables(context, tableCatalog, tableSchema, tableName);
        final List<Table<?>> matchingTables = tables.stream()
                .filter(v -> nameMatch(v.getQualifiedName(), name))
                .collect(Collectors.toList());

        if (matchingTables.size() > 1) {
            final List<String> matchingNames = matchingTables.stream()
                    .map(Named::getQualifiedName)
                    .map(n -> n.quotedName().toString())
                    .collect(Collectors.toList());
            throw new IllegalStateException("Multiple matching tables found for " + name + ": " + matchingNames);
        } else if (!matchingTables.isEmpty()) {
            return matchingTables.get(0);
        } else {
            throw new IllegalStateException("Table " + name + " not found");
        }
    }

    default boolean nameMatch(final org.jooq.Name name, final org.jooq.Name matchName) {

        final org.jooq.Name[] nameParts = name.parts();
        final org.jooq.Name[] matchNameParts = matchName.parts();
        for (int i = 0; i != Math.min(nameParts.length, matchNameParts.length); ++i) {
            if (i < nameParts.length) {
                if (!nameParts[nameParts.length - (i + 1)].equalsIgnoreCase(matchNameParts[matchNameParts.length - (i + 1)])) {
                    return false;
                }
            }
        }
        return true;
    }

    default SQLExpressionVisitor expressionResolver(final NamingStrategy namingStrategy, final QueryableSchema schema, final Function<Name, QueryPart> columnResolver) {

        final InferenceContext inferenceContext = InferenceContext.from(schema);
        return new SQLExpressionVisitor(this, inferenceContext, columnResolver);
    }

    default ResultQuery<Record1<Long>> incrementSequence(final DSLContext context, final org.jooq.Name sequenceName) {

        throw new UnsupportedOperationException();
    }

    default List<Pair<Field<?>, SelectField<?>>> orderedRecord(final Map<Field<?>, SelectField<?>> record) {

        return record.entrySet().stream().map(Pair::of)
                .sorted(Comparator.comparing(e -> e.getFirst().getUnqualifiedName()))
                .collect(Collectors.toList());
    }

    default int createObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final String id, final Map<Field<?>, SelectField<?>> record) {

        final List<Pair<Field<?>, SelectField<?>>> orderedRecord = orderedRecord(record);

        return context.insertInto(table)
                .columns(Pair.mapToFirst(orderedRecord))
                .select(DSL.select(Pair.mapToSecond(orderedRecord).toArray(new SelectFieldOrAsterisk[0])))
                .execute();
    }

    default int updateObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version, final Map<Field<?>, SelectField<?>> record) {

        Condition condition = idField.eq(id);
        if (version != null) {
            condition = condition.and(versionField.eq(version));
        }

        return context.update(table).set(record)
                .where(condition).limit(DSL.inline(1)).execute();
    }

    default int createHistoryLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version, final Map<Field<?>, SelectField<?>> record) {

        final List<Pair<Field<?>, SelectField<?>>> orderedRecord = orderedRecord(record);

        context.deleteFrom(table).where(idField.eq(id).and(versionField.eq(version))).execute();

        return context.insertInto(table)
                .columns(Pair.mapToFirst(orderedRecord))
                .select(DSL.select(Pair.mapToSecond(orderedRecord).toArray(new SelectFieldOrAsterisk[0])))
                .execute();
    }

    default int deleteObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version) {

        Condition condition = idField.eq(id);
        if (version != null) {
            condition = condition.and(versionField.eq(version));
        }
        return context.deleteFrom(table)
                .where(condition).limit(DSL.inline(1)).execute();
    }

    default SQL getReplacedSqlWithBindings(final String sql, final List<Argument> arguments, final Map<String, Object> values) {

        final List<Object> bindings = new ArrayList<>();
        final StringBuffer str = new StringBuffer();

        final Pattern pattern = Pattern.compile("\\$\\{(.*?)}");
        final Matcher matcher = pattern.matcher(sql);
        while (matcher.find()) {
            final String name = matcher.group(1);
            final Argument argument = arguments.stream().filter(arg -> name.equals(arg.getName()))
                    .findFirst().orElseThrow(() -> new IllegalStateException("Argument " + name + " not found"));
            final Object value = values.get(argument.getName());
            if (value == null && !argument.getType().isOptional()) {
                throw new IllegalStateException("Argument " + argument.getName() + " not provided");
            }
            final Field<?> field = Nullsafe.map(toSQLValue(argument.getType(), value), DSL::field);
            matcher.appendReplacement(str, "?");
            bindings.add(field);
        }
        matcher.appendTail(str);

        return DSL.sql("(" + str + ")", bindings.toArray());
    }
}