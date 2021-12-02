package io.basestar.storage.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.schema.util.Casing;
import io.basestar.secret.Secret;
import io.basestar.storage.sql.resolver.FieldResolver;
import io.basestar.storage.sql.resolver.ValueResolver;
import io.basestar.util.Name;
import io.basestar.util.*;
import org.jooq.Constraint;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.time.Instant;
import java.time.LocalDate;
import java.util.*;
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

    <T> Map<Field<?>, SelectField<?>> arrayToSQLValues(UseArray<T> type, FieldResolver field, List<T> value);

    Map<Field<?>, SelectField<?>> dateToSQLValues(UseDate type, FieldResolver field, LocalDate value);

    Map<Field<?>, SelectField<?>> dateTimeToSQLValues(UseDateTime type, FieldResolver field, Instant value);

    <T> Map<Field<?>, SelectField<?>> pageToSQLValues(UsePage<T> type, FieldResolver field, Page<T> value);

    <T> Map<Field<?>, SelectField<?>> setToSQLValues(UseSet<T> type, FieldResolver field, Set<T> value);

    <T> Map<Field<?>, SelectField<?>> mapToSQLValues(UseMap<T> type, FieldResolver field, Map<String, T> value);

    Map<Field<?>, SelectField<?>> structToSQLValues(UseStruct type, FieldResolver field, Instance value);

    Map<Field<?>, SelectField<?>> viewToSQLValues(UseView type, FieldResolver field, Instance value);

    Map<Field<?>, SelectField<?>> refToSQLValues(UseRef type, FieldResolver field, Instance value);

    Map<Field<?>, SelectField<?>> binaryToSQLValues(UseBinary type, FieldResolver field, Bytes value);

    default Map<Field<?>, SelectField<?>> secretToSQLValues(final UseSecret type, final FieldResolver field, final Secret value) {

        return binaryToSQLValues(UseBinary.DEFAULT, field, new Bytes(value.encrypted()));
    }

    Map<Field<?>, SelectField<?>> anyToSQLValues(UseAny type, FieldResolver field, Object value);

    <T> List<T> arrayFromSQLValue(UseArray<T> type, ValueResolver value);

    <T> Page<T> pageFromSQLValue(UsePage<T> type, ValueResolver value);

    <T> Set<T> setFromSQLValue(UseSet<T> type, ValueResolver value);

    <T> Map<String, T> mapFromSQLValue(UseMap<T> type, ValueResolver value);

    Instance structFromSQLValue(UseStruct type, ValueResolver value);

    Instance viewFromSQLValue(UseView type, ValueResolver value);

    Instance refFromSQLValue(UseRef type, ValueResolver value);

    Bytes binaryFromSQLValue(UseBinary type, ValueResolver value);

    default Secret secretFromSQLValue(final UseSecret type, final ValueResolver value) {

        return Secret.encrypted(binaryFromSQLValue(UseBinary.DEFAULT, value).getBytes());
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

    default QueryPart refIdField(final UseRef type, final Name name) {

        return DSL.field(columnName(name));
    }

    default Optional<? extends Field<?>> missingMetadataValue(final LinkableSchema schema, final String name) {

        return Optional.empty();
    }

    default String createFunctionDDL(final DSLContext context, final org.jooq.Name name, final Use<?> returns, final List<Argument> arguments, final String language, final String definition) {

        return createFunctionDDLHeader(context, name, returns, arguments, language) + " " + createFunctionDDLBody(context, definition);
    }

    default String createFunctionDDLHeader(final DSLContext context, final org.jooq.Name name, final Use<?> returns, final List<Argument> arguments, final String language) {

        final Configuration configuration = context.configuration();
        final StringBuilder result = new StringBuilder();
        result.append("CREATE OR REPLACE FUNCTION ");
        result.append(name.toString());
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

        return true;
    }

    default boolean supportsMaterializedView(final ViewSchema schema) {

        return false;
    }

    default QueryPart in(final Field<Object> lhs, final Field<Object> rhs) {

        return lhs.in(rhs);
    }
}