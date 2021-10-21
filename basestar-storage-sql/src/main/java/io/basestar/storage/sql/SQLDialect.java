package io.basestar.storage.sql;

import io.basestar.schema.*;
import io.basestar.schema.use.*;
import io.basestar.secret.Secret;
import io.basestar.schema.util.Casing;
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

    <T> Object arrayToSQLValue(UseArray<T> type, List<T> value);

    <T> Object pageToSQLValue(UsePage<T> type, Page<T> value);

    <T> Object setToSQLValue(UseSet<T> type, Set<T> value);

    <T> Object mapToSQLValue(UseMap<T> type, Map<String, T> value);

    Object structToSQLValue(UseStruct type, Instance value);

    Object viewToSQLValue(UseView type, Instance value);

    Object refToSQLValue(UseRef type, Instance value);

    Object binaryToSQLValue(UseBinary type, Bytes value);

    default Object secretToSQLValue(final UseSecret type, final Secret value) {

        return binaryToSQLValue(UseBinary.DEFAULT, new Bytes(value.encrypted()));
    }

    Object anyToSQLValue(UseAny type, Object value);

    <T> List<T> arrayFromSQLValue(UseArray<T> type, Object value);

    <T> Page<T> pageFromSQLValue(UsePage<T> type, Object value);

    <T> Set<T> setFromSQLValue(UseSet<T> type, Object value);

    <T> Map<String, T> mapFromSQLValue(UseMap<T> type, Object value);

    Instance structFromSQLValue(UseStruct type, Object value);

    Instance viewFromSQLValue(UseView type, Object value);

    Instance refFromSQLValue(UseRef type, Object value);

    Bytes binaryFromSQLValue(UseBinary type, Object value);

    default Secret secretFromSQLValue(final UseSecret type, final Object value) {

        return Secret.encrypted(binaryFromSQLValue(UseBinary.DEFAULT, value).getBytes());
    }

    Object anyFromSQLValue(UseAny type, Object value);

    <V, T extends Collection<V>> Field<?> selectCollection(final UseCollection<V, T> type, final Field<?> field);

    <V> Field<?> selectMap(final UseMap<V> type, final Field<?> field);

    Field<?> selectRef(final UseRef type, final Field<?> field);

    Field<?> selectStruct(final UseStruct type, final Field<?> field);

    Field<?> selectView(final UseView type, final Field<?> field);

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

    default Object toSQLValue(final Use<?> type, final Object value) {

        if (value == null) {
            return null;
        }
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
            public Object visitRef(final UseRef type) {

                return refToSQLValue(type, (Instance) value);
            }

            @Override
            public <T> Object visitArray(final UseArray<T> type) {

                return arrayToSQLValue(type, type.create(value));
            }

            @Override
            public <T> Object visitPage(final UsePage<T> type) {

                return pageToSQLValue(type, type.create(value));
            }

            @Override
            public Object visitDecimal(final UseDecimal type) {

                return type.create(value);
            }

            @Override
            public Object visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> Object visitSet(final UseSet<T> type) {

                return setToSQLValue(type, type.create(value));
            }

            @Override
            public <T> Object visitMap(final UseMap<T> type) {

                return mapToSQLValue(type, type.create(value));
            }

            @Override
            public Object visitStruct(final UseStruct type) {

                return structToSQLValue(type, type.create(value));
            }

            @Override
            public Object visitAny(final UseAny type) {

                return anyToSQLValue(type, type.create(value));
            }

            @Override
            public Object visitBinary(final UseBinary type) {

                return binaryToSQLValue(type, type.create(value));
            }

            @Override
            public Object visitDate(final UseDate type) {

                return ISO8601.toSqlDate(type.create(value));
            }

            @Override
            public Object visitDateTime(final UseDateTime type) {

                return ISO8601.toSqlTimestamp(type.create(value));
            }

            @Override
            public Object visitView(final UseView type) {

                return viewToSQLValue(type, type.create(value));
            }

            @Override
            public <T> Object visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this);
            }

            @Override
            public Object visitSecret(final UseSecret type) {

                return secretToSQLValue(type, type.create(value));
            }
        });
    }

    default Object fromSQLValue(final Use<?> type, final Object value) {

        if (value == null) {
            return null;
        }

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

                return type.create(value);
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
            public LocalDate visitDate(final UseDate type) {

                return type.create(value);
            }

            @Override
            public Instant visitDateTime(final UseDateTime type) {

                return type.create(value);
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

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default List<Field<?>> fields(final Casing casing, final LinkableSchema schema) {

        return Stream.concat(
                schema.metadataSchema().entrySet().stream()
                        .map(e -> DSL.field(DSL.name(casing.name(e.getKey())), dataType(e.getValue()))),
                schema.getProperties().entrySet().stream()
                        .map(e -> DSL.field(DSL.name(casing.name(e.getKey())),
                                dataType(e.getValue().typeOf())))
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
        if (ReferableSchema.ID.equals(name.last())) {
            return DSL.field(DSL.name(columnName(casing, name.withoutLast())));
        } else {
            return DSL.field(DSL.name(columnName(casing, name)));
        }
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

    default io.basestar.util.Name columnPath(final io.basestar.util.Name v) {

        return io.basestar.util.Name.of(v.toString(Reserved.PREFIX));
    }

    default org.jooq.Name columnName(final Casing casing, final io.basestar.util.Name v) {

        return DSL.name(v.stream().map(casing::name).collect(Collectors.joining(Reserved.PREFIX)));
    }

    default Constraint primaryKey(final Casing casing, final ReferableSchema schema, final io.basestar.schema.Index index) {

        final List<org.jooq.Name> names = new ArrayList<>();
        index.resolvePartitionNames().forEach(v -> names.add(columnName(casing, v)));
        index.getSort().forEach(v -> names.add(columnName(casing, v.getName())));
        return DSL.primaryKey(names.toArray(new org.jooq.Name[0]));
    }

    @SuppressWarnings(Warnings.RETURN_GENERIC_WILDCARD)
    default Field<?> selectField(final Field<?> field, final Use<?> type) {

        return type.visit(new Use.Visitor.Defaulting<Field<?>>() {

            @Override
            public <T> Field<?> visitDefault(final Use<T> type) {

                return field;
            }

            @Override
            public <V, T extends Collection<V>> Field<?> visitCollection(final UseCollection<V, T> type) {

                return selectCollection(type, field);
            }

            @Override
            public <V> Field<?> visitMap(final UseMap<V> type) {

                return selectMap(type, field);
            }

            @Override
            public Field<?> visitRef(final UseRef type) {

                return selectRef(type, field);
            }

            @Override
            public Field<?> visitStruct(final UseStruct type) {

                return selectStruct(type, field);
            }

            @Override
            public Field<?> visitView(final UseView type) {

                return selectView(type, field);
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

        final Name rest = name.withoutFirst();
        final Field<String> sourceId = DSL.field(DSL.name(name.first()), String.class);
        if (rest.equals(ObjectSchema.ID_NAME)) {
            return sourceId;
        } else {
            throw new UnsupportedOperationException("Query of this type is not supported");
        }
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
}