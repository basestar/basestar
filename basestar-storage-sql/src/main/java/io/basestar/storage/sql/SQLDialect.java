package io.basestar.storage.sql;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Expression;
import io.basestar.schema.Index;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.use.*;
import io.basestar.schema.util.Casing;
import io.basestar.secret.Secret;
import io.basestar.storage.sql.mapping.*;
import io.basestar.storage.sql.resolver.ColumnResolver;
import io.basestar.storage.sql.util.DelegatingDatabaseMetaData;
import io.basestar.util.Name;
import io.basestar.util.*;
import org.jooq.Constraint;
import org.jooq.Named;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public interface SQLDialect {

    org.jooq.SQLDialect dmlDialect();

    default org.jooq.SQLDialect ddlDialect() {

        return dmlDialect();
    }

    DataType<String> stringType(UseStringLike<?> type);

    default DataType<Boolean> booleanType(final UseBoolean type) {

        return SQLDataType.BOOLEAN;
    }

    default DataType<Long> integerType(final UseInteger type) {

        return SQLDataType.BIGINT;
    }

    default DataType<Double> numberType(final UseNumber type) {

        return SQLDataType.DOUBLE;
    }

    default DataType<java.sql.Date> dateType(final UseDate type) {

        return SQLDataType.DATE;
    }

    default DataType<Timestamp> dateTimeType(final UseDateTime type) {

        return SQLDataType.TIMESTAMP;
    }

    default DataType<String> enumType(final UseEnum type) {

        return stringType(UseString.DEFAULT);
    }

    default DataType<BigDecimal> decimalType(final UseDecimal type) {

        return SQLDataType.DECIMAL(type.getPrecision(), type.getScale());
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

                return decimalType(type);
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

    default QueryMapping schemaMapping(final QuerySchema schema, final Map<String, Object> arguments, final Set<Name> expand) {

        return new QueryMapping(schema, arguments, null, false, propertyMappings(schema, expand), linkMappings(schema, expand));
    }

    default QueryMapping schemaMapping(final LinkableSchema schema, final boolean versioned, final Set<Name> expand) {

        return new QueryMapping(schema, ImmutableMap.of(), null, versioned, propertyMappings(schema, expand), linkMappings(schema, expand));
    }

    default QueryMapping schemaMapping(final LinkableSchema schema, final Index index, final Set<Name> expand) {

        if (index != null) {
            throw new UnsupportedOperationException();
        } else {
            return schemaMapping(schema, false, expand);
        }
    }

    default Map<String, PropertyMapping<?>> propertyMappings(final QueryableSchema schema, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, PropertyMapping<?>> properties = new HashMap<>();
        schema.metadataSchema().forEach((name, type) -> {
            final Set<Name> branch = branches.get(name);
            properties.put(name, propertyMapping(type, branch));
        });
        schema.getProperties().forEach((name, prop) -> {
            final Set<Name> branch = branches.get(name);
            properties.put(name, propertyMapping(prop.getType(), branch));
        });
        return properties;
    }

    default Map<String, LinkMapping> linkMappings(final QueryableSchema schema, final Set<Name> expand) {

        final Map<String, Set<Name>> branches = Name.branch(expand);
        final Map<String, LinkMapping> links = new HashMap<>();
        schema.getLinks().forEach((name, link) -> {
            final Set<Name> branch = branches.get(name);
            if (branch != null) {
                linkMapping(link, branch).ifPresent(mapping -> links.put(name, mapping));
            }
        });
        return links;
    }

    default Optional<LinkMapping> linkMapping(final Link link, final Set<Name> expand) {

        if (link.isSingle()) {
            final Expression expression = link.getExpression().bind(io.basestar.expression.Context.init());
            final StrictEqualityVisitor visitor = new StrictEqualityVisitor();
            final Map<Name, Name> names = visitor.visit(expression);
            final List<String> primaryKey = link.getSchema().primaryKey();
            if (!primaryKey.isEmpty() && primaryKey.stream().allMatch(key -> {
                final Name keyName = Name.of(key);
                return names.containsKey(keyName) || names.containsValue(keyName);
            })) {
                return Optional.of(new SingleLinkMapping(expression, schemaMapping(link.getSchema(), false, expand)));
            }
        }
        return Optional.empty();
    }


    default PropertyMapping<Boolean> booleanMapping(final UseBoolean type) {

        return PropertyMapping.simple(booleanType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    default PropertyMapping<Long> integerMapping(final UseInteger type) {

        return PropertyMapping.simple(integerType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    default PropertyMapping<Double> numberMapping(final UseNumber type) {

        return PropertyMapping.simple(numberType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    default PropertyMapping<String> stringMapping(final UseString type) {

        return PropertyMapping.simple(stringType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    default PropertyMapping<String> enumMapping(final UseEnum type) {

        return PropertyMapping.simple(stringType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    default PropertyMapping<BigDecimal> decimalMapping(final UseDecimal type) {

        return PropertyMapping.simple(decimalType(type), new ValueTransform.Coercing<>(type::create, v -> v));
    }

    PropertyMapping<LocalDate> dateMapping(UseDate type);

    PropertyMapping<Instant> dateTimeMapping(UseDateTime type);

    PropertyMapping<Map<String, Object>> refMapping(UseRef type, Set<Name> expand);

    PropertyMapping<Map<String, Object>> structMapping(UseStruct type, Set<Name> expand);

    PropertyMapping<Map<String, Object>> viewMapping(UseView type, Set<Name> expand);

    <T> PropertyMapping<List<T>> arrayMapping(UseArray<T> type, Set<Name> expand);

    <T> PropertyMapping<Page<T>> pageMapping(UsePage<T> type, Set<Name> expand);

    <T> PropertyMapping<Set<T>> setMapping(UseSet<T> type, Set<Name> expand);

    <T> PropertyMapping<Map<String, T>> mapMapping(UseMap<T> type, Set<Name> expand);

    PropertyMapping<Bytes> binaryMapping(UseBinary type);

    PropertyMapping<Secret> secretMapping(UseSecret type);

    <T> PropertyMapping<Object> anyMapping(UseAny type);

    default PropertyMapping<?> propertyMapping(final Use<?> type, final Set<Name> expand) {

        return type.visit(new Use.Visitor<PropertyMapping<?>>() {
            @Override
            public PropertyMapping<?> visitBoolean(final UseBoolean type) {

                return booleanMapping(type);
            }

            @Override
            public PropertyMapping<?> visitInteger(final UseInteger type) {

                return integerMapping(type);
            }

            @Override
            public PropertyMapping<?> visitNumber(final UseNumber type) {

                return numberMapping(type);
            }

            @Override
            public PropertyMapping<?> visitString(final UseString type) {

                return stringMapping(type);
            }

            @Override
            public PropertyMapping<?> visitEnum(final UseEnum type) {

                return enumMapping(type);
            }

            @Override
            public PropertyMapping<?> visitRef(final UseRef type) {

                return refMapping(type, expand);
            }

            @Override
            public <T> PropertyMapping<?> visitArray(final UseArray<T> type) {

                return arrayMapping(type, expand);
            }

            @Override
            public <T> PropertyMapping<?> visitSet(final UseSet<T> type) {

                return setMapping(type, expand);
            }

            @Override
            public <T> PropertyMapping<?> visitMap(final UseMap<T> type) {

                return mapMapping(type, expand);
            }

            @Override
            public PropertyMapping<?> visitStruct(final UseStruct type) {

                return structMapping(type, expand);
            }

            @Override
            public PropertyMapping<?> visitBinary(final UseBinary type) {

                return binaryMapping(type);
            }

            @Override
            public PropertyMapping<?> visitDate(final UseDate type) {

                return dateMapping(type);
            }

            @Override
            public PropertyMapping<?> visitDateTime(final UseDateTime type) {

                return dateTimeMapping(type);
            }

            @Override
            public PropertyMapping<?> visitView(final UseView type) {

                return viewMapping(type, expand);
            }

            @Override
            public PropertyMapping<?> visitQuery(final UseQuery type) {

                throw new UnsupportedOperationException();
            }

            @Override
            public <T> PropertyMapping<?> visitOptional(final UseOptional<T> type) {

                return type.getType().visit(this).nullable();
            }

            @Override
            public PropertyMapping<?> visitAny(final UseAny type) {

                return anyMapping(type);
            }

            @Override
            public PropertyMapping<?> visitSecret(final UseSecret type) {

                return secretMapping(type);
            }

            @Override
            public <T> PropertyMapping<?> visitPage(final UsePage<T> type) {

                return pageMapping(type, expand);
            }

            @Override
            public PropertyMapping<?> visitDecimal(final UseDecimal type) {

                return decimalMapping(type);
            }

            @Override
            public PropertyMapping<?> visitComposite(final UseComposite type) {

                throw new UnsupportedOperationException();
            }
        });
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
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

    @Deprecated
    default Condition condition(final QueryPart part) {

        if (part == null) {
            return null;
        } else if (part instanceof Field<?>) {
            final Field<?> field = (Field<?>) part;
            return field.isNotNull().and(field.cast(Boolean.class));
        } else if (part instanceof Condition) {
            return (Condition) part;
        } else {
            throw new IllegalStateException();
        }
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

    default SQLExpressionVisitor expressionResolver(final InferenceContext inferenceContext, final ColumnResolver columnResolver) {

        return new SQLExpressionVisitor(this, inferenceContext, columnResolver::requireColumn);
    }

    default ResultQuery<Record1<Long>> incrementSequence(final DSLContext context, final org.jooq.Name sequenceName) {

        throw new UnsupportedOperationException();
    }

    default List<Pair<Field<?>, SelectField<?>>> orderedRecord(final Map<Field<?>, SelectField<?>> record) {

        return record.entrySet().stream().map(Pair::of)
                .sorted(Comparator.comparing(e -> e.getFirst().getUnqualifiedName()))
                .collect(Collectors.toList());
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
            @SuppressWarnings("unchecked") final PropertyMapping<Object> propertyMapping = (PropertyMapping<Object>) propertyMapping(argument.getType(), ImmutableSet.of());
            final Field<?> field = DSL.field(propertyMapping.emitMerged(value, ImmutableSet.of()));
            matcher.appendReplacement(str, "?");
            bindings.add(field);
        }
        matcher.appendTail(str);

        return DSL.sql("(" + str + ")", bindings.toArray());
    }

}