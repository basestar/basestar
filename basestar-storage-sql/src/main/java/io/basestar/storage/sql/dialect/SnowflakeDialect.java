package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.basestar.expression.Expression;
import io.basestar.expression.constant.NameConstant;
import io.basestar.expression.iterate.ContextIterator;
import io.basestar.expression.iterate.For;
import io.basestar.expression.iterate.ForAll;
import io.basestar.expression.iterate.ForAny;
import io.basestar.schema.*;
import io.basestar.schema.expression.InferenceContext;
import io.basestar.schema.from.FromSql;
import io.basestar.schema.use.UseAny;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseSet;
import io.basestar.storage.sql.SQLExpressionVisitor;
import io.basestar.storage.sql.strategy.NamingStrategy;
import io.basestar.util.Pair;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class SnowflakeDialect extends JSONDialect {

    private static final org.jooq.SQLDialect DIALECT = SQLDialect.POSTGRES;

    private static final DataType<?> ARRAY_TYPE = new DefaultDataType<>(SQLDialect.POSTGRES, Object.class, "ARRAY");

    private static final DataType<?> OBJECT_TYPE = new DefaultDataType<>(SQLDialect.POSTGRES, Object.class, "OBJECT");

    private static final DataType<?> VARIANT_TYPE = new DefaultDataType<>(SQLDialect.POSTGRES, Object.class, "VARIANT");

    @Override
    public org.jooq.SQLDialect dmlDialect() {

        return DIALECT;
    }

    @Override
    protected DataType<?> jsonType() {

        return VARIANT_TYPE;
    }

    @Override
    public <T> DataType<?> arrayType(final UseArray<T> type) {

        return ARRAY_TYPE;
    }

    @Override
    public <T> DataType<?> setType(final UseSet<T> type) {

        return ARRAY_TYPE;
    }

    @Override
    public <T> DataType<?> mapType(final UseMap<T> type) {

        return OBJECT_TYPE;
    }

    @Override
    public DataType<?> anyType(final UseAny type) {

        return VARIANT_TYPE;
    }

    @Override
    protected SelectField<?> castJson(final String value) {

        return DSL.function("parse_json", Object.class, DSL.val(value));
    }

    @Override
    public boolean supportsUDFs() {

        return true;
    }

    @Override
    public boolean supportsSequences() {

        return true;
    }

    @Override
    public boolean supportsConstraints() {

        return false;
    }

    @Override
    public boolean supportsIndexes() {

        return false;
    }

    @Override
    public boolean supportsILike() {

        return true;
    }

    @Override
    protected boolean isJsonEscaped() {

        return false;
    }

    @Override
    public Optional<? extends Field<?>> missingMetadataValue(final LinkableSchema schema, final String name) {

        if (schema instanceof ViewSchema && ViewSchema.ID.equals(name)) {
            return Optional.of(DSL.inline((String) null));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean supportsMaterializedView(final ViewSchema schema) {

//        return schema.getFrom() instanceof FromSql && (((FromSql) schema.getFrom()).getUsing().size() == 1);
        return false;
    }

    @Override
    public String createFunctionDDLLanguage(final String language) {

        if ("sql".equalsIgnoreCase(language)) {
            return "";
        } else {
            return super.createFunctionDDLLanguage(language);
        }
    }

    @Override
    public String createSequenceDDL(final DSLContext context, final Name name, final Long start, final Long increment) {

        final StringBuilder str = new StringBuilder();
        str.append("CREATE SEQUENCE IF NOT EXISTS ");
        str.append(name);
        if (start != null) {
            str.append(" WITH START = ");
            str.append(DSL.inline(start));
        }
        if (increment != null) {
            str.append(" INCREMENT = ");
            str.append(DSL.inline(increment));
        }
        return str.toString();
    }

    @Override
    public QueryPart in(final Field<Object> lhs, final Field<Object> rhs) {

        return DSL.condition(DSL.sql("ARRAY_CONTAINS(?::VARIANT, ?)", lhs, rhs));
    }

    @Override
    public List<Table<?>> describeTables(final DSLContext context, final String tableCatalog, final String tableSchema, final String tableName) {

        final List<Table<?>> tables = new ArrayList<>();
        final Name qualifiedSchemaName = tableCatalog == null ? DSL.name(tableSchema) : DSL.name(DSL.name(tableCatalog), DSL.name(tableSchema));
        final Result<org.jooq.Record> tablesResult = context.resultQuery(DSL.sql("SHOW OBJECTS LIKE " + DSL.inline(tableName) + " IN SCHEMA " + qualifiedSchemaName)).fetch();
        tablesResult.forEach(tableResult -> {
            final String resolvedTableCatalog = tableResult.get("database_name", String.class);
            final String resolvedTableSchema = tableResult.get("schema_name", String.class);
            final String resolvedTableName = tableResult.get("name", String.class);
            final String resolvedKind = tableResult.get("kind", String.class);
            final Name qualifiedName = DSL.name(DSL.name(resolvedTableCatalog), DSL.name(resolvedTableSchema), DSL.name(resolvedTableName));
            final Result<org.jooq.Record> columnsResult = context.resultQuery(DSL.sql("SHOW COLUMNS IN " + resolvedKind + " " + qualifiedName)).fetch();
            final Table<?> table = new TableImpl<org.jooq.Record>(qualifiedName) {
                {
                    columnsResult.forEach(columnResult -> {
                        final String realColumnName = columnResult.get("column_name", String.class);
                        final String dataType = columnResult.get("data_type", String.class);
                        createField(DSL.name(realColumnName), describedColumnType(dataType));
                    });
                }
            };
            tables.add(table);
        });
        return tables;
    }

    private DataType<?> describedColumnType(final String dataType) {

        try {
            final SnowflakeColumnType columnType = objectMapper.readValue(dataType, SnowflakeColumnType.class);
            final String typeName = columnType.getType().toUpperCase();
            final int precision = columnType.getPrecision();
            final int scale = columnType.getScale();
            final int length = columnType.getLength();
            final boolean nullable = columnType.isNullable();
            // Special cases for when known snowflake type doesn't correspond to the wrapped postgres dialect
            switch (typeName) {
                case "TIMESTAMP_NTZ":
                    return SQLDataType.TIMESTAMP(precision).nullable(nullable);
                case "TIMESTAMP_TZ":
                    return SQLDataType.TIMESTAMPWITHTIMEZONE(precision).nullable(nullable);
                case "TEXT":
                    return SQLDataType.LONGVARCHAR(length);
                case "FIXED":
                    return SQLDataType.NUMERIC(precision, scale).nullable(nullable);
                case "REAL":
                    return SQLDataType.DOUBLE.nullable(nullable);
                case "BINARY":
                    return SQLDataType.LONGVARBINARY(length).nullable(nullable);
                case "VARIANT":
                case "OBJECT":
                case "ARRAY":
                    return SQLDataType.OTHER.nullable(nullable);
                default:
                    return DefaultDataType.getDataType(DIALECT.family(), typeName, precision, scale).length(length).nullable(nullable);
            }
        } catch (final Exception e) {
            log.error("Failed to calculate snowflake data type for {}", dataType, e);
            return SQLDataType.OTHER;
        }
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class SnowflakeColumnType {

        private String type;

        private int length;

        private int precision;

        private int scale;

        private boolean nullable;
    }

    @Override
    public SQLExpressionVisitor expressionResolver(final NamingStrategy namingStrategy, final QueryableSchema schema, final Function<io.basestar.util.Name, QueryPart> columnResolver) {

        final InferenceContext inferenceContext = InferenceContext.from(schema);
        return new SQLExpressionVisitor(this, inferenceContext, columnResolver) {

            private List<Field<?>> idFields(final QueryableSchema schema) {

                if (schema instanceof ViewSchema) {
                    final ViewSchema viewSchema = (ViewSchema) schema;
                    if (viewSchema.getFrom() instanceof FromSql) {
                        final FromSql fromSql = (FromSql) viewSchema.getFrom();
                        final List<String> primaryKey = fromSql.getPrimaryKey();
                        if (primaryKey != null && !primaryKey.isEmpty()) {
                            return primaryKey.stream()
                                    .map(name -> DSL.field(namingStrategy.columnName(io.basestar.util.Name.of(name))))
                                    .collect(Collectors.toList());
                        }
                    }
                } else if (schema instanceof ReferableSchema) {
                    return ImmutableList.of(DSL.field(namingStrategy.columnName(io.basestar.util.Name.of(ReferableSchema.ID))));
                }
                throw new IllegalStateException("For any/for all only supported for object schemas, and view schemas with a primaryKey");
            }

            @Override
            public QueryPart visitForAny(final ForAny expression) {

                return visitFor(expression, false);
            }

            @Override
            public QueryPart visitForAll(final ForAll expression) {

                return visitFor(expression, true);
            }

            /*
            Implements for-any and for-all behaviour, assuming the following:

             - the current schema is an object schema, or a sql view with a defined primary key
             - the iterator used is an array (value type) iterator rather than a map (key-value type) iterator
             - the iterator is over a simple column (collapses to a constant name)

             The last two conditions could probably be relaxed, but want to establish if this is a viable/performant
             enough implementation first.

             Approach used is to create a non-correlated subquery in the where clause that lateral joins the table
             of the current row to the flattened iterated column, and then boolean and/or aggregate the result
             of applying the lhs condition to each rhs value. This subquery is then implicitly joined to the current
             row using tuple-equality over the id columns and the expected 'true' result of the boolean aggregation.

             This may be less performant than a UDF implementation in that the result of other terms in the surrounding
             query cannot short-circuit the inclusion of the related records in the subquery. However, this is more
             likely to get optimized by the query planner than an opaque UDF.
             */

            private QueryPart visitFor(final For expression, final boolean all) {

                final String alias1 = Reserved.PREFIX + "1";
                final String alias2 = Reserved.PREFIX + "2";

                final Table<?> table = DSL.table(namingStrategy.entityName(schema));

                final ContextIterator iterator = expression.getIterator();
                // map keys not supported
                if (iterator instanceof ContextIterator.OfValue) {
                    final ContextIterator.OfValue of = (ContextIterator.OfValue) iterator;
                    if (of.getExpr() instanceof NameConstant) {
                        final Expression lhs = expression.getYields().get(0);
                        final NameConstant rhs = (NameConstant) of.getExpr();
                        final String as = ((ContextIterator.OfValue) iterator).getValue();

                        final List<Field<?>> idFields = idFields(schema);

                        final SQLExpressionVisitor lhsVisitor = expressionResolver(namingStrategy, schema, name -> {
                            if (name.first().equals(as)) {
                                if (name.size() == 1) {
                                    return DSL.field(DSL.name(alias2, "VALUE"));
                                } else {
                                    return DSL.field(DSL.sql(DSL.name(alias2, "VALUE") + ":" + name.withoutFirst()
                                            .stream().map(v -> DSL.name(v).toString())
                                            .collect(Collectors.joining("."))));
                                }
                            } else {
                                return DSL.field(DSL.name(alias1).append(namingStrategy.columnName(name)));
                            }
                        });

                        final List<Field<?>> aggFields = new ArrayList<>(idFields);
                        aggFields.add(DSL.field(DSL.sql(all ? "BOOLAND_AGG(?)" : "BOOLOR_AGG(?)", lhsVisitor.field(lhs))));

                        final List<Field<?>> matchFields = new ArrayList<>(idFields);
                        matchFields.add(DSL.inline(true));

                        final Field<Object> match = DSL.field(DSL.sql("(" + Joiner.on(", ").join(matchFields) + ")"));
                        final Condition query = match.in(DSL.select(aggFields).from(table.as(alias1),
                                        DSL.table(DSL.sql("LATERAL FLATTEN(input => ?)", namingStrategy.columnName(rhs.getName()))).as(alias2))
                                .groupBy(idFields));

                        if (all) {
                            // For-all over an empty array should always return true
                            return DSL.field(DSL.sql("ARRAY_SIZE(?)", visit(rhs))).eq(DSL.inline(0)).or(query);
                        } else {
                            return query;
                        }
                    }
                }
                return null;
            }
        };
    }

    @Override
    public ResultQuery<Record1<Long>> incrementSequence(final DSLContext context, final Name sequenceName) {

        return context.select(DSL.field(DSL.sql(sequenceName + ".nextval")).cast(Long.class));
    }

    private void merge(final StringBuilder merge, final org.jooq.Table<?> table, final Field<String> idField) {

        merge.append("MERGE INTO ");
        merge.append(table.getQualifiedName());
        merge.append(" AS TARGET USING (?) AS SOURCE ON SOURCE.");
        merge.append(idField.getUnqualifiedName());
        merge.append(" = TARGET.");
        merge.append(idField.getUnqualifiedName());
        merge.append(" ");
    }

    private void merge(final StringBuilder merge, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField) {

        merge(merge, table, idField);
        merge.append("AND SOURCE.");
        merge.append(versionField.getUnqualifiedName());
        merge.append(" = TARGET.");
        merge.append(versionField.getUnqualifiedName());
        merge.append(" ");
    }

    private void mergeNotMatchedInsert(final StringBuilder merge, final List<Pair<Field<?>, SelectField<?>>> record) {

        merge.append("WHEN NOT MATCHED THEN INSERT (");
        merge.append(record.stream().map(Pair::getFirst).map(f -> f.getUnqualifiedName().toString()).collect(Collectors.joining(",")));
        merge.append(") VALUES (");
        merge.append(record.stream().map(Pair::getFirst).map(f -> "SOURCE." + f.getUnqualifiedName().toString()).collect(Collectors.joining(",")));
        merge.append(") ");
    }

    private void mergeWhenMatched(final StringBuilder merge, final Field<Long> versionField, final Long version) {

        merge.append("WHEN MATCHED");
        if (version != null) {
            merge.append(" AND TARGET.");
            merge.append(versionField.getUnqualifiedName());
            merge.append(" = ");
            merge.append(DSL.inline(version));
        }
        merge.append(" ");
    }

    private QueryPart mergeSelect(final List<Pair<Field<?>, SelectField<?>>> record) {

        return DSL.select(record.stream()
                .map(e -> DSL.field(e.getSecond()).as(e.getFirst().getUnqualifiedName()))
                .toArray(SelectFieldOrAsterisk[]::new));
    }

    @Override
    public int createObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final String id, final Map<Field<?>, SelectField<?>> record) {

        final List<Pair<Field<?>, SelectField<?>>> orderedRecord = orderedRecord(record);

        final StringBuilder merge = new StringBuilder();
        merge(merge, table, idField);
        mergeNotMatchedInsert(merge, orderedRecord);

        return context.execute(DSL.sql(merge.toString(), mergeSelect(orderedRecord)));
    }

    @Override
    public int updateObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version, final Map<Field<?>, SelectField<?>> record) {

        final List<Pair<Field<?>, SelectField<?>>> orderedRecord = orderedRecord(record);

        final StringBuilder merge = new StringBuilder();
        merge(merge, table, idField);
        mergeWhenMatched(merge, versionField, version);
        merge.append("THEN UPDATE SET ");
        merge.append(record.keySet().stream()
                .map(f -> "TARGET." + f.getUnqualifiedName() + " = SOURCE." + f.getUnqualifiedName())
                .collect(Collectors.joining(",")));

        return context.execute(DSL.sql(merge.toString(), mergeSelect(orderedRecord)));
    }

    @Override
    public int deleteObjectLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version) {

        final StringBuilder merge = new StringBuilder();
        merge(merge, table, idField);
        mergeWhenMatched(merge, versionField, version);
        merge.append("THEN DELETE");

        return context.execute(DSL.sql(merge.toString(), mergeSelect(orderedRecord(ImmutableMap.of(idField, DSL.inline(id))))));
    }

    @Override
    public int createHistoryLayer(final DSLContext context, final org.jooq.Table<?> table, final Field<String> idField, final Field<Long> versionField, final String id, final Long version, final Map<Field<?>, SelectField<?>> record) {

        final List<Pair<Field<?>, SelectField<?>>> orderedRecord = orderedRecord(record);

        final StringBuilder merge = new StringBuilder();
        merge(merge, table, idField, versionField);
        mergeNotMatchedInsert(merge, orderedRecord);
        merge.append("WHEN MATCHED THEN UPDATE SET ");
        merge.append(record.keySet().stream()
                .map(f -> "TARGET." + f.getUnqualifiedName() + " = SOURCE." + f.getUnqualifiedName())
                .collect(Collectors.joining(",")));

        return context.execute(DSL.sql(merge.toString(), mergeSelect(orderedRecord)));
    }
}
