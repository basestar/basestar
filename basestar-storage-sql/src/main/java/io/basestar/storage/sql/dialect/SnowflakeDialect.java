package io.basestar.storage.sql.dialect;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.use.UseAny;
import io.basestar.schema.use.UseArray;
import io.basestar.schema.use.UseMap;
import io.basestar.schema.use.UseSet;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultDataType;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    public QueryPart in(final Field<Object> lhs, final Field<Object> rhs) {

        return DSL.condition(DSL.sql("ARRAY_CONTAINS(?::VARIANT, ?)", lhs, rhs));
    }

    @Override
    public List<Table<?>> describeTables(final DSLContext context, final String tableCatalog, final String tableSchema, final String tableName) {

        final List<Table<?>> tables = new ArrayList<>();
        final Name qualifiedSchemaName = tableCatalog == null ? DSL.name(tableSchema) : DSL.name(DSL.name(tableCatalog), DSL.name(tableSchema));
        final Result<Record> tablesResult = context.resultQuery(DSL.sql("SHOW OBJECTS LIKE " + DSL.inline(tableName) + " IN SCHEMA " + qualifiedSchemaName)).fetch();
        tablesResult.forEach(tableResult -> {
            final String realTableName = tableResult.get("name", String.class);
            final Name qualifiedName = qualifiedSchemaName.append(DSL.name(realTableName));
            final Result<Record> columnsResult = context.resultQuery(DSL.sql("SHOW COLUMNS IN TABLE " + qualifiedName)).fetch();
            final Table<?> table = new TableImpl<Record>(qualifiedName) {
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
}
