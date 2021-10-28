package io.basestar.storage.sql.strategy;

import io.basestar.schema.FunctionSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.util.Casing;
import io.basestar.storage.sql.SQLUtils;
import io.basestar.util.Nullsafe;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.jooq.Name;
import org.jooq.impl.DSL;

import javax.annotation.Nullable;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Data
@Slf4j
@Builder(builderClassName = "Builder")
public class DefaultNamingStrategy implements NamingStrategy {

    private static final String CUSTOM_TABLE_NAME_EXTENSION = "sql.table";

    @Nullable
    private final Casing entityCasing;

    @Nullable
    private final Casing columnCasing;

    @Nullable
    private final String delimiter;

    @Nullable
    private final String objectSchemaName;

    private final io.basestar.storage.sql.SQLDialect dialect;

    @Override
    public org.jooq.Name objectTableName(final ReferableSchema schema) {

        return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
    }

    @Override
    public org.jooq.Name functionName(final FunctionSchema schema) {

        return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
    }

    @Override
    public Name viewName(final ViewSchema schema) {

        return combineNames(DSL.name(objectSchemaName), customizeName(schema, () -> DSL.name(name(schema))));
    }

    @Override
    public String columnName(final String name) {

        return Nullsafe.orDefault(columnCasing, Casing.AS_SPECIFIED).name(name);
    }

    @Override
    public Name columnName(final io.basestar.util.Name name) {

        return dialect.columnName(getColumnCasing(), name);
    }

    @Override
    public String name(final Schema<?> schema) {

        final Casing casing = getEntityCasing();
        return schema.getQualifiedName().stream()
                .map(casing::name)
                .collect(Collectors.joining(getDelimiter()));
    }

    private org.jooq.Name customizeName(final Schema<?> schema, final Supplier<Name> defaultName) {

        return schema.getOptionalExtension(String.class, CUSTOM_TABLE_NAME_EXTENSION)
                .map(SQLUtils::parseName)
                .orElseGet(defaultName);
    }

    static org.jooq.Name combineNames(final org.jooq.Name schemaName, final org.jooq.Name name) {

        if (name.getName().length == 1) {
            return DSL.name(schemaName, name);
        } else {
            return name;
        }
    }

    @Override
    public Casing getColumnCasing() {

        return Nullsafe.orDefault(columnCasing, Casing.AS_SPECIFIED);
    }

    @Override
    public Casing getEntityCasing() {

        return Nullsafe.orDefault(entityCasing, Casing.AS_SPECIFIED);
    }

    @Override
    public String getDelimiter() {

        return Nullsafe.orDefault(delimiter, "_");
    }
}
