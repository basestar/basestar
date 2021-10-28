package io.basestar.storage.sql.strategy;

import io.basestar.schema.*;
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
    private final String catalogName;

    @Nullable
    private final String objectSchemaName;

    @Nullable
    private final String historySchemaName;


    private final io.basestar.storage.sql.SQLDialect dialect;

    @Override
    public Name catalogName(final ReferableSchema schema) {

        return DSL.name(catalogName);
    }

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

        return dialect.columnName(Nullsafe.orDefault(columnCasing, Casing.AS_SPECIFIED), name);
    }

    @Override
    public org.jooq.Name historyTableName(final ReferableSchema schema) {

        return combineNames(DSL.name(historySchemaName), DSL.name(name(schema)));
    }

    @Override
    public org.jooq.Name indexTableName(final ReferableSchema schema, final Index index) {

        final String name = name(schema) + Reserved.PREFIX + index.getName();
        return combineNames(DSL.name(objectSchemaName), DSL.name(name));
    }

    private String name(final Schema<?> schema) {

        final Casing casing = Nullsafe.orDefault(entityCasing, Casing.AS_SPECIFIED);
        return schema.getQualifiedName().stream().map(casing::name).collect(Collectors.joining("_"));
    }

    private org.jooq.Name customizeName(final Schema<?> schema, final Supplier<Name> defaultName) {

        return schema.getOptionalExtension(String.class, CUSTOM_TABLE_NAME_EXTENSION).map(SQLUtils::parseName).orElseGet(defaultName);
    }

    private org.jooq.Name combineNames(final org.jooq.Name schemaName, final org.jooq.Name name) {

        if (name.getName().length == 1) {
            return DSL.name(schemaName, name);
        } else {
            return name;
        }
    }

    public Casing getColumnCasing() {
        return Nullsafe.orDefault(columnCasing, Casing.AS_SPECIFIED);
    }


    public Casing getEntityCasing() {
        return Nullsafe.orDefault(entityCasing, Casing.AS_SPECIFIED);
    }
}
