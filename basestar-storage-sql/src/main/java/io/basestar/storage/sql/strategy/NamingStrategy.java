package io.basestar.storage.sql.strategy;

import io.basestar.schema.*;
import io.basestar.schema.util.Casing;

public interface NamingStrategy {

    org.jooq.Name catalogName(ReferableSchema schema);

    org.jooq.Name objectTableName(ReferableSchema schema);

    org.jooq.Name historyTableName(ReferableSchema schema);

    org.jooq.Name functionName(FunctionSchema schema);

    org.jooq.Name viewName(ViewSchema schema);

    String columnName(String name);

    org.jooq.Name columnName(io.basestar.util.Name name);

    default org.jooq.Name entityName(final Schema<?> schema) {
        if (schema instanceof ReferableSchema) {
            return objectTableName((ReferableSchema) schema);
        } else if (schema instanceof ViewSchema) {
            return viewName((ViewSchema) schema);
        } else if (schema instanceof FunctionSchema) {
            return functionName((FunctionSchema) schema);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    // Only used for multi-value indexes
    org.jooq.Name indexTableName(ReferableSchema schema, Index index);

    Casing getColumnCasing();

    Casing getEntityCasing();

    String getObjectSchemaName();

    String getHistorySchemaName();
}
