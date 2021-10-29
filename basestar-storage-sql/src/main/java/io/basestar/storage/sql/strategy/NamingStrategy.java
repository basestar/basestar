package io.basestar.storage.sql.strategy;

import io.basestar.schema.FunctionSchema;
import io.basestar.schema.ReferableSchema;
import io.basestar.schema.Schema;
import io.basestar.schema.ViewSchema;
import io.basestar.schema.util.Casing;

public interface NamingStrategy {

    org.jooq.Name objectTableName(ReferableSchema schema);

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

    String name(Schema<?> schema);

    Casing getColumnCasing();

    Casing getEntityCasing();

    String getDelimiter();
}
