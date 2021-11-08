package io.basestar.storage.sql.strategy;

import io.basestar.schema.*;
import io.basestar.schema.util.Casing;
import org.jooq.Name;

import java.util.Optional;

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

    default org.jooq.Name reference(final Schema<?> schema) {
        return entityName(schema);
    }

    Casing getColumnCasing();

    Casing getEntityCasing();

    String getDelimiter();

    Optional<Name> historyTableName(ReferableSchema schema);

    Optional<Name> indexTableName(ReferableSchema schema, Index index);

    Optional<Name> getSchema(Schema<?> schema);

    Optional<Name> getCatalog(Schema<?> schema);
}
