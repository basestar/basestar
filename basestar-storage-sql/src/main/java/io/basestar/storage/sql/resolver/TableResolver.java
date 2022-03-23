package io.basestar.storage.sql.resolver;

import io.basestar.schema.Index;
import io.basestar.schema.Schema;
import org.jooq.DSLContext;

import java.util.Map;
import java.util.Optional;

public interface TableResolver {

    Optional<ResolvedTable> table(DSLContext context, Schema schema, Map<String, Object> arguments, Index index, boolean versioned);

    default ResolvedTable requireTable(final DSLContext context, final Schema schema, final Map<String, Object> arguments, final Index index, final boolean versioned) {

        return table(context, schema, arguments, index, versioned).orElseThrow(() -> new IllegalStateException("No table for " + schema.getQualifiedName()));
    }
}
