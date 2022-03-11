package io.basestar.storage.sql.resolver;

import io.basestar.schema.Index;
import io.basestar.schema.QuerySchema;
import io.basestar.schema.Schema;
import org.jooq.DSLContext;

import java.util.Map;
import java.util.Optional;

public abstract class QueryTableResolver implements TableResolver {

    private final TableResolver delegate;

    public QueryTableResolver(final TableResolver delegate) {

        this.delegate = delegate;
    }

    @Override
    public Optional<ResolvedTable> table(final DSLContext context, final Schema schema, final Map<String, Object> arguments, final Index index, final boolean versioned) {

        if (schema instanceof QuerySchema) {
            return query(context, (QuerySchema) schema, arguments);
        } else {
            return delegate.table(context, schema, arguments, index, versioned);
        }
    }

    protected abstract Optional<ResolvedTable> query(DSLContext context, QuerySchema schema, Map<String, Object> arguments);
}
