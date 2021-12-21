package io.basestar.schema.use;

import io.basestar.schema.LinkableSchema;
import io.basestar.schema.QuerySchema;
import io.basestar.schema.QueryableSchema;

public interface UseQueryable extends UseInstance {

    static UseQueryable from(final QueryableSchema schema, final Object config) {

        if (schema instanceof QuerySchema) {
            return UseQuery.from((QuerySchema) schema, config);
        } else if (schema instanceof LinkableSchema) {
            return UseLinkable.from((LinkableSchema) schema, config);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    QueryableSchema getSchema();
}
