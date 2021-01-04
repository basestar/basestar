package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.LinkableSchema;
import io.basestar.schema.Reserved;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class QueryDelegatingStorage implements DelegatingStorage {

    private final List<Storage> storage;

    public QueryDelegatingStorage(final List<Storage> storage) {

        this.storage = Immutable.list(storage);
        if(storage.isEmpty()) {
            throw new IllegalStateException("Query fallback storage must have at least one storage engine");
        }
    }

    @Override
    public Storage storage(final LinkableSchema schema) {

        return storage.get(0);
    }

    @Override
    public Pager<Map<String, Object>> query(final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        return tryQuery(0, schema, query, sort, expand);
    }

    protected Pager<Map<String, Object>> tryQuery(final int offset, final LinkableSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        if (offset >= storage.size()) {
            throw new UnsupportedQueryException(schema.getQualifiedName(), query);
        } else {
            try {
                final Pager<Map<String, Object>> result = storage.get(offset).query(schema, query, sort, expand);
                if(offset != 0) {
                    return result.map(v -> Instance.without(v, Reserved.META));
                } else {
                    return result;
                }
            } catch (final UnsupportedQueryException e) {
                return tryQuery(offset + 1, schema, query, sort, expand);
            }
        }
    }
}