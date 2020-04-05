package io.basestar.storage;

import io.basestar.expression.Expression;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.exception.UnsupportedQueryException;
import io.basestar.storage.util.Pager;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;

import java.util.List;
import java.util.Map;

public class QueryFallbackStorage implements DelegatingStorage {

    private final List<Storage> storage;

    public QueryFallbackStorage(final List<Storage> storage) {

        this.storage = Nullsafe.immutableCopy(storage);
    }

    @Override
    public Storage storage(final ObjectSchema schema) {

        return storage.get(0);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return tryQuery(storage, schema, query, sort);
    }

    public List<Pager.Source<Map<String, Object>>> tryQuery(final List<Storage> storage, final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        if (storage.isEmpty()) {
            throw new UnsupportedQueryException(schema.getName(), query);
        } else {
            try {
                return storage.get(0).query(schema, query, sort);
            } catch (final UnsupportedQueryException e) {
                return tryQuery(storage.subList(1, storage.size()), schema, query, sort);
            }
        }
    }
}