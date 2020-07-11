package io.basestar.storage;

/*-
 * #%L
 * basestar-storage
 * %%
 * Copyright (C) 2019 - 2020 Basestar.IO
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
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
        if(storage.isEmpty()) {
            throw new IllegalStateException("Query fallback storage must have at least one storage engine");
        }
    }

    @Override
    public Storage storage(final ObjectSchema schema) {

        return storage.get(0);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return tryQuery(storage, schema, query, sort);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    public List<Pager.Source<Map<String, Object>>> tryQuery(final List<Storage> storage, final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        if (storage.isEmpty()) {
            throw new UnsupportedQueryException(schema.getQualifiedName(), query);
        } else {
            try {
                return storage.get(0).query(schema, query, sort);
            } catch (final UnsupportedQueryException e) {
                return tryQuery(storage.subList(1, storage.size()), schema, query, sort);
            }
        }
    }
}
