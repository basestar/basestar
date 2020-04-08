package io.basestar.database;

/*-
 * #%L
 * basestar-database
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

import io.basestar.auth.Caller;
import io.basestar.database.options.*;
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface Database {

//    void register(Registry registry);

    Namespace namespace();

    default CompletableFuture<Instance> read(final Caller caller, final String schema, final String id) {

        return read(caller, schema, id, new ReadOptions());
    }

    CompletableFuture<Instance> read(Caller caller, String schema, String id, ReadOptions options);

    default CompletableFuture<Instance> create(final Caller caller, final String schema, final Map<String, Object> data) {

        return create(caller, schema, data, new CreateOptions());
    }

    default CompletableFuture<Instance> create(final Caller caller, final String schema, final Map<String, Object> data, final CreateOptions options) {

        return create(caller, schema, UUID.randomUUID().toString(), data, options);
    }

    default CompletableFuture<Instance> create(final Caller caller, final String schema, final String id, final Map<String, Object> data) {

        return create(caller, schema, id, data, new CreateOptions());
    }

    CompletableFuture<Instance> create(Caller caller, String schema, String id, Map<String, Object> data, CreateOptions options);

    default CompletableFuture<Instance> update(final Caller caller, final String schema, final String id, final Map<String, Object> data) {

        return update(caller, schema, id, data, new UpdateOptions());
    }

    CompletableFuture<Instance> update(Caller caller, String schema, String id, Map<String, Object> data, UpdateOptions options);

    default CompletableFuture<Boolean> delete(final Caller caller, final String schema, final String id) {

        return delete(caller, schema, id, new DeleteOptions());
    }

    CompletableFuture<Boolean> delete(Caller caller, String schema, String id, DeleteOptions options);

    default CompletableFuture<PagedList<Instance>> query(final Caller caller, final String schema, final Expression expression) {

        return query(caller, schema, expression, new QueryOptions());
    }

    CompletableFuture<PagedList<Instance>> query(Caller caller, String schema, Expression expression, QueryOptions options);

    default CompletableFuture<PagedList<Instance>> page(final Caller caller, final String schema, final String id, final String rel) {

        return page(caller, schema, id, rel, new QueryOptions());
    }

    CompletableFuture<PagedList<Instance>> page(Caller caller, String schema, String id, String rel, QueryOptions options);

    CompletableFuture<PagingToken> reindex(Caller caller, Map<String, ? extends Collection<String>> schemaIndexes, int count, PagingToken paging);
}
