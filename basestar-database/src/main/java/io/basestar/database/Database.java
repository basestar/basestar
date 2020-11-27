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
import io.basestar.util.Name;
import io.basestar.util.Page;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface Database {

    Namespace namespace();

    CompletableFuture<Instance> read(Caller caller, ReadOptions options);

    default CompletableFuture<Instance> read(final Caller caller, final Name schema, final String id) {

        return read(caller, schema, id, null);
    }

    default CompletableFuture<Instance> read(final Caller caller, final Name schema, final String id, final Long version) {

        return read(caller, ReadOptions.builder().schema(schema).id(id).version(version).build());
    }

    CompletableFuture<Instance> create(Caller caller, CreateOptions options);

    default CompletableFuture<Instance> create(final Caller caller, final Name schema, final Map<String, Object> data) {

        return create(caller, schema, null, data);
    }

    default CompletableFuture<Instance> create(final Caller caller, final Name schema, final String id, final Map<String, Object> data) {

        return create(caller, CreateOptions.builder().schema(schema).id(id).data(data).build());
    }

    CompletableFuture<Instance> update(Caller caller, UpdateOptions options);

    default CompletableFuture<Instance> update(final Caller caller, final Name schema, final String id, final Map<String, Object> data) {

        return update(caller, schema, id, null, data);
    }

    default CompletableFuture<Instance> update(final Caller caller, final Name schema, final String id, final Long version, final Map<String, Object> data) {

        return update(caller, UpdateOptions.builder().schema(schema).id(id).version(version).data(data).build());
    }

    CompletableFuture<Instance> delete(Caller caller, DeleteOptions options);

    default CompletableFuture<Instance> delete(final Caller caller, final Name schema, final String id) {

        return delete(caller, schema, id, null);
    }

    default CompletableFuture<Instance> delete(final Caller caller, final Name schema, final String id, final Long version) {

        return delete(caller, DeleteOptions.builder().schema(schema).id(id).version(version).build());
    }

    CompletableFuture<Page<Instance>> query(Caller caller, QueryOptions options);

    default CompletableFuture<Page<Instance>> query(final Caller caller, final Name schema, final Expression expression) {

        return query(caller, QueryOptions.builder().schema(schema).expression(expression).build());
    }

    CompletableFuture<Page<Instance>> queryLink(Caller caller, QueryLinkOptions options);

    default CompletableFuture<Page<Instance>> queryLink(final Caller caller, final Name schema, final String id, final String link) {

        return queryLink(caller, QueryLinkOptions.builder().schema(schema).id(id).link(link).build());
    }

    CompletableFuture<Map<String, Instance>> batch(Caller caller, BatchOptions options);

    default CompletableFuture<Instance> expand(Caller caller, Map<String, Object> instance, Set<Name> expand) {

        // FIXME: should not read the root object (will break in versioning scenario anyway)
        return read(caller, ReadOptions.builder()
                .id(Instance.getId(instance))
                .schema(Instance.getSchema(instance))
                .version(Instance.getVersion(instance))
                .expand(expand)
                .build());
    }

    CompletableFuture<?> repair(Caller caller, RepairOptions options);
}
