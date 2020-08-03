package io.basestar.database;

/*-
 * #%L
 * basestar-database-client
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

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import io.basestar.auth.Caller;
import io.basestar.database.options.*;
import io.basestar.database.transport.Transport;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.util.Name;
import io.basestar.util.PagedList;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class DatabaseClient implements Database {

    private final String baseUrl;

    private final Transport transport;

    @Override
    public Namespace namespace() {
        return null;
    }

    @Override
    public CompletableFuture<Instance> read(final Caller caller, final ReadOptions options) {

        final Name schema = options.getSchema();
        final String id = options.getId();
        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return transport.get(objectUrl(schema, id), query, headers(caller), instanceReader());
    }

    @Override
    public CompletableFuture<Instance> create(final Caller caller, final CreateOptions options) {

        final Name schema = options.getSchema();
        final String id = options.getId();
        final Map<String, Object> data = options.getData();
        final Multimap<String, String> query = HashMultimap.create();
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return null;
    }

    @Override
    public CompletableFuture<Instance> update(final Caller caller, final UpdateOptions options) {

        final Name schema = options.getSchema();
        final String id = options.getId();
        final Map<String, Object> data = options.getData();
        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return null;
    }

    @Override
    public CompletableFuture<Instance> delete(final Caller caller, final DeleteOptions options) {

        final Name schema = options.getSchema();
        final String id = options.getId();
        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());

        return transport.delete(objectUrl(schema, id), query, headers(caller), instanceReader());
    }

    @Override
    public CompletableFuture<PagedList<Instance>> query(final Caller caller, final QueryOptions options) {

        return null;
    }

    @Override
    public CompletableFuture<PagedList<Instance>> queryLink(final Caller caller, final QueryLinkOptions options) {

        return null;
    }

    @Override
    public CompletableFuture<Map<String, Instance>> transaction(final Caller caller, final BatchOptions options) {

        return null;
    }

    private void addVersion(final Multimap<String, String> query, final Long version) {

        if(version != null) {
            query.put("version", Long.toString(version));
        }
    }

    private void addExpand(final Multimap<String, String> query, final Set<Name> expand) {

        if(expand != null) {
            query.put("expand", Joiner.on(",").join(expand));
        }
    }

    private void addProjection(final Multimap<String, String> query, final Set<String> projection) {

        if(projection != null) {
            query.put("projection", Joiner.on(",").join(projection));
        }
    }

    private String schemaUrl(final Name schema) {


        return null;
    }

    private String objectUrl(final Name schema, final String id) {

        return null;
    }

    private Transport.BodyReader<Instance> instanceReader() {

        return null;
    }

    private Multimap<String, String> headers(final Caller caller) {

        return null;
    }

}
