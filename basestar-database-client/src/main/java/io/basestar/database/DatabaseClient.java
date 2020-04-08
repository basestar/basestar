package io.basestar.database;

/*-
 * #%L
 * basestar-database-client
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2020 basestar.io
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
import io.basestar.expression.Expression;
import io.basestar.schema.Instance;
import io.basestar.schema.Namespace;
import io.basestar.util.PagedList;
import io.basestar.util.PagingToken;
import io.basestar.util.Path;
import lombok.RequiredArgsConstructor;

import java.util.Collection;
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
    public CompletableFuture<Instance> read(final Caller caller, final String schema, final String id, final ReadOptions options) {

        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return transport.get(objectUrl(schema, id), query, headers(caller), instanceReader());
    }

    @Override
    public CompletableFuture<Instance> create(final Caller caller, final String schema, final String id, final Map<String, Object> data, final CreateOptions options) {

        final Multimap<String, String> query = HashMultimap.create();
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return null;
    }

    @Override
    public CompletableFuture<Instance> update(final Caller caller, final String schema, final String id, final Map<String, Object> data, final UpdateOptions options) {

        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());
        addExpand(query, options.getExpand());
//        addProjection(query, options.getProjection());

        return null;
    }

    @Override
    public CompletableFuture<Boolean> delete(final Caller caller, final String schema, final String id, final DeleteOptions options) {

        final Multimap<String, String> query = HashMultimap.create();
        addVersion(query, options.getVersion());

        return transport.delete(objectUrl(schema, id), query, headers(caller), booleanReader());
    }

    @Override
    public CompletableFuture<PagedList<Instance>> query(final Caller caller, final String schema, final Expression expression, final QueryOptions options) {

        return null;
    }

    @Override
    public CompletableFuture<PagedList<Instance>> page(final Caller caller, final String schema, final String id, final String rel, final QueryOptions options) {

        return null;
    }

    @Override
    public CompletableFuture<PagingToken> reindex(final Caller caller, final Map<String, ? extends Collection<String>> schemaIndexes, final int count, final PagingToken paging) {

        return null;
    }

    private void addVersion(final Multimap<String, String> query, final Long version) {

        if(version != null) {
            query.put("version", Long.toString(version));
        }
    }

    private void addExpand(final Multimap<String, String> query, final Set<Path> expand) {

        if(expand != null) {
            query.put("expand", Joiner.on(",").join(expand));
        }
    }

    private void addProjection(final Multimap<String, String> query, final Set<String> projection) {

        if(projection != null) {
            query.put("projection", Joiner.on(",").join(projection));
        }
    }

    private String schemaUrl(final String schema) {
        return null;
    }

    private String objectUrl(final String schema, final String id) {
        return null;
    }

    private Transport.BodyReader<Boolean> booleanReader() {

        return null;
    }

    private Transport.BodyReader<Instance> instanceReader() {

        return null;
    }

    private Multimap<String, String> headers(final Caller caller) {

        return null;
    }

}
