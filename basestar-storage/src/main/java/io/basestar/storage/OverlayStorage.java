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

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.schema.Consistency;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import io.basestar.schema.aggregate.Aggregate;
import io.basestar.storage.util.Pager;
import io.basestar.util.Nullsafe;
import io.basestar.util.PagedList;
import io.basestar.util.Sort;

import java.util.*;
import java.util.concurrent.CompletableFuture;

// Used in batch to allow pending creates/updates to be linked and referenced in permission expressions.
// This storage will respond as if the provided items exist in the underlying storage.

// FIXME: doesn't handle deletes

public class OverlayStorage implements Storage {

    private final Storage delegate;

    private final Map<String, Map<String, Map<String, Object>>> data;

    public OverlayStorage(final Storage delegate, final Collection<? extends Map<String, Object>> items) {

        this.delegate = delegate;
        final Map<String, Map<String, Map<String, Object>>> data = new HashMap<>();
        items.forEach(item -> {
            final String schema = Instance.getSchema(item);
            final String id = Instance.getId(item);
            final Map<String, Map<String, Object>> target = data.computeIfAbsent(schema, ignored -> new HashMap<>());
            target.put(id, item);
        });
        this.data = Collections.unmodifiableMap(data);
    }

    private Map<String, Object> get(final String schema, final String id) {

        return Nullsafe.option(data.get(schema)).get(id);
    }

    private Map<String, Object> get(final Map<String, Object> instance) {

        return get(Instance.getSchema(instance), Instance.getId(instance));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final Map<String, Object> object = get(schema.getName(), id);
        if(object != null) {
            return CompletableFuture.completedFuture(object);
        } else {
            return delegate.readObject(schema, id);
        }
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        final Map<String, Object> object = get(schema.getName(), id);
        if(object != null && Long.valueOf(version).equals(Instance.getVersion(object))) {
            return CompletableFuture.completedFuture(object);
        } else {
            return delegate.readObjectVersion(schema, id, version);
        }
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        final List<Pager.Source<Map<String, Object>>> sources = new ArrayList<>();

        // Add a source that will emit matching results

        sources.add((count, token) -> {
            // Don't do any paging, just return all matches
            final List<Map<String, Object>> page = new ArrayList<>();
            Nullsafe.option(data.get(schema.getName())).forEach((id, item) -> {
                if(query.evaluatePredicate(Context.init(item))) {
                    page.add(item);
                }
            });

            // Must be sorted
            final Comparator<Map<String, Object>> comparator = Sort.comparator(sort, (t, path) -> (Comparable)path.apply(t));
            page.sort(comparator);

            return CompletableFuture.completedFuture(new PagedList<>(page, null));
        });

        // Add underlying sources, but filter out any matches

        delegate.query(schema, query, sort).forEach(source -> sources.add((count, token) -> source.page(count, token)
                            .thenApply(page -> page.filter(instance -> get(instance) == null))));

        return sources;
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        final ReadTransaction transaction = delegate.read(consistency);
        return new ReadTransaction() {

            private final Map<BatchResponse.Key, Map<String, Object>> results = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                final Map<String, Object> object = get(schema.getName(), id);
                if(object != null) {
                    results.put(new BatchResponse.Key(schema.getName(), id), object);
                    return this;
                } else {
                    transaction.readObject(schema, id);
                    return this;
                }
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                final Map<String, Object> object = get(schema.getName(), id);
                if(object != null && Long.valueOf(version).equals(Instance.getVersion(object))) {
                    results.put(new BatchResponse.Key(schema.getName(), id, version), object);
                    return this;
                } else {
                    transaction.readObjectVersion(schema, id, version);
                    return this;
                }
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return transaction.read().thenApply(response -> {
                    final Map<BatchResponse.Key, Map<String, Object>> merged = new HashMap<>(response);
                    merged.putAll(results);
                    return new BatchResponse.Basic(merged);
                });
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return delegate.write(consistency);
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return delegate.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return delegate.storageTraits(schema);
    }
}
