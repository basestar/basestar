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
import io.basestar.schema.*;
import io.basestar.storage.util.Pager;
import io.basestar.util.Nullsafe;
import io.basestar.util.Sort;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

// Used in batch to allow pending creates/updates to be linked and referenced in permission expressions.
// This storage will respond as if the provided items exist in the underlying storage.

// FIXME: doesn't handle deletes

public class OverlayStorage implements Storage {

    private static final String TOMBSTONE_KEY = Reserved.PREFIX + "overlay";

    private final Storage baseline;

    private final Storage overlay;

    @lombok.Builder(builderClassName = "Builder")
    OverlayStorage(final Storage baseline, final Storage overlay) {

        this.baseline = baseline;
        this.overlay = overlay;
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        final CompletableFuture<Map<String, Object>> futBase = baseline.readObject(schema, id);
        final CompletableFuture<Map<String, Object>> futOver = overlay.readObject(schema, id);
        return CompletableFuture.allOf(futBase, futOver).thenApply(ignored -> {
            final Map<String, Object> over = futOver.getNow(null);
            return over != null ? over : futBase.getNow(null);
        });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        final CompletableFuture<Map<String, Object>> futBase = baseline.readObjectVersion(schema, id, version);
        final CompletableFuture<Map<String, Object>> futOver = overlay.readObjectVersion(schema, id, version);
        final CompletableFuture<Map<String, Object>> futCheck = overlay.readObject(schema, id);
        return CompletableFuture.allOf(futBase, futOver, futCheck).thenApply(ignored -> {
            final Map<String, Object> over = futOver.getNow(null);
            if (over != null) {
                return over;
            } else {
                // Cannot return a version greater than the latest in the overlay
                final Map<String, Object> check = futCheck.getNow(null);
                if(check != null) {
                    final Long checkVersion = Instance.getVersion(check);
                    if(checkVersion != null && checkVersion > version) {
                        return null;
                    }
                }
                return futBase.getNow(null);
            }
        });
    }

    @Override
//    @SuppressWarnings({"unchecked", "rawtypes"})
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        final List<Pager.Source<Map<String, Object>>> sources = new ArrayList<>();

        // FIXME: relies on stable impl of Stream.min() in Pager
        // Overlay entries appear first, so they will be chosen
        sources.addAll(overlay.query(schema, query, sort));
        sources.addAll(baseline.query(schema, query, sort));

//        baseline.query(schema, query, sort).forEach(source -> sources.add((count, token) -> source.page(count, token)
//                .thenApply(page -> page.filter(instance -> get(instance) == null))));

        // Add a source that will emit matching results

//        sources.add((count, token) -> {
//            // Don't do any paging, just return all matches
//            final List<Map<String, Object>> page = new ArrayList<>();
//            Nullsafe.option(data.get(schema.getName())).forEach((id, item) -> {
//                if(query.evaluatePredicate(Context.init(item))) {
//                    page.add(item);
//                }
//            });
//
//            // Must be sorted
//            final Comparator<Map<String, Object>> comparator = Sort.comparator(sort, (t, path) -> (Comparable)path.apply(t));
//            page.sort(comparator);
//
//            return CompletableFuture.completedFuture(new PagedList<>(page, null));
//        });

        return sources;
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
//        final ReadTransaction fromBaseline = baseline.read(consistency);
//        final ReadTransaction fromOverlay = overlay.read(consistency);
//        return new ReadTransaction() {
//
//            private final Map<BatchResponse.Key, Map<String, Object>> results = new HashMap<>();
//
//            @Override
//            public ReadTransaction readObject(final ObjectSchema schema, final String id) {
//
//                final Map<String, Object> object = get(schema.getName(), id);
//                if(object != null) {
//                    results.put(new BatchResponse.Key(schema.getName(), id), object);
//                    return this;
//                } else {
//                    transaction.readObject(schema, id);
//                    return this;
//                }
//            }
//
//            @Override
//            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {
//
//                final Map<String, Object> object = get(schema.getName(), id);
//                if(object != null && Long.valueOf(version).equals(Instance.getVersion(object))) {
//                    results.put(new BatchResponse.Key(schema.getName(), id, version), object);
//                    return this;
//                } else {
//                    transaction.readObjectVersion(schema, id, version);
//                    return this;
//                }
//            }
//
//            @Override
//            public CompletableFuture<BatchResponse> read() {
//
//                return transaction.read().thenApply(response -> {
//                    final Map<BatchResponse.Key, Map<String, Object>> merged = new HashMap<>(response);
//                    merged.putAll(results);
//                    return new BatchResponse.Basic(merged);
//                });
//            }
//        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        final WriteTransaction overlayWrite = overlay.write(consistency);
        return new WriteTransaction() {
            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                overlayWrite.createObject(schema, id, after);
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.updateObject(schema, id, before, after);
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                // FIXME: need to know if overlay has the value already for correct versioning
                final Map<String, Object> tombstone = tombstone(schema, id, before);
                overlayWrite.updateObject(schema, id, before, tombstone);
                return this;
            }
            
            private Map<String, Object> tombstone(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final Map<String, Object> tombstone = new HashMap<>();
                Instance.setSchema(tombstone, schema.getQualifiedName());
                Instance.setId(tombstone, id);
                Instance.setCreated(tombstone, Instance.getCreated(before));
                Instance.setUpdated(tombstone, LocalDateTime.now());
                Instance.setVersion(tombstone, Nullsafe.option(Instance.getVersion(before)) + 1);
                Instance.setHash(tombstone, schema.hash(tombstone));
                tombstone.put(TOMBSTONE_KEY, true);
                return tombstone;
            }

            @Override
            public WriteTransaction createIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.createIndex(schema, index, id, version, key, projection);
                return this;
            }

            @Override
            public WriteTransaction updateIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.updateIndex(schema, index, id, version, key, projection);
                return this;
            }

            @Override
            public WriteTransaction deleteIndex(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.deleteIndex(schema, index, id, version, key);
                return this;
            }

            @Override
            public WriteTransaction createHistory(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.createHistory(schema, id, version, after);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> commit() {

                return overlayWrite.commit();
            }
        };
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return overlay.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return overlay.storageTraits(schema);
    }
}
