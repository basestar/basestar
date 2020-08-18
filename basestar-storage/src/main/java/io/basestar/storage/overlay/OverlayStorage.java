package io.basestar.storage.overlay;

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

import com.google.common.collect.Sets;
import io.basestar.expression.Expression;
import io.basestar.expression.aggregate.Aggregate;
import io.basestar.schema.*;
import io.basestar.storage.*;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import io.basestar.util.Pager;
import io.basestar.util.Sort;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id, final Set<Name> expand) {

        final CompletableFuture<Map<String, Object>> futBase = baseline.readObject(schema, id, expand);
        final CompletableFuture<Map<String, Object>> futOver = overlay.readObject(schema, id, expand);
        return CompletableFuture.allOf(futBase, futOver).thenApply(ignored -> {
            final Map<String, Object> over = futOver.getNow(null);
            final Map<String, Object> base = futBase.getNow(null);
            return OverlayMetadata.wrap(base, over).applyTo(over != null ? over : base);
        });
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version, final Set<Name> expand) {

        final CompletableFuture<Map<String, Object>> futBase = baseline.readObjectVersion(schema, id, version, expand);
        final CompletableFuture<Map<String, Object>> futOver = overlay.readObjectVersion(schema, id, version, expand);
        final CompletableFuture<Map<String, Object>> futCheck = overlay.readObject(schema, id, expand);
        return CompletableFuture.allOf(futBase, futOver, futCheck).thenApply(ignored -> {
            final Map<String, Object> over = futOver.getNow(null);
            final Map<String, Object> base = futOver.getNow(null);
            final OverlayMetadata meta = OverlayMetadata.wrap(base, over);
            if (over != null) {
                return meta.applyTo(over);
            } else {
                // Cannot return a version greater than the latest in the overlay
                final Map<String, Object> check = futCheck.getNow(null);
                if(check != null) {
                    final Long checkVersion = Instance.getVersion(check);
                    if(checkVersion != null && checkVersion > version) {
                        return null;
                    }
                }
                return meta.applyTo(base);
            }
        });
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final List<Pager.Source<Map<String, Object>>> sources = new ArrayList<>();

        // FIXME: relies on stable impl of Stream.min() in Pager
        // Overlay entries appear first, so they will be chosen
        sources.addAll(Pager.map(overlay.query(schema, query, sort, expand), v -> OverlayMetadata.wrap(null, v).applyTo(v)));
        sources.addAll(Pager.map(baseline.query(schema, query, sort, expand), v -> OverlayMetadata.wrap(v, null).applyTo(v)));

        return sources;
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction.Basic(this);
    }

    @Override
    public CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

        // FIXME: need to know if overlay has the value already for correct versioning
        return overlay.asyncIndexCreated(schema, index, id, version, key, projection);
    }

    @Override
    public CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

        // FIXME: need to know if overlay has the value already for correct versioning
        return overlay.asyncIndexUpdated(schema, index, id, version, key, projection);
    }

    @Override
    public CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

        // FIXME: need to know if overlay has the value already for correct versioning
        return overlay.asyncIndexDeleted(schema, index, id, version, key);
    }

    @Override
    public CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

        // FIXME: need to know if overlay has the value already for correct versioning
        return overlay.asyncHistoryCreated(schema, id, version, after);
    }

    @Override
    public List<Pager.Source<RepairInfo>> repair(final ObjectSchema schema) {

        return Stream.of(baseline, overlay).flatMap(v -> v.repair(schema).stream())
                .collect(Collectors.toList());
    }

    @Override
    public List<Pager.Source<RepairInfo>> repairIndex(final ObjectSchema schema, final Index index) {

        return Stream.of(baseline, overlay).flatMap(v -> v.repairIndex(schema, index).stream())
                .collect(Collectors.toList());
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final WriteTransaction overlayWrite = overlay.write(consistency, versioning);
        return new WriteTransaction() {
            @Override
            public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                overlayWrite.createObject(schema, id, OverlayMetadata.unwrapOverlay(after));
                return this;
            }

            @Override
            public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                // FIXME: need to know if overlay has the value already for correct versioning
                overlayWrite.updateObject(schema, id, OverlayMetadata.unwrapOverlay(before), OverlayMetadata.unwrapOverlay(after));
                return this;
            }

            @Override
            public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                // FIXME: need to know if overlay has the value already for correct versioning
                final Map<String, Object> tombstone = tombstone(schema, id, before);
                overlayWrite.updateObject(schema, id, OverlayMetadata.unwrapOverlay(before), tombstone);
                return this;
            }
            
            private Map<String, Object> tombstone(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final Map<String, Object> tombstone = new HashMap<>();
                Instance.setSchema(tombstone, schema.getQualifiedName());
                Instance.setId(tombstone, id);
                Instance.setCreated(tombstone, before == null ? null : Instance.getCreated(before));
                Instance.setUpdated(tombstone, Instant.now());
                Instance.setVersion(tombstone, before == null ? null : Nullsafe.orDefault(Instance.getVersion(before)) + 1);
                Instance.setHash(tombstone, schema.hash(tombstone));
                tombstone.put(TOMBSTONE_KEY, true);
                return tombstone;
            }

            @Override
            public CompletableFuture<BatchResponse> write() {

                return overlayWrite.write();
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

    @Override
    public Set<Name> supportedExpand(final ObjectSchema schema, final Set<Name> expand) {

        return Sets.intersection(baseline.supportedExpand(schema, expand), overlay.supportedExpand(schema, expand));
    }
}
