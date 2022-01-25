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
import io.basestar.event.Event;
import io.basestar.expression.Expression;
import io.basestar.schema.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.Storage;
import io.basestar.storage.StorageTraits;
import io.basestar.storage.Versioning;
import io.basestar.util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

// Used in batch to allow pending creates/updates to be linked and referenced in permission expressions.
// This storage will respond as if the provided items exist in the underlying storage.

// FIXME: doesn't handle deletes

public class OverlayStorage implements Storage {

    private static final String OVERLAY_KEY = Reserved.PREFIX + "overlay";

    private static final String TOMBSTONE_KEY = Reserved.PREFIX + "deleted";

    private final Storage baseline;

    private final Storage overlay;

    @lombok.Builder(builderClassName = "Builder")
    OverlayStorage(final Storage baseline, final Storage overlay) {

        this.baseline = baseline;
        this.overlay = overlay;
    }

    @Override
    public Pager<Map<String, Object>> query(final Consistency consistency, final QueryableSchema schema, final Map<String, Object> arguments, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Map<String, Pager<Map<String, Object>>> pagers = new HashMap<>();
        // Overlay entries appear first, so they will be chosen
        pagers.put("o", overlay.query(consistency, schema, arguments, query, sort, expand).map(v -> Immutable.put(v, OVERLAY_KEY, 0)));
        pagers.put("b", baseline.query(consistency, schema, arguments, query, sort, expand).map(v -> Immutable.put(v, OVERLAY_KEY, 1)));

        return Pager.merge(Instance.comparator(sort)
                .thenComparing(v -> ((Number) v.get(OVERLAY_KEY)).intValue()), pagers);
    }

    @Override
    public Pager<Map<String, Object>> queryHistory(final Consistency consistency, final ReferableSchema schema, final String id, final Expression query, final List<Sort> sort, final Set<Name> expand) {

        final Map<String, Pager<Map<String, Object>>> pagers = new HashMap<>();
        // Overlay entries appear first, so they will be chosen
        pagers.put("o", overlay.queryHistory(consistency, schema, id, query, sort, expand).map(v -> Immutable.put(v, OVERLAY_KEY, 0)));
        pagers.put("b", baseline.queryHistory(consistency, schema, id, query, sort, expand).map(v -> Immutable.put(v, OVERLAY_KEY, 1)));

        return Pager.merge(Instance.comparator(sort)
                .thenComparing(v -> ((Number) v.get(OVERLAY_KEY)).intValue()), pagers);
    }

    @Override
    public void validate(final ObjectSchema schema) {

        overlay.validate(schema);
    }

    @Override
    public CompletableFuture<Set<Event>> afterCreate(final ObjectSchema schema, final String id, final Map<String, Object> after) {

        return overlay.afterCreate(schema, id, after);
    }

    @Override
    public CompletableFuture<Set<Event>> afterUpdate(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before, final Map<String, Object> after) {

        return overlay.afterUpdate(schema, id, version, before, after);
    }

    @Override
    public CompletableFuture<Set<Event>> afterDelete(final ObjectSchema schema, final String id, final long version, final Map<String, Object> before) {

        return overlay.afterDelete(schema, id, version, before);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        return new ReadTransaction() {

            ReadTransaction baselineTransaction = baseline.read(consistency);
            ReadTransaction overlayTransaction = overlay.read(consistency);

            @Override
            public ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

                baselineTransaction = baselineTransaction.get(schema, id, expand);
                overlayTransaction = overlayTransaction.get(schema, id, expand);
                return this;
            }

            @Override
            public ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

                baselineTransaction = baselineTransaction.getVersion(schema, id, version, expand);
                overlayTransaction = overlayTransaction.getVersion(schema, id, version, expand);
                // See: ref()
                overlayTransaction = overlayTransaction.get(schema, id, expand);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                final CompletableFuture<BatchResponse> baselineFuture = baselineTransaction.read();
                final CompletableFuture<BatchResponse> overlayFuture = overlayTransaction.read();

                return baselineFuture.thenCombine(overlayFuture, (baselineResponse, overlayResponse) -> {

                    final Map<BatchResponse.RefKey, Map<String, Object>> refs = new HashMap<>();
                    Stream.of(baselineResponse, overlayResponse)
                            .flatMap(v -> v.getRefs().keySet().stream()).forEach(key -> {
                        refs.put(key, ref(baselineResponse, overlayResponse, key));
                    });
                    return new BatchResponse(refs);
                });
            }

            private Map<String, Object> ref(final BatchResponse baselineResponse, final BatchResponse overlayResponse, final BatchResponse.RefKey key) {

                final Map<String, Object> over = overlayResponse.get(key);
                final Map<String, Object> base = baselineResponse.get(key);
                if(over != null && over.containsKey(TOMBSTONE_KEY)) {
                    return null;
                }
                if(key.hasVersion()) {
                    final OverlayMetadata meta = OverlayMetadata.wrap(base, over);
                    if (over != null) {
                        return meta.applyTo(over);
                    } else {
                        // Cannot return a version greater than the latest in the overlay
                        final Map<String, Object> check = overlayResponse.get(key.withoutVersion());
                        if(check != null) {
                            final Long checkVersion = Instance.getVersion(check);
                            if(checkVersion != null && checkVersion > key.getVersion()) {
                                return null;
                            }
                        }
                        return meta.applyTo(base);
                    }
                } else {
                    return OverlayMetadata.wrap(base, over).applyTo(over != null ? over : base);
                }
            }
        };
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final WriteTransaction overlayWrite = overlay.write(consistency, versioning);
        return new WriteTransaction() {

            @Override
            public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                overlayWrite.write(schema, OverlayMetadata.unwrapOverlay(after));
                return this;
            }

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

            @Override
            public WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                overlayWrite.writeHistory(schema, id, OverlayMetadata.unwrapOverlay(after));
                return this;
            }

            private Map<String, Object> tombstone(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                final Map<String, Object> tombstone = new HashMap<>();
                Instance.setSchema(tombstone, schema.getQualifiedName());
                Instance.setId(tombstone, id);
                Instance.setCreated(tombstone, before == null ? null : Instance.getCreated(before));
                Instance.setUpdated(tombstone, ISO8601.now());
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
    public EventStrategy eventStrategy(final ReferableSchema schema) {

        return overlay.eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ReferableSchema schema) {

        return overlay.storageTraits(schema);
    }

    @Override
    public Set<Name> supportedExpand(final LinkableSchema schema, final Set<Name> expand) {

        return Sets.intersection(baseline.supportedExpand(schema, expand), overlay.supportedExpand(schema, expand));
    }
}
