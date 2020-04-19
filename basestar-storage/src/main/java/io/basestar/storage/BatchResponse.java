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

import com.google.common.collect.ImmutableSortedMap;
import io.basestar.schema.Instance;
import io.basestar.schema.ObjectSchema;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface BatchResponse extends Map<BatchResponse.Key, Map<String, Object>> {

    default Map<String, Object> getObject(final ObjectSchema schema, final String id) {

        return getObject(schema.getName(), id);
    }

    Map<String, Object> getObject(String schema, String id);

    default Map<String, Object> getObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return getObjectVersion(schema.getName(), id, version);
    }

    Map<String, Object> getObjectVersion(String schema, String id, long version);

    static BatchResponse empty() {

        return Basic.EMPTY;
    }

    static BatchResponse single(final String schema, final Map<String, Object> object) {

        return object == null ? empty() : new Basic(Key.from(schema, object), object);
    }

    static BatchResponse merge(final Stream<? extends BatchResponse> responses) {

        final SortedMap<Key, Map<String, Object>> all = new TreeMap<>();
        responses.forEach(all::putAll);
        return new Basic(all);
    }

    static CompletableFuture<BatchResponse> mergeFutures(final Stream<? extends CompletableFuture<? extends BatchResponse>> responses) {

        final List<CompletableFuture<? extends BatchResponse>> futures = responses.collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> BatchResponse.merge(futures.stream()
                        .map(v -> v.getNow(null))
                        .map(v -> v == null ? BatchResponse.empty() : v)));
    }

    class Basic extends AbstractMap<Key, Map<String, Object>> implements BatchResponse {

        private static final Basic EMPTY = new Basic();

        private final NavigableMap<Key, Map<String, Object>> items;

        public Basic() {

            this.items = ImmutableSortedMap.of();
        }

        @Override
        public Set<Entry<Key, Map<String, Object>>> entrySet() {

            return items.entrySet();
        }

        public Basic(final Key key, final Map<String, Object> object) {

            this.items = ImmutableSortedMap.of(key, object);
        }

        public Basic(final Map<Key, Map<String, Object>> items) {

            this.items = ImmutableSortedMap.copyOf(items);
        }

        public Basic(final SortedMap<Key, Map<String, Object>> items) {

            this.items = ImmutableSortedMap.copyOfSorted(items);
        }

        @Override
        public Map<String, Object> getObject(final String schema, final String id) {

            final Map.Entry<Key, Map<String, Object>> entry = items.floorEntry(new Key(schema, id, null));
            return entry == null ? null : entry.getValue();
        }

        @Override
        public Map<String, Object> getObjectVersion(final String schema, final String id, final long version) {

            return items.get(new Key(schema, id, version));
        }
    }

    @Data
    @RequiredArgsConstructor
    class Key implements Comparable<Key>, Serializable {

        private final String schema;

        private final String id;

        private final Long version;

        public Key(final String schema, final String id) {

            this(schema, id, null);
        }

        public static Key from(final String schema, final Map<String, Object> object) {

            final String id = Instance.getId(object);
            final Long version = Instance.getVersion(object);
            return new Key(schema, id, version);
        }

        @Override
        public int compareTo(final Key o) {

            return COMPARATOR.compare(this, o);
        }

        private static final Comparator<Key> COMPARATOR = Comparator.comparing(Key::getSchema)
                .thenComparing(Key::getId)
                .thenComparing(Comparator.comparing(Key::getVersion, Comparator.nullsFirst(Comparator.naturalOrder())).reversed());
    }
}
