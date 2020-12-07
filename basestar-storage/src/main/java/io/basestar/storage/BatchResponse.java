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

import com.google.common.collect.ImmutableMap;
import io.basestar.schema.Instance;
import io.basestar.schema.ReferableSchema;
import io.basestar.util.CompletableFutures;
import io.basestar.util.Immutable;
import io.basestar.util.Name;
import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
public class BatchResponse {

    private final NavigableMap<RefKey, Map<String, Object>> refs;

    public BatchResponse() {

        this(Collections.emptyNavigableMap());
    }
    
    public BatchResponse(final Map<RefKey, Map<String, Object>> refs) {

        this.refs = Immutable.navigableCopy(refs.entrySet().stream().filter(v -> v.getValue() != null).collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue
        )));
    }

    public static BatchResponse fromRef(final Name schema, final Map<String, Object> ref) {

        if(ref == null) {
            return BatchResponse.empty();
        } else {
            return new BatchResponse(ImmutableMap.of(RefKey.from(schema, ref), ref));
        }
    }

    public static BatchResponse fromRefs(final Map<RefKey, Map<String, Object>> refs) {

        return new BatchResponse(refs);
    }

    public Map<String, Object> get(final Name schema, final String id) {

        final Map.Entry<RefKey, Map<String, Object>> entry = refs.floorEntry(new RefKey(schema, id, null));
        if (entry != null && (schema.equals(entry.getKey().getSchema()) && id.equals(entry.getKey().getId()))) {
            return entry.getValue();
        } else {
            return null;
        }
    }

    public Map<String, Object> get(final RefKey key) {

        return refs.get(key);
    }

    public Map<String, Object> getVersion(final Name schema, final String id, final long version) {

        return refs.get(new RefKey(schema, id, version));
    }

    public BatchResponse with(final Name schema, final Map<String, Object> ref) {

        return new BatchResponse(
                Immutable.copyPutAll(refs, ImmutableMap.of(RefKey.from(schema, ref), ref))
        );
    }

    public Map<String, Object> get(final ReferableSchema schema, final String id) {

        return get(schema.getQualifiedName(), id);
    }

    public Map<String, Object> getVersion(final ReferableSchema schema, final String id, final long version) {

        return getVersion(schema.getQualifiedName(), id, version);
    }

    public BatchResponse with(final ReferableSchema schema, final Map<String, Object> ref) {

        return with(schema.getQualifiedName(), ref);
    }

    public static BatchResponse empty() {

        return new BatchResponse();
    }

    public static BatchResponse merge(final Stream<? extends BatchResponse> responses) {

        final Map<RefKey, Map<String, Object>> refs = new HashMap<>();
        responses.forEach(response -> {
            refs.putAll(response.getRefs());
        });
        return new BatchResponse(refs);
    }

    public static CompletableFuture<BatchResponse> mergeFutures(final Stream<CompletableFuture<BatchResponse>> futures) {

        return CompletableFutures.allOf(futures).thenApply(BatchResponse::merge);
    }

    @Data
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class RefKey implements Comparable<RefKey>, Serializable {

        private final Name schema;

        private final String id;

        private final Long version;

        public boolean hasVersion() {
            
            return version != null;
        }

        public static RefKey latest(final Name schema, final String id) {

            return new RefKey(schema, id, null);
        }

        public static RefKey version(final Name schema, final String id, final Long version) {

            return new RefKey(schema, id, version);
        }

        public static RefKey from(final Name schema, final Map<String, Object> object) {

            final String id = Instance.getId(object);
            final Long version = Instance.getVersion(object);
            return new RefKey(schema, id, version);
        }

        @Override
        public int compareTo(final RefKey o) {

            return COMPARATOR.compare(this, o);
        }

        private static final Comparator<RefKey> COMPARATOR = Comparator.comparing(RefKey::getSchema)
                .thenComparing(RefKey::getId)
                .thenComparing(Comparator.comparing(RefKey::getVersion, Comparator.nullsFirst(Comparator.naturalOrder())).reversed());

        public Map<String, Object> matchOrNull(final Map<String, Object> object) {

            if(object == null) {
                return null;
            } else if(!id.equals(Instance.getId(object))) {
                return null;
            } else if(hasVersion() && !version.equals(Instance.getVersion(object))) {
                return null;
            } else {
                return object;
            }
        }

        public RefKey withVersion(final Long version) {

            return new RefKey(schema, id, version);
        }

        public RefKey withoutVersion() {

            return withVersion(null);
        }
    }
}