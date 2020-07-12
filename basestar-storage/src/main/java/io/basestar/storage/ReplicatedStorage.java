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
import io.basestar.schema.Consistency;
import io.basestar.schema.Index;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.util.Pager;
import io.basestar.util.Name;
import io.basestar.util.Sort;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * NOTE: this class will not perform replication, it just implements fallback reads on the primary:
 * you should use an io.basestar.database.Replicator and a suitable event pump to replicate data from
 * the primary to the replica.
 */

public class ReplicatedStorage implements Storage {

    private final Supplier<Storage> primary;

    private final Supplier<Storage> replica;

    private ReplicatedStorage(final Builder builder) {

        this.primary = builder.primary;
        this.replica = builder.replica;
    }

    public static Builder builder() {

        return new Builder();
    }

    @Setter
    @Accessors(chain = true)
    public static class Builder {

        private Supplier<Storage> primary;

        private Supplier<Storage> replica;

        public Builder setPrimary(final Storage primary) {

            this.primary = () -> primary;
            return this;
        }

        public Builder setReplica(final Storage replica) {

            this.replica = () -> replica;
            return this;
        }

        public ReplicatedStorage build() {

            return new ReplicatedStorage(this);
        }
    }

    protected Storage primary() {

        return primary.get();
    }

    protected Storage replica() {

        return replica.get();
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObject(final ObjectSchema schema, final String id) {

        return replica().readObject(schema, id)
                .thenCompose(result -> result == null ? primary().readObject(schema, id) : CompletableFuture.completedFuture(result));
    }

    @Override
    public CompletableFuture<Map<String, Object>> readObjectVersion(final ObjectSchema schema, final String id, final long version) {

        return replica().readObjectVersion(schema, id, version)
                .thenCompose(result -> result == null ? primary().readObjectVersion(schema, id, version) : CompletableFuture.completedFuture(result));
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> query(final ObjectSchema schema, final Expression query, final List<Sort> sort) {

        return replica().query(schema, query, sort);
    }

    @Override
    public List<Pager.Source<Map<String, Object>>> aggregate(final ObjectSchema schema, final Expression query, final Map<String, Expression> group, final Map<String, Aggregate> aggregates) {

        throw new UnsupportedOperationException();
    }

    @Override
    public WriteTransaction write(final Consistency consistency) {

        return primary().write(consistency);
    }

    @Override
    public EventStrategy eventStrategy(final ObjectSchema schema) {

        return primary().eventStrategy(schema);
    }

    @Override
    public StorageTraits storageTraits(final ObjectSchema schema) {

        return primary().storageTraits(schema);
    }

    @Override
    public CompletableFuture<?> asyncIndexCreated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

        return primary().asyncIndexCreated(schema, index, id, version, key, projection);
    }

    @Override
    public CompletableFuture<?> asyncIndexUpdated(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key, final Map<String, Object> projection) {

        return primary().asyncIndexUpdated(schema, index, id, version, key, projection);
    }

    @Override
    public CompletableFuture<?> asyncIndexDeleted(final ObjectSchema schema, final Index index, final String id, final long version, final Index.Key key) {

        return primary().asyncIndexDeleted(schema, index, id, version, key);
    }

    @Override
    public CompletableFuture<?> asyncHistoryCreated(final ObjectSchema schema, final String id, final long version, final Map<String, Object> after) {

        return primary().asyncHistoryCreated(schema, id, version, after);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        // Atomic reads cannot
        if(consistency == Consistency.ATOMIC) {
            return primary().read(consistency);
        }
        final ReadTransaction delegate = replica().read(consistency);
        return new ReadTransaction() {

            private final Set<BatchResponse.Key> keys = new HashSet<>();

            private final Map<Name, ObjectSchema> schemas = new HashMap<>();

            @Override
            public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                keys.add(BatchResponse.Key.latest(schema.getQualifiedName(), id));
                schemas.put(schema.getQualifiedName(), schema);
                delegate.readObject(schema, id);
                return this;
            }

            @Override
            public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                keys.add(BatchResponse.Key.version(schema.getQualifiedName(), id, version));
                schemas.put(schema.getQualifiedName(), schema);
                delegate.readObjectVersion(schema, id, version);
                return this;
            }

            @Override
            public CompletableFuture<BatchResponse> read() {

                return delegate.read()
                        .thenCompose(replicaResponse -> {

                            final Set<BatchResponse.Key> missing = new HashSet<>();
                            for(final BatchResponse.Key key : keys) {
                                if(!replicaResponse.containsKey(key)) {
                                    missing.add(key);
                                }
                            }
                            if(missing.isEmpty()) {
                                return CompletableFuture.completedFuture(replicaResponse);
                            } else {
                                final ReadTransaction next = primary().read(consistency);
                                keys.forEach(k -> {
                                    final ObjectSchema schema = schemas.get(k.getSchema());
                                    assert schema != null;
                                    if(k.getVersion() == null) {
                                        next.readObject(schema, k.getId());
                                    } else {
                                        next.readObjectVersion(schema, k.getId(), k.getVersion());
                                    }
                                });
                                return next.read().thenApply(primaryResponse ->
                                        BatchResponse.merge(Stream.of(primaryResponse, replicaResponse)));
                            }
                        });
            }
        };
    }
}
