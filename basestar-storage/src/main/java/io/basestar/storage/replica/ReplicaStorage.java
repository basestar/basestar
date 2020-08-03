package io.basestar.storage.replica;

import com.google.common.collect.Sets;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;
import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.schema.ObjectSchema;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DelegatingStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import io.basestar.storage.replica.event.ReplicaSyncEvent;
import io.basestar.util.Nullsafe;
import lombok.Builder;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

@Builder(builderClassName = "Builder", setterPrefix = "set")
public class ReplicaStorage implements DelegatingStorage, Handler<Event> {

    private static final Handlers<ReplicaStorage> HANDLERS = Handlers.<ReplicaStorage>builder()
            .on(ReplicaSyncEvent.class, ReplicaStorage::onSync)
            .build();

    @Override
    public CompletableFuture<?> handle(final Event event, final Map<String, String> metadata) {

        return HANDLERS.handle(this, event, metadata);
    }

    public static class Builder {

        public Builder setSimplePrimary(final Storage primary) {

            return setPrimary(schema -> primary);
        }

        public Builder setSimpleReplica(final Storage secondary) {

            return setReplica(schema -> Optional.ofNullable(secondary));
        }

        public Builder setSimpleReadConsistency(final Consistency consistency) {

            return setReadConsistency(ignored -> consistency);
        }

        public Builder setSimpleWriteConsistency(final Consistency consistency) {

            return setWriteConsistency(ignored -> consistency);
        }

        public Builder setSimplePrimaryConsistency(final Consistency consistency) {

            return setPrimaryConsistency((schema, ignored) -> consistency);
        }

        public Builder setSimplePrimaryVersioning(final Versioning versioning) {

            return setPrimaryVersioning((schema, ignored) -> versioning);
        }

        public Builder setSimpleReplicaConsistency(final Consistency consistency) {

            return setReplicaConsistency((schema, ignored) -> consistency);
        }

        public Builder setSimpleReplicaVersioning(final Versioning versioning) {

            return setReplicaVersioning((schema, ignored) -> versioning);
        }

        public ReplicaStorage build() {

            return new ReplicaStorage(namespace, emitter, Nullsafe.require(primary), Nullsafe.require(replica),
                    Nullsafe.option(readConsistency, (consistency) -> consistency),
                    Nullsafe.option(writeConsistency, (consistency) -> consistency),
                    Nullsafe.option(primaryConsistency, (schema, consistency) -> consistency),
                    Nullsafe.option(primaryVersioning, (schema, versioning) -> versioning),
                    Nullsafe.option(replicaConsistency, (schema, consistency) -> consistency),
                    Nullsafe.option(replicaVersioning, (schema, versioning) -> versioning));
        }
    }

    @Nullable
    private final Namespace namespace;

    @Nullable
    private final Emitter emitter;

    private final Function<ObjectSchema, Storage> primary;

    private final Function<ObjectSchema, Optional<Storage>> replica;

    private final Function<Consistency, Consistency> readConsistency;

    private final Function<Consistency, Consistency> writeConsistency;

    private final BiFunction<ObjectSchema, Consistency, Consistency> primaryConsistency;

    private final BiFunction<ObjectSchema, Versioning, Versioning> primaryVersioning;

    private final BiFunction<ObjectSchema, Consistency, Consistency> replicaConsistency;

    private final BiFunction<ObjectSchema, Versioning, Versioning> replicaVersioning;

    @Override
    public Storage storage(final ObjectSchema schema) {

        return primary.apply(schema);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        final Consistency replicationConsistency = this.readConsistency.apply(consistency);
        if(replicationConsistency.isStrongerOrEqual(Consistency.QUORUM)) {

            final IdentityHashMap<Storage, ReadTransaction> primaryTransactions = new IdentityHashMap<>();
            final IdentityHashMap<Storage, ReadTransaction> replicaTransactions = new IdentityHashMap<>();

            return new ReadTransaction() {

                private ReadTransaction primaryTransaction(final ObjectSchema schema) {

                    return primaryTransactions.computeIfAbsent(storage(schema), v -> v.read(consistency));
                }

                private ReadTransaction replicaTransaction(final ObjectSchema schema) {

                    return replicaTransactions.computeIfAbsent(storage(schema), v -> v.read(consistency));
                }

                @Override
                public ReadTransaction readObject(final ObjectSchema schema, final String id) {

                    primaryTransaction(schema).readObject(schema, id);
                    replicaTransaction(schema).readObject(schema, id);

                    return this;
                }

                @Override
                public ReadTransaction readObjectVersion(final ObjectSchema schema, final String id, final long version) {

                    primaryTransaction(schema).readObjectVersion(schema, id, version);
                    replicaTransaction(schema).readObjectVersion(schema, id, version);

                    return this;
                }

                @Override
                public CompletableFuture<BatchResponse> read() {

                    final CompletableFuture<BatchResponse> primaryFuture = BatchResponse.mergeFutures(primaryTransactions.values().stream().map(ReadTransaction::read));
                    final CompletableFuture<BatchResponse> replicaFuture = BatchResponse.mergeFutures(replicaTransactions.values().stream().map(ReadTransaction::read));

                    return mergeFutures(primaryFuture, replicaFuture);
                }
            };

        } else {

            return DelegatingStorage.super.read(consistency);
        }
    }

    private CompletableFuture<BatchResponse> mergeFutures(final CompletableFuture<BatchResponse> primaryFuture, final CompletableFuture<BatchResponse> replicaFuture) {

        return primaryFuture.thenCombine(replicaFuture, (primaryResponse, replicaResponse) -> {

            final Map<BatchResponse.Key, Map<String, Object>> results = new HashMap<>();
            Sets.union(primaryResponse.keySet(), replicaResponse.keySet()).forEach(key -> {

                final Map<String, Object> primary = primaryResponse.get(key);
                final Map<String, Object> replica = replicaResponse.get(key);

                final Map<String, Object> result = Nullsafe.option(primary, replica);

                final ReplicaMetadata meta = ReplicaMetadata.wrap(primary, replica);

                results.put(key, meta.applyTo(result));
            });

            return new BatchResponse.Basic(results);
        });
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final Consistency replicationConsistency = this.writeConsistency.apply(consistency);
        if(replicationConsistency.isStrongerOrEqual(Consistency.QUORUM) || namespace == null || emitter == null) {

            final IdentityHashMap<Storage, WriteTransaction> primaryTransactions = new IdentityHashMap<>();
            final IdentityHashMap<Storage, WriteTransaction> replicaTransactions = new IdentityHashMap<>();

            return new WriteTransaction() {

                private WriteTransaction primaryTransaction(final ObjectSchema schema) {

                    return primaryTransactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning));
                }

                private Optional<WriteTransaction> replicaTransaction(final ObjectSchema schema) {

                    return replica.apply(schema).map(storage -> {

                        final Consistency resolvedConsistency = replicaConsistency.apply(schema, consistency);
                        final Versioning resolvedVersioning = replicaVersioning.apply(schema, versioning);

                        return replicaTransactions.computeIfAbsent(storage, v -> v.write(resolvedConsistency, resolvedVersioning));
                    });
                }

                @Override
                public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                    primaryTransaction(schema).createObject(schema, id, ReplicaMetadata.unwrapPrimary(after));
                    replicaTransaction(schema).ifPresent(t -> t.createObject(schema, id, ReplicaMetadata.unwrapReplica(after)));

                    return this;
                }

                @Override
                public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                    primaryTransaction(schema).updateObject(schema, id, ReplicaMetadata.unwrapPrimary(before), ReplicaMetadata.unwrapPrimary(after));
                    replicaTransaction(schema).ifPresent(t -> t.updateObject(schema, id, ReplicaMetadata.unwrapReplica(before), ReplicaMetadata.unwrapReplica(after)));

                    return this;
                }

                @Override
                public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                    primaryTransaction(schema).deleteObject(schema, id, ReplicaMetadata.unwrapPrimary(before));
                    replicaTransaction(schema).ifPresent(t -> t.deleteObject(schema, id, ReplicaMetadata.unwrapReplica(before)));

                    return this;
                }

                @Override
                public CompletableFuture<BatchResponse> write() {

                    if(consistency != Consistency.NONE && primaryTransactions.size() > 1) {
                        throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
                    } else {

                        final CompletableFuture<BatchResponse> primaryFuture = BatchResponse.mergeFutures(primaryTransactions.values().stream().map(WriteTransaction::write));
                        final CompletableFuture<BatchResponse> replicaFuture = BatchResponse.mergeFutures(replicaTransactions.values().stream().map(WriteTransaction::write));

                        return mergeFutures(primaryFuture, replicaFuture);
                    }
                }
            };

        } else {

            final IdentityHashMap<Storage, WriteTransaction> primaryTransactions = new IdentityHashMap<>();
            final List<ReplicaSyncEvent> events = new ArrayList<>();

            return new WriteTransaction() {

                private WriteTransaction primaryTransaction(final ObjectSchema schema) {

                    return primaryTransactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning));
                }

                @Override
                public WriteTransaction createObject(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                    primaryTransaction(schema).createObject(schema, id, ReplicaMetadata.unwrapPrimary(after));
                    events.add(ReplicaSyncEvent.create(schema.getQualifiedName(), id, after, consistency, versioning));

                    return this;
                }

                @Override
                public WriteTransaction updateObject(final ObjectSchema schema, final String id, final Map<String, Object> before, final Map<String, Object> after) {

                    primaryTransaction(schema).updateObject(schema, id, ReplicaMetadata.unwrapPrimary(before), ReplicaMetadata.unwrapPrimary(after));
                    events.add(ReplicaSyncEvent.update(schema.getQualifiedName(), id, before, after, consistency, versioning));

                    return this;
                }

                @Override
                public WriteTransaction deleteObject(final ObjectSchema schema, final String id, final Map<String, Object> before) {

                    primaryTransaction(schema).deleteObject(schema, id, ReplicaMetadata.unwrapPrimary(before));
                    events.add(ReplicaSyncEvent.delete(schema.getQualifiedName(), id, before, consistency, versioning));

                    return this;
                }

                @Override
                public CompletableFuture<BatchResponse> write() {

                    if(consistency != Consistency.NONE && primaryTransactions.size() > 1) {
                        throw new IllegalStateException("Consistent write transaction spanned multiple storage engines");
                    } else {
                        return BatchResponse.mergeFutures(primaryTransactions.values().stream().map(WriteTransaction::write))
                                .thenCompose(results -> emitter.emit(events).thenApply(ignored -> results));
                    }
                }
            };
        }
    }

    private CompletableFuture<?> onSync(final ReplicaSyncEvent event, final Map<String, String> meta) {

        assert namespace != null;
        final ObjectSchema schema = namespace.requireObjectSchema(event.getSchema());
        return this.replica.apply(schema).map(replica -> {

            final Consistency resolvedConsistency = replicaConsistency.apply(schema, event.getConsistency());
            final Versioning resolvedVersioning = replicaVersioning.apply(schema, event.getVersioning());

            final WriteTransaction write = replica.write(resolvedConsistency, resolvedVersioning);

            switch (event.getAction()) {
                case CREATE: {
                    final Map<String, Object> after = ReplicaMetadata.unwrapReplica(event.getAfter());
                    write.createObject(schema, event.getId(), after);
                    break;
                }
                case UPDATE: {
                    final Map<String, Object> before = ReplicaMetadata.unwrapReplica(event.getBefore());
                    final Map<String, Object> after = ReplicaMetadata.unwrapReplica(event.getAfter());
                    write.updateObject(schema, event.getId(), before, after);
                    break;
                }
                case DELETE: {
                    final Map<String, Object> before = ReplicaMetadata.unwrapReplica(event.getBefore());
                    write.createObject(schema, event.getId(), before);
                    break;
                }
            }

            return write.write();

        }).orElseGet(() -> CompletableFuture.completedFuture(null));
    }
}
