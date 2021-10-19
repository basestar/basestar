package io.basestar.storage.replica;

import com.google.common.collect.Sets;
import io.basestar.event.Emitter;
import io.basestar.event.Event;
import io.basestar.event.Handler;
import io.basestar.event.Handlers;
import io.basestar.schema.*;
import io.basestar.storage.BatchResponse;
import io.basestar.storage.DelegatingStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.Versioning;
import io.basestar.storage.replica.event.ReplicaSyncEvent;
import io.basestar.util.Name;
import io.basestar.util.Nullsafe;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

@Slf4j
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
                    Nullsafe.orDefault(readConsistency, (consistency) -> consistency),
                    Nullsafe.orDefault(writeConsistency, (consistency) -> consistency),
                    Nullsafe.orDefault(primaryConsistency, (schema, consistency) -> consistency),
                    Nullsafe.orDefault(primaryVersioning, (schema, versioning) -> versioning),
                    Nullsafe.orDefault(replicaConsistency, (schema, consistency) -> consistency),
                    Nullsafe.orDefault(replicaVersioning, (schema, versioning) -> versioning));
        }
    }

    @Nullable
    private final Namespace namespace;

    @Nullable
    private final Emitter emitter;

    private final Function<LinkableSchema, Storage> primary;

    private final Function<LinkableSchema, Optional<Storage>> replica;

    private final UnaryOperator<Consistency> readConsistency;

    private final UnaryOperator<Consistency> writeConsistency;

    private final BiFunction<LinkableSchema, Consistency, Consistency> primaryConsistency;

    private final BiFunction<LinkableSchema, Versioning, Versioning> primaryVersioning;

    private final BiFunction<LinkableSchema, Consistency, Consistency> replicaConsistency;

    private final BiFunction<LinkableSchema, Versioning, Versioning> replicaVersioning;

    @Override
    public Storage storage(final LinkableSchema schema) {

        return primary.apply(schema);
    }

    @Override
    public ReadTransaction read(final Consistency consistency) {

        final Consistency replicationConsistency = this.readConsistency.apply(consistency);
        if(replicationConsistency.isStrongerOrEqual(Consistency.QUORUM)) {

            final IdentityHashMap<Storage, ReadTransaction> primaryTransactions = new IdentityHashMap<>();
            final IdentityHashMap<Storage, ReadTransaction> replicaTransactions = new IdentityHashMap<>();

            return new ReadTransaction() {

                private void primaryTransaction(final LinkableSchema schema, final Function<ReadTransaction, ReadTransaction> apply) {

                    final Storage storage = storage(schema);
                    final ReadTransaction transaction = primaryTransactions.computeIfAbsent(storage, v -> v.read(consistency));
                    primaryTransactions.put(storage, apply.apply(transaction));
                }

                private void replicaTransaction(final LinkableSchema schema, final Function<ReadTransaction, ReadTransaction> apply) {

                    final Storage storage = storage(schema);
                    final ReadTransaction transaction = replicaTransactions.computeIfAbsent(storage, v -> v.read(consistency));
                    replicaTransactions.put(storage, apply.apply(transaction));
                }

                @Override
                public ReadTransaction get(final ReferableSchema schema, final String id, final Set<Name> expand) {

                    primaryTransaction(schema, v -> v.get(schema, id, expand));
                    replicaTransaction(schema, v -> v.get(schema, id, expand));
                    return this;
                }

                @Override
                public ReadTransaction getVersion(final ReferableSchema schema, final String id, final long version, final Set<Name> expand) {

                    primaryTransaction(schema, v -> v.getVersion(schema, id, version, expand));
                    replicaTransaction(schema, v -> v.getVersion(schema, id, version, expand));
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

        return primaryFuture.thenCombine(replicaFuture.exceptionally(e -> {
            log.error("Failed to replicate", e);
            return BatchResponse.empty();
        }), (primaryResponse, replicaResponse) -> {

            try {

                final Map<BatchResponse.RefKey, Map<String, Object>> refs = new HashMap<>();
                Sets.union(primaryResponse.getRefs().keySet(), replicaResponse.getRefs().keySet()).forEach(key -> {

                    final Map<String, Object> primary = primaryResponse.get(key);
                    final Map<String, Object> replica = replicaResponse.get(key);

                    if (primary != null) {

                        final ReplicaMetadata meta = ReplicaMetadata.wrap(primary, replica);
                        refs.put(key, meta.applyTo(primary));
                    }
                });

                return new BatchResponse(refs);
            } catch (final Exception e) {
                log.error("Failed to process replica response");
                return primaryResponse;
            }
        });
    }

    @Override
    public WriteTransaction write(final Consistency consistency, final Versioning versioning) {

        final Consistency replicationConsistency = this.writeConsistency.apply(consistency);
        if(replicationConsistency.isStrongerOrEqual(Consistency.QUORUM) || namespace == null || emitter == null) {

            final IdentityHashMap<Storage, WriteTransaction> primaryTransactions = new IdentityHashMap<>();
            final IdentityHashMap<Storage, WriteTransaction> replicaTransactions = new IdentityHashMap<>();

            return new WriteTransaction() {

                private WriteTransaction primaryTransaction(final LinkableSchema schema) {

                    return primaryTransactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning));
                }

                private Optional<WriteTransaction> replicaTransaction(final LinkableSchema schema) {

                    return replica.apply(schema).map(storage -> {

                        final Consistency resolvedConsistency = replicaConsistency.apply(schema, consistency);
                        final Versioning resolvedVersioning = replicaVersioning.apply(schema, versioning);

                        return replicaTransactions.computeIfAbsent(storage, v -> v.write(resolvedConsistency, resolvedVersioning));
                    });
                }

                @Override
                public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                    primaryTransaction(schema).write(schema, ReplicaMetadata.unwrapPrimary(after));
                    replicaTransaction(schema).ifPresent(t -> t.write(schema, ReplicaMetadata.unwrapReplica(after)));

                    return this;
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
                public WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                    primaryTransaction(schema).writeHistory(schema, id, ReplicaMetadata.unwrapPrimary(after));
                    // replica will write history automatically
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

                private WriteTransaction primaryTransaction(final LinkableSchema schema) {

                    return primaryTransactions.computeIfAbsent(storage(schema), v -> v.write(consistency, versioning));
                }

                @Override
                public WriteTransaction write(final LinkableSchema schema, final Map<String, Object> after) {

                    primaryTransaction(schema).write(schema, ReplicaMetadata.unwrapPrimary(after));
                    events.add(ReplicaSyncEvent.write(schema.getQualifiedName(), after, consistency, versioning));
                    return this;
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
                public WriteTransaction writeHistory(final ObjectSchema schema, final String id, final Map<String, Object> after) {

                    primaryTransaction(schema).writeHistory(schema, id, ReplicaMetadata.unwrapPrimary(after));
                    // replica will write history automatically
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
        final LinkableSchema schema = namespace.requireLinkableSchema(event.getSchema());
        return this.replica.apply(schema).map(replica -> {

            final Consistency resolvedConsistency = replicaConsistency.apply(schema, event.getConsistency());
            final Versioning resolvedVersioning = replicaVersioning.apply(schema, event.getVersioning());

            final WriteTransaction write = replica.write(resolvedConsistency, resolvedVersioning);

            switch (event.getAction()) {
                case CREATE: {
                    final Map<String, Object> after = ReplicaMetadata.unwrapReplica(event.getAfter());
                    write.createObject((ObjectSchema)schema, event.getId(), after);
                    break;
                }
                case UPDATE: {
                    final Map<String, Object> before = ReplicaMetadata.unwrapReplica(event.getBefore());
                    final Map<String, Object> after = ReplicaMetadata.unwrapReplica(event.getAfter());
                    write.updateObject((ObjectSchema)schema, event.getId(), before, after);
                    break;
                }
                case DELETE: {
                    final Map<String, Object> before = ReplicaMetadata.unwrapReplica(event.getBefore());
                    write.deleteObject((ObjectSchema)schema, event.getId(), before);
                    break;
                }
                case WRITE: {
                    final Map<String, Object> after = ReplicaMetadata.unwrapReplica(event.getAfter());
                    write.write(schema, after);
                    break;
                }
            }

            return write.write();

        }).orElseGet(() -> CompletableFuture.completedFuture(null));
    }

//    @Override
//    public Pager<RepairInfo> repair(final LinkableSchema schema) {
//
//        final Map<String, Optional<Storage>> delegates = ImmutableMap.of(
//                "p", Optional.of(primary.apply(schema)),
//                "r", replica.apply(schema)
//        );
//        return Page.merge(Immutable.transformValues(delegates, (k, v) -> v.map(v2 -> v2.repair(schema)).orElse(Pager.empty()));
//
//        return Stream.of(Optional.of(primary.apply(schema)), replica.apply(schema))
//                .flatMap(v -> v.map(Stream::of).orElse(Stream.of()))
//                .flatMap(v -> v.repair(schema))
//                .collect(Collectors.toMap());
//    }
//
//    @Override
//    public Pager<RepairInfo> repairIndex(final LinkableSchema schema, final Index index) {
//
//        return Stream.of(Optional.of(primary.apply(schema)), replica.apply(schema))
//                .flatMap(v -> v.map(Stream::of).orElse(Stream.of()))
//                .flatMap(v -> v.repairIndex(schema, index).stream())
//                .collect(Collectors.toList());
//    }
}
