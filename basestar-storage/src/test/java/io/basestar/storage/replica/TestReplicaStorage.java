package io.basestar.storage.replica;

import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;

class TestReplicaStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        final Storage storageA = MemoryStorage.builder().build();
        final Storage storageB = MemoryStorage.builder().build();

        return ReplicaStorage.builder()
                .setSimplePrimary(storageA)
                .setSimpleReplica(storageB)
                .setSimplePrimaryConsistency(Consistency.ATOMIC)
                .setSimpleReplicaConsistency(Consistency.ATOMIC)
                .build();
    }
}
