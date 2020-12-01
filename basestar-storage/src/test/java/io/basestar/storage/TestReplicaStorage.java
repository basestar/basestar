package io.basestar.storage;

import io.basestar.schema.Namespace;
import io.basestar.storage.replica.ReplicaStorage;

class TestReplicaStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        return ReplicaStorage.builder()
                .setSimplePrimary(MemoryStorage.builder().build())
                .setSimpleReplica(MemoryStorage.builder().build())
                .build();
    }
}
