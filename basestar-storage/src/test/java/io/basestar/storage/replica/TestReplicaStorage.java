package io.basestar.storage.replica;

import com.google.common.collect.Multimap;
import io.basestar.schema.Consistency;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;

import java.util.Map;

public class TestReplicaStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final Storage storageA = MemoryStorage.builder().build();
        final Storage storageB = MemoryStorage.builder().build();

        writeAll(storageA, namespace, data);

        return ReplicaStorage.builder()
                .setSimplePrimary(storageA)
                .setSimpleReplica(storageB)
                .setSimplePrimaryConsistency(Consistency.ATOMIC)
                .setSimpleReplicaConsistency(Consistency.ATOMIC)
                .build();
    }
}
