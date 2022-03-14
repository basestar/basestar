package io.basestar.database;

import io.basestar.auth.Caller;
import io.basestar.event.Emitter;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;

public class TestDatabaseStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        return new DatabaseStorage(
                new DatabaseServer(namespace, MemoryStorage.builder().build(), Emitter.skip(), DatabaseMode.DEFAULT),
                Caller.SUPER
        );
    }

    @Override
    protected boolean supportsMultiValueIndexes() {

        return false;
    }
}
