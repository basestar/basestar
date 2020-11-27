package io.basestar.stream;

import io.basestar.storage.MemoryStorage;

class TestStorageSubscriptions extends TestSubscriptions {

    @Override
    protected Subscriptions subscriber() {

        final MemoryStorage storage = MemoryStorage.builder().build();
        return new StorageSubscriptions(storage);
    }
}
