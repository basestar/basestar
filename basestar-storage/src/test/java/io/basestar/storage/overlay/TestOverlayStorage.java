package io.basestar.storage.overlay;

import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import org.junit.jupiter.api.Disabled;

@Disabled
class TestOverlayStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace) {

        final Storage storageA = MemoryStorage.builder().build();
        final Storage storageB = MemoryStorage.builder().build();

        return OverlayStorage.builder()
                .baseline(storageA)
                .overlay(storageB)
                .build();
    }
}
