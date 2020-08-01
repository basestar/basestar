package io.basestar.storage.overlay;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;
import io.basestar.storage.MemoryStorage;
import io.basestar.storage.Storage;
import io.basestar.storage.TestStorage;
import org.junit.jupiter.api.Disabled;

import java.util.Map;

@Disabled
public class TestOverlayStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final Storage storageA = MemoryStorage.builder().build();
        final Storage storageB = MemoryStorage.builder().build();

        writeAll(storageB, namespace, data);

        return OverlayStorage.builder()
                .baseline(storageA)
                .overlay(storageB)
                .build();
    }
}
