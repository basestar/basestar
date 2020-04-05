package io.basestar.storage;

import com.google.common.collect.Multimap;
import io.basestar.schema.Namespace;

import java.util.Map;

public class TestMemoryStorage extends TestStorage {

    @Override
    protected Storage storage(final Namespace namespace, final Multimap<String, Map<String, Object>> data) {

        final Storage storage = MemoryStorage.builder().build();

        writeAll(storage, namespace, data);

        return storage;
    }
}
