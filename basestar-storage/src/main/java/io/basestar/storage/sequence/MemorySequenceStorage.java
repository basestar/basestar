package io.basestar.storage.sequence;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemorySequenceStorage implements SequenceStorage {

    private final ConcurrentMap<String, AtomicLong> values;

    public MemorySequenceStorage() {

        this.values = new ConcurrentHashMap<>();
    }

    public MemorySequenceStorage(final Map<String, Long> values) {

        this.values = new ConcurrentHashMap<>();
        values.forEach((k, v) -> this.values.put(k, new AtomicLong(v)));
    }

    @Override
    public CompletableFuture<Long> getAndAdd(final String sequence, final long by) {

        final AtomicLong value = values.computeIfAbsent(sequence, ignored -> new AtomicLong(0L));
        final long result = value.getAndAdd(by);
        return CompletableFuture.completedFuture(result);
    }
}
