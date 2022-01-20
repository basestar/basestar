package io.basestar.storage.sequence;

import java.util.concurrent.CompletableFuture;

public interface SequenceStorage {

    default CompletableFuture<Long> getAndAdd(final String sequence) {

        return getAndAdd(sequence, 1L);
    }

    CompletableFuture<Long> getAndAdd(String sequence, long by);
}
