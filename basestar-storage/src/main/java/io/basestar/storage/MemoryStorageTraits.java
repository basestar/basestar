package io.basestar.storage;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;

public class MemoryStorageTraits implements StorageTraits {

    public static final MemoryStorageTraits INSTANCE = new MemoryStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.ATOMIC;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.ATOMIC;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        return Consistency.ASYNC;
    }

    @Override
    public boolean supportsPolymorphism() {

        return true;
    }

    @Override
    public boolean supportsMultiObject() {

        return true;
    }

    @Override
    public Concurrency getObjectConcurrency() {

        return Concurrency.OPTIMISTIC;
    }
}
