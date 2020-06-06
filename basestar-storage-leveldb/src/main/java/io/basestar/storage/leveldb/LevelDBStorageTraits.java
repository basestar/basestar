package io.basestar.storage.leveldb;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class LevelDBStorageTraits implements StorageTraits {

    public static final StorageTraits INSTANCE = new LevelDBStorageTraits();

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

        return Consistency.ATOMIC;
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
