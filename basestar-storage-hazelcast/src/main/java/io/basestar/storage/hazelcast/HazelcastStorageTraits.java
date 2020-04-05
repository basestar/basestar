package io.basestar.storage.hazelcast;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class HazelcastStorageTraits implements StorageTraits {

    public static final HazelcastStorageTraits INSTANCE = new HazelcastStorageTraits();

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
