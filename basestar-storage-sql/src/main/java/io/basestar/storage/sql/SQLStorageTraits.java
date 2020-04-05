package io.basestar.storage.sql;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class SQLStorageTraits implements StorageTraits {

    public static final SQLStorageTraits INSTANCE = new SQLStorageTraits();

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
