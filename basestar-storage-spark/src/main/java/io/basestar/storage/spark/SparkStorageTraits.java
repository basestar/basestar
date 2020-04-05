package io.basestar.storage.spark;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class SparkStorageTraits implements StorageTraits {

    public static final SparkStorageTraits INSTANCE = new SparkStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.NONE;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.NONE;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        return Consistency.NONE;
    }

    @Override
    public boolean supportsPolymorphism() {

        // FIXME
        return false;
    }

    @Override
    public boolean supportsMultiObject() {

        // FIXME
        return false;
    }

    @Override
    public Concurrency getObjectConcurrency() {

        // FIXME
        return Concurrency.NONE;
    }
}
