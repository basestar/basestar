package io.basestar.storage.elasticsearch;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class ElasticsearchStorageTraits implements StorageTraits {

    public static final ElasticsearchStorageTraits INSTANCE = new ElasticsearchStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.ASYNC;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.ATOMIC;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        // FIXME
        return Consistency.EVENTUAL;
    }

    @Override
    public boolean supportsPolymorphism() {

        return false;
    }

    @Override
    public boolean supportsMultiObject() {

        return false;
    }

    @Override
    public Concurrency getObjectConcurrency() {

        return Concurrency.OPTIMISTIC;
    }
}
