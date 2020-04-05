package io.basestar.storage.cognito;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class CognitoStorageTraits implements StorageTraits {

    public static final CognitoStorageTraits INSTANCE = new CognitoStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.NONE;
    }

    @Override
    public Consistency getSingleValueIndexConsistency() {

        return Consistency.ATOMIC;
    }

    @Override
    public Consistency getMultiValueIndexConsistency() {

        return Consistency.NONE;
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

        return Concurrency.NONE;
    }
}
