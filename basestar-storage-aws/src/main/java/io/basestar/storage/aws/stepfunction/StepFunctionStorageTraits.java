package io.basestar.storage.aws.stepfunction;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class StepFunctionStorageTraits implements StorageTraits {

    public static final StorageTraits INSTANCE = new StepFunctionStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.ATOMIC;
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
