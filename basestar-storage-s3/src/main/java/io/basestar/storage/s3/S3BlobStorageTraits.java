package io.basestar.storage.s3;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;
import io.basestar.storage.StorageTraits;

public class S3BlobStorageTraits implements StorageTraits {

    public static final S3BlobStorageTraits INSTANCE = new S3BlobStorageTraits();

    @Override
    public Consistency getHistoryConsistency() {

        return Consistency.ASYNC;
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

        return Concurrency.NONE;
    }
}
