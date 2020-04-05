package io.basestar.storage;

import io.basestar.schema.Concurrency;
import io.basestar.schema.Consistency;

public interface StorageTraits {

    Consistency getHistoryConsistency();

    Consistency getSingleValueIndexConsistency();

    Consistency getMultiValueIndexConsistency();

    boolean supportsPolymorphism();

    boolean supportsMultiObject();

    Concurrency getObjectConcurrency();

    default Consistency getIndexConsistency(boolean multi) {

        return multi ? getMultiValueIndexConsistency() : getSingleValueIndexConsistency();
    }


//    Consistency getObjectReadConsistency();
//
//    Consistency getObjectWriteConsistency();
//
//    Consistency getHistoryReadConsistency();
//
//    Consistency getHistoryWriteConsistency();
//
//    Consistency getPolymorphicReadConsistency();
//
//    Consistency getPolymorphicWriteConsistency();
//
//    Consistency getSingleIndexReadConsistency();
//
//    Consistency getSingleIndexWriteConsistency();
//
//    Consistency getMultiValueIndexReadConsistency();
//
//    Consistency getMultiValueIndexWriteConsistency();
//
//    Consistency getRefIndexReadConsistency();
//
//    Consistency getRefIndexWriteConsistency();
//
//    Consistency getMultiObjectReadConsistency();
//
//    Consistency getMultiObjectWriteConsistency();
}
