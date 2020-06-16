package io.basestar.storage;

public interface CloseableLock extends AutoCloseable {

    void release();

    @Override
    default void close() {

        release();
    }
}
