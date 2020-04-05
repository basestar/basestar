package io.basestar.schema;

public enum Concurrency {

    OPTIMISTIC,
    NONE;

    public boolean isEnabled() {

        return this != NONE;
    }
}
