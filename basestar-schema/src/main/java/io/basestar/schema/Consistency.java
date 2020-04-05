package io.basestar.schema;

public enum Consistency implements Comparable<Consistency> {

    ATOMIC,
    QUORUM,
    EVENTUAL,
    ASYNC,
    NONE; /* internal use only */

    public boolean isAtomic() {

        return this == ATOMIC;
    }

    public boolean isQuorum() {

        return this == QUORUM;
    }

    public boolean isEventual() {

        return this == EVENTUAL;
    }

    public boolean isAsync() {

        return this == ASYNC;
    }

    public boolean isStronger(final Consistency other) {

        return ordinal() < other.ordinal();
    }

    public boolean isStrongerOrEqual(final Consistency other) {

        return ordinal() <= other.ordinal();
    }

    public boolean isWeaker(final Consistency other) {

        return ordinal() > other.ordinal();
    }

    public boolean isWeakerOrEqual(final Consistency other) {

        return ordinal() >= other.ordinal();
    }
}
