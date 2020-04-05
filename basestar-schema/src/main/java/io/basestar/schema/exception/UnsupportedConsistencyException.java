package io.basestar.schema.exception;

import io.basestar.schema.Consistency;

public class UnsupportedConsistencyException extends RuntimeException {

    public UnsupportedConsistencyException(final String path, final String storage, final Consistency best, final Consistency requested) {

        super("Consistency " + requested + " for " + path + "is not supported by storage " + storage + ", best available is " + best);
    }
}
