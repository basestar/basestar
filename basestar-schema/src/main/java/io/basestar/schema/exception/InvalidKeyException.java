package io.basestar.schema.exception;

import io.basestar.schema.use.Use;

public class InvalidKeyException extends RuntimeException {

    public InvalidKeyException(final Use<?> type) {

        super("Object of type " + type.toString() + " cannot be used as a key");
    }
}
