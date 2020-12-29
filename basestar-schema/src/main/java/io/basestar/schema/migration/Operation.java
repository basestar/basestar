package io.basestar.schema.migration;

public enum  Operation {

    CREATE,
    UPDATE,
    UPSERT,
    DELETE;

    public static Operation parse(final String str) {

        return Operation.valueOf(str.toUpperCase());
    }
}
