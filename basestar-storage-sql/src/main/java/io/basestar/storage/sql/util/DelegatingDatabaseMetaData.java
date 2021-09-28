package io.basestar.storage.sql.util;

import lombok.experimental.Delegate;

import java.sql.DatabaseMetaData;

public class DelegatingDatabaseMetaData implements DatabaseMetaData {

    @Delegate
    private final DatabaseMetaData delegate;

    public DelegatingDatabaseMetaData(final DatabaseMetaData delegate) {

        this.delegate = delegate;
    }
}
