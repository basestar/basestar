package io.basestar.database;

public enum DatabaseMode {

    DEFAULT {

        @Override
        boolean isReadonly() {

            return false;
        }

    },
    READONLY {

        @Override
        boolean isReadonly() {

            return true;
        }

    };

    abstract boolean isReadonly();
}
