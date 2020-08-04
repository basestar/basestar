package io.basestar.schema.util;

public enum Casing {

    LOWER {
        @Override
        public String apply(final String str) {

            return str.toLowerCase();
        }
    },
    UPPER {
        @Override
        public String apply(final String str) {

            return str.toUpperCase();
        }
    };

    public abstract String apply(String k);
}
