package io.basestar.schema.util;

public enum Cascade {

    NONE,
    DELETE;

    public static Cascade from(final Object o) {

        if(o == null) {
            return NONE;
        } else {
            return valueOf((String)o);
        }
    }

    public boolean includes(final Cascade cascade) {

        return cascade.equals(this);
    }
}
