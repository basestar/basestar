package io.basestar.schema;

public class Reserved {

    public static final String DELIMITER = "/";

    public static final String PREFIX = "__";

    public static final String META_PREFIX = "@";

    public static final String ID = "id";

    public static final String SCHEMA = "schema";

    public static final String CREATED = "created";

    public static final String UPDATED = "updated";

    public static final String VERSION = "version";

    public static final String HASH = "hash";

    public static final String THIS = "this";

    public static boolean isReserved(final String name) {

        if(name.startsWith(PREFIX) || (name.indexOf('\0') != -1)) {
            return true;
        } else {
            switch (name) {
//                case ID:
//                case SCHEMA:
//                case CREATED:
//                case UPDATED:
//                case VERSION:
//                case HASH:
                case THIS:
                    return true;
                default:
                    return false;
            }
        }
    }
}
