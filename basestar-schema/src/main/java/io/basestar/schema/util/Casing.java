package io.basestar.schema.util;

import io.basestar.schema.Reserved;
import io.basestar.util.Text;

public enum Casing {

    AS_SPECIFIED {
        @Override
        public String name(final String name) {

            return name;
        }
    },
    LOWERCASE {
        @Override
        public String name(final String name) {

            return name.toLowerCase();
        }
    },
    UPPERCASE {
        @Override
        public String name(final String name) {

            return name.toUpperCase();
        }
    },
    LOWERCASE_SNAKE {
        @Override
        public String name(final String name) {

            if (name.startsWith(Reserved.PREFIX)) {
                return Reserved.PREFIX + Text.lowerUnderscore(name.substring(Reserved.PREFIX.length()));
            } else {
                return Text.lowerUnderscore(name);
            }
        }
    },
    UPPERCASE_SNAKE {
        @Override
        public String name(final String name) {

            if (name.startsWith(Reserved.PREFIX)) {
                return Reserved.PREFIX + Text.upperUnderscore(name.substring(Reserved.PREFIX.length()));
            } else {
                return Text.upperUnderscore(name);
            }
        }
    };

    public abstract String name(String name);
}
