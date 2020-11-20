package io.basestar.spark.util;

import java.io.Serializable;

public interface NamingConvention extends Serializable {

    CaseInsensitive CASE_INSENSITIVE = new CaseInsensitive();

    CaseSensitive CASE_SENSITIVE = new CaseSensitive();

    ToLowercase TO_LOWERCASE = new ToLowercase();

    ToUppercase TO_UPPERCASE = new ToUppercase();

    NamingConvention DEFAULT = CASE_INSENSITIVE;

    String output(String input);

    boolean equals(String input, String output);

    interface Preserving extends NamingConvention {

        @Override
        default String output(final String input) {

            return input;
        }
    }

    class CaseInsensitive implements Preserving {

        @Override
        public boolean equals(final String input, final String output) {

            return input.equalsIgnoreCase(output);
        }
    }

    class CaseSensitive implements Preserving {

        @Override
        public boolean equals(final String a, final String b) {

            return a.equals(b);
        }
    }

    class ToLowercase implements NamingConvention {

        @Override
        public String output(final String input) {

            return input.toLowerCase();
        }

        @Override
        public boolean equals(final String input, final String output) {

            return input.toLowerCase().equals(output);
        }
    }

    class ToUppercase implements NamingConvention {

        @Override
        public String output(final String input) {

            return input.toUpperCase();
        }

        @Override
        public boolean equals(final String input, final String output) {

            return input.toUpperCase().equals(output);
        }
    }
}
