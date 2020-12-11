package io.basestar.schema.use;

import java.util.Objects;

public interface Widening {

    boolean canWiden(Use<?> from, Use<?> to);

    class None implements Widening {

        @Override
        public boolean canWiden(final Use<?> from, final Use<?> to) {

            return Objects.equals(from, to);
        }
    }
}
