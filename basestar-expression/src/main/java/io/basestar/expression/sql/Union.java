package io.basestar.expression.sql;

import io.basestar.expression.Expression;
import lombok.Data;

public interface Union {

    <T> T visit(Visitor<T> visitor);

    @Data
    class Distinct implements Union {

        private final Expression expr;

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitDistinct(this);
        }
    }

    @Data
    class All implements Union {

        private final Expression expr;

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAll(this);
        }
    }

    interface Visitor<T> {

        T visitDistinct(Distinct from);

        T visitAll(All from);
    }
}
