package io.basestar.expression.sql;

import io.basestar.expression.Expression;
import lombok.Data;

public interface Select {

    boolean isConstant(Expression.Closure closure);

    <T> T visit(Visitor<T> visitor);

    @Data
    class All implements Select {

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return true;
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAll(this);
        }
    }

    @Data
    class Anonymous implements Select {

        private final Expression expression;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAnonymous(this);
        }
    }

    @Data
    class Named implements Select {

        private final Expression expression;

        private final String name;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitNamed(this);
        }
    }

    interface Visitor<T> {

        T visitAll(All from);

        T visitAnonymous(Anonymous from);

        T visitNamed(Named from);
    }
}
