package io.basestar.expression.sql;

import io.basestar.expression.Expression;
import lombok.Data;

public interface Select {

    boolean isConstant(Expression.Closure closure);

    @Data
    class All implements Select {

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return true;
        }
    }

    @Data
    class Anonymous implements Select {

        private final Expression expression;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
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
    }
}
