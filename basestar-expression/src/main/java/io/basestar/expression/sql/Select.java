package io.basestar.expression.sql;

import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import lombok.Data;

import java.io.Serializable;

public interface Select extends Serializable {

    boolean isConstant(Expression.Closure closure);

    <T> T visit(Visitor<T> visitor);

    Select bind(Context context, Renaming root);

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

        @Override
        public Select bind(final Context context, final Renaming root) {

            return this;
        }

        @Override
        public String toString() {

            return "*";
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

        @Override
        public Select bind(final Context context, final Renaming root) {

            final Expression bound = expression.bind(context, root);
            return bound == expression ? this : new Anonymous(bound);
        }

        @Override
        public String toString() {

            return expression.toString();
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

        @Override
        public Select bind(final Context context, final Renaming root) {

            final Expression bound = expression.bind(context, root);
            return bound == expression ? this : new Named(bound, name);
        }

        @Override
        public String toString() {

            return expression.toString() + " AS " + name;
        }
    }

    interface Visitor<T> {

        T visitAll(All from);

        T visitAnonymous(Anonymous from);

        T visitNamed(Named from);
    }
}
