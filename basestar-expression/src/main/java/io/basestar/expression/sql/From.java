package io.basestar.expression.sql;

import com.google.common.collect.ImmutableSet;
import io.basestar.expression.Context;
import io.basestar.expression.Expression;
import io.basestar.expression.Renaming;
import lombok.Data;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface From {

    boolean isConstant(Expression.Closure closure);

    Set<String> names();

    <T> T visit(Visitor<T> visitor);

    From bind(Context context, Renaming root);

    @Data
    class Anonymous implements From {

        private final Expression expression;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public Set<String> names() {

            return ImmutableSet.of();
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitAnonymous(this);
        }

        @Override
        public From bind(final Context context, final Renaming root) {

            final Expression bound = expression.bind(context, root);
            return bound == expression ? this : new Anonymous(bound);
        }

        @Override
        public String toString() {

            return expression.toString();
        }
    }

    @Data
    class Named implements From {

        private final Expression expression;

        private final String name;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return expression.isConstant(closure);
        }

        @Override
        public Set<String> names() {

            return ImmutableSet.of(name);
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitNamed(this);
        }

        @Override
        public From bind(final Context context, final Renaming root) {

            final Expression bound = expression.bind(context, root);
            return bound == expression ? this : new Named(bound, name);
        }

        @Override
        public String toString() {

            return expression.toString() + " AS " + name;
        }
    }

    @Data
    class Join implements From {

        public enum Type {

            INNER("INNER"),
            LEFT_OUTER("LEFT OUTER"),
            RIGHT_OUTER("RIGHT OUTER"),
            FULL_OUTER("FULL OUTER");

            private final String string;

            Type(final String string) {

                this.string = string;
            }

            @Override
            public String toString() {

                return string;
            }
        }

        private final From left;

        private final From right;

        private final Expression on;

        private final Join.Type type;

        @Override
        public boolean isConstant(final Expression.Closure closure) {

            return left.isConstant(closure) && right.isConstant(closure) && on.isConstant(closure.with(names()));
        }

        @Override
        public Set<String> names() {

            return Stream.concat(left.names().stream(), right.names().stream()).collect(Collectors.toSet());
        }

        @Override
        public <T> T visit(final Visitor<T> visitor) {

            return visitor.visitJoin(this);
        }

        @Override
        public From bind(final Context context, final Renaming root) {

            final From left = this.left.bind(context, root);
            final From right = this.right.bind(context, root);
            final Expression on = this.on.bind(context, root);
            if(left == this.left && right == this.right && on == this.on) {
                return this;
            } else {
                return new Join(left, right, on, type);
            }
        }

        @Override
        public String toString() {

            return left + " " + type + " JOIN " + right + " ON " + on;
        }
    }

    interface Visitor<T> {

        T visitAnonymous(Anonymous from);

        T visitNamed(Named from);

        T visitJoin(Join from);
    }
}
